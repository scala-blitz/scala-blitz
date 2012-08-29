package scala.collection.parallel



import sun.misc.Unsafe
import annotation.{tailrec, static}
import collection._



object Utils {

  def getUnsafe(): Unsafe = {
    if (this.getClass.getClassLoader == null) Unsafe.getUnsafe()
    try {
      val fld = classOf[Unsafe].getDeclaredField("theUnsafe")
      fld.setAccessible(true)
      return fld.get(this.getClass).asInstanceOf[Unsafe]
    } catch {
      case e: Throwable => throw new RuntimeException("Could not obtain access to sun.misc.Unsafe", e)
    }
  }

}


object Loop extends testing.Benchmark {

  val size = sys.props("size").toInt
  var result = 0

  private def quickloop(start: Int, limit: Int): Int = {
    var i = start
    var sum = 0
    while (i < limit) {
      sum += i
      i += 1
    }
    sum
  }

  def run() {
    var sum = quickloop(0, size)
    result = sum
  }
  
}


object AdvLoop extends testing.Benchmark {

  final class Work(var from: Int, val step: Int, val until: Int) {
    final def CAS(ov: Int, nv: Int) = unsafe.compareAndSwapInt(this, Work.FROM_OFFSET, ov, nv)
  }

  object Work {
    @static val FROM_OFFSET = unsafe.objectFieldOffset(classOf[AdvLoop.Work].getDeclaredField("from"))
  }

  val unsafe = Utils.getUnsafe()
  val size = sys.props("size").toInt
  val block = sys.props("block").toInt
  var result = 0
  var work: Work = null

  private def quickloop(start: Int, limit: Int): Int = {
    var i = start
    var sum = 0
    while (i < limit) {
      sum += i
      i += 1
    }
    sum
  }

  def run() {
    work = new Work(0, block, size)
    var sum = 0
    var looping = true
    while (looping) {
      val currFrom = work.from
      val nextFrom = work.from + work.step
      if (currFrom < work.until) {
        work.from = nextFrom
        sum += quickloop(currFrom, nextFrom)
      } else looping = false
    }
    result = sum
  }
  
}


object EqualLoadLoop extends testing.Benchmark {

  val size = sys.props("size").toInt
  val par = sys.props("par").toInt

  def quickloop(start: Int, limit: Int) = {
    var i = start
    var sum = 0
    while (i < limit) {
      sum += i
      i += 1
    }
    sum
  }

  final class Worker extends Thread {
    @volatile var result = 0
    override def run() {
      var i = 0
      val until = size / par
      result = quickloop(0, until)
    }
  }

  def run() {
    val workers = for (_ <- 0 until par) yield new Worker
    for (w <- workers) w.start()
    for (w <- workers) w.join()
    workers.map(_.result).sum
  }

}


object StealLoop extends testing.Benchmark {

  final class Ptr(val level: Int)(@volatile var child: Node) {
    def casChild(ov: Node, nv: Node) = unsafe.compareAndSwapObject(this, Ptr.CHILD_OFFSET, ov, nv)
    def writeChild(nv: Node) = unsafe.putObjectVolatile(this, Ptr.CHILD_OFFSET, nv)

    /** Try to expand node and return true if node was expanded.
     *  Return false if node was completed.
     */
    @tailrec def expand(): Boolean = {
      val child_t0 = /*READ*/child
      if (!child_t0.isLeaf) true else { // already expanded
        // first set progress to -progress
        val progress_t1 = /*READ*/child_t0.progress
        if (progress_t1 == child_t0.until) false // already completed
        else {
          if (progress_t1 >= 0) {
            if (child_t0.casProgress(progress_t1, -(progress_t1 + 1))) expand() // marked stolen - now move on to node creation
            else expand() // wasn't marked stolen and failed marking stolen - retry
          } else { // marked stolen
            // node effectively immutable (except for `result`) - expand it
            val expanded = child_t0.newExpanded
            if (casChild(child_t0, expanded)) true // try to replace with expansion
            else expand() // failure (spurious or due to another expand) - retry
          }
        }
      }
    }

    def toString(l: Int): String = "\t" * level + "Ptr" + level + " -> " + child.toString(l)

    def balance: collection.immutable.HashMap[Owner, Int] = {
      val here = collection.immutable.HashMap(child.owner -> (child.progress - child.start))
      if (child.isLeaf) here
      else Seq(here, child.left.balance, child.right.balance).foldLeft(collection.immutable.HashMap[Owner, Int]()) {
        case (a, b) => a.merged(b) {
          case ((ak, av), (bk, bv)) => ((ak, av + bv))
        }
      }
    }

    /** Whether stealer should go left at this level. */
    def chooseAsStealer(index: Int, total: Int): Boolean = {
      if (isTreeTop(total, level)) chooseInTreeTop(index, level)
      else false
    }

    /** Whether victim should go left at this level. */
    def chooseAsVictim(index: Int, total: Int): Boolean = {
      if (isTreeTop(total, level)) chooseInTreeTop(index, level)
      else true
    }

    def choosePtrAsStealer(index: Int, total: Int): Ptr = {
      if (chooseAsStealer(index, total)) child.left
      else child.right
    }

    def choosePtrAsVictim(index: Int, total: Int): Ptr = {
      if (chooseAsVictim(index, total)) child.left
      else child.right
    }

  }

  object Ptr {
    @static val CHILD_OFFSET = unsafe.objectFieldOffset(classOf[StealLoop.Ptr].getDeclaredField("child"))
  }

  type T = Int
  type Owner = Thread

  final class Node(val left: Ptr, val right: Ptr, val parent: Ptr)(val start: Int, @volatile var progress: Int, val step: Int, val until: Int) {
    /*
     * progress: start -> x1 -> ... -> xn -> until
     *                                    -> -(xn + 1)
     * x1 > start, x(i+1) > xi, until > xn
     * 
     * owner: null -> v, v != null
     *
     * result: None --(if owner != null)--> Some(v)
     */

    @volatile var owner: Owner = null
    @volatile var result: Option[T] = None

    final def casProgress(ov: Int, nv: Int) = unsafe.compareAndSwapInt(this, Node.PROGRESS_OFFSET, ov, nv)
    final def casOwner(ov: Owner, nv: Owner) = unsafe.compareAndSwapObject(this, Node.OWNER_OFFSET, ov, nv)

    def nonEmpty = (until - start) > 0

    def completed = /*READ*/progress == until

    def stolen = /*READ*/progress < 0

    def isLeaf = left eq null

    @tailrec def tryOwn(thiz: Owner): Boolean = {
      val currowner = /*READ*/owner
      if (currowner != null) false
      else if (casOwner(currowner, thiz)) true
      else tryOwn(thiz)
    }

    def trySteal(): Boolean = parent.expand()

    def newExpanded: Node = {
      val stolenP = /*READ*/progress
      val p = -(stolenP) - 1
      val remaining = until - p
      val firsthalf = remaining / 2
      val secondhalf = remaining - firsthalf
      val lptr = new Ptr(parent.level + 1)(null)
      val rptr = new Ptr(parent.level + 1)(null)
      val lnode = new Node(null, null, lptr)(p, p, step, p + firsthalf)
      val rnode = new Node(null, null, rptr)(p + firsthalf, p + firsthalf, step, until)
      lptr.writeChild(lnode)
      rptr.writeChild(rnode)
      val nnode = new Node(lptr, rptr, parent)(start, p, step, until)
      nnode.owner = this.owner
      nnode
    }

    def toString(lev: Int): String = {
      "Node(%s: %d, %d, %d, %d)".format(owner, start, progress, step, until) + (if (!isLeaf) {
        "\n" + left.toString(lev + 1) + right.toString(lev + 1)
      } else "\n")
    }
  }

  object Node {
    @static val PROGRESS_OFFSET = unsafe.objectFieldOffset(classOf[StealLoop.Node].getDeclaredField("progress"))
    @static val OWNER_OFFSET = unsafe.objectFieldOffset(classOf[StealLoop.Node].getDeclaredField("owner"))
  }

  val unsafe = Utils.getUnsafe()
  def random = scala.concurrent.forkjoin.ThreadLocalRandom.current
  val size = sys.props("size").toInt
  val block = sys.props("block").toInt
  val par = sys.props("par").toInt
  val inspectgc = sys.props.getOrElse("inspectgc", "false").toBoolean

  /** Returns true iff the level of the tree is such that: 2^level < total */
  final def isTreeTop(total: Int, level: Int): Boolean = (1 << level) < total

  /** Returns true iff the worker should first go left at this level of the tree top. */
  final def chooseInTreeTop(index: Int, level: Int): Boolean = ((index >> level) & 0x1) == 0

  /** Returns true if the worker labeled with `index` with a total of
   *  `total` workers should go left at level `level`.
   *  Returns false otherwise.
   */
  final def choose(index: Int, total: Int, level: Int): Boolean = {
    if (isTreeTop(total, level)) {
      chooseInTreeTop(index, level)
    } else random.nextBoolean
  }

  private def quickloop(start: Int, limit: Int): Int = {
    var i = start
    var sum = 0
    while (i < limit) {
      sum += i
      i += 1
    }
    sum
  }

  /** Returns true if completed with no stealing.
   *  Returns false if steal occurred.
   */
  def workloop(tree: Ptr): Boolean = {
    val work = tree.child
    var sum = 0
    var looping = true
    while (looping) {
      val currprog = /*READ*/work.progress
      var nextprog = math.min(currprog + work.step, work.until)
      if (currprog < work.until && currprog >= 0) {
        if (work.casProgress(currprog, nextprog)) sum += quickloop(currprog, nextprog)
      } else looping = false
    }
    if (work.completed) {
      work.result = Some(sum)
      true
    } else if (work.stolen) {
      // help expansion if necessary
      if (tree.child.isLeaf) tree.expand()
      tree.child.result = Some(sum)
      false
    } else sys.error("unreachable: " + work.progress + ", " + work.until)
  }
  
  class Worker(val root: Ptr, val index: Int, val total: Int) extends Thread {
    setName("Worker: " + index)

    def findWork(tree: Ptr): Ptr = {
      val node = tree.child
      if (node.isLeaf) {
        if (node.completed) null // no further expansions
        else {
          // more work
          if (node.tryOwn(this)) tree
          else if (node.trySteal()) {
            val subnode = tree.choosePtrAsStealer(index, total)
            if (subnode.child.tryOwn(this)) subnode
            else findWork(tree)
          } else findWork(tree)
        }
      } else {
        // descend deeper
        if (choose(index, total, root.level)) {
          val ln = findWork(node.left)
          if (ln != null) ln else findWork(node.right)
        } else {
          val rn = findWork(node.right)
          if (rn != null) rn else findWork(node.left)
        }
      }
    }

    @tailrec override final def run() {
      val leaf = findWork(root)
      if (leaf != null) {
        @tailrec def workAndDescend(leaf: Ptr) {
          val nosteals = workloop(leaf)
          if (!nosteals) {
            val subnode = leaf.choosePtrAsVictim(index, total)
            if (subnode.child.tryOwn(this)) workAndDescend(subnode)
          }
        }
        workAndDescend(leaf)
        run()
      } else {
        // no more work
      }
    }
  }

  var repetition = 0
  var root: Ptr = _

  def run() {
    root = new Ptr(0)(null)
    val work = new Node(null, null, root)(0, 0, block, size)
    root.writeChild(work)

    val workers = for (i <- 0 until par) yield new Worker(root, i, par)
    for (w <- workers) w.start()
    for (w <- workers) w.join()
  }

  override def setUp() {
    if (inspectgc) {
      println("run starting...")
      Thread.sleep(500)
    }
  }

  override def tearDown() {
    if (inspectgc) {
      Thread.sleep(500)
      println("run completed...")
    }
    repetition += 1
    if (repetition == 15) {
      val balance = root.balance
      println(root.toString(0))
      println(balance.toList.sortBy(_._1.getName).map(p => p._1 + ": " + p._2).mkString("work balance:\n", "\n", ""))
      println("total: " + balance.foldLeft(0)(_ + _._2))
    }
    root = null
  }

}
























