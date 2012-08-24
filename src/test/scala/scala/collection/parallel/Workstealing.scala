package scala.collection.parallel



import sun.misc.Unsafe
import annotation.{tailrec, static}



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


object StealLoop extends testing.Benchmark {

  final class Ptr(@volatile var child: Node) {
    def casChild(ov: Node, nv: Node) = unsafe.compareAndSwapObject(this, Ptr.CHILD_OFFSET, ov, nv)
    def writeChild(nv: Node) = unsafe.putObjectVolatile(this, Ptr.CHILD_OFFSET, nv)
  }

  object Ptr {
    @static val CHILD_OFFSET = unsafe.objectFieldOffset(classOf[StealLoop.Ptr].getDeclaredField("child"))
  }

  type T = Int
  type Owner = Thread

  final class Node(val left: Ptr, val right: Ptr, val parent: Ptr)(val start: Int, @volatile var progress: Int, val step: Int, val until: Int) {
    /*
     * progress: start -> x1 -> ... -> xn -> until
     *                                    -> -xn
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

    /** Try to expand node and return true if node was expanded.
     *  Return false if node was completed.
     */
    @tailrec def expand(): Boolean = if (parent.child != this) true else { // already expanded
      // first set progress to -progress
      val progress_t0 = /*READ*/progress
      if (progress_t0 == until) false // already completed
      else {
        if (progress_t0 >= 0 && !casProgress(progress_t0, -progress_t0)) expand() // not marked stolen and failed marking stolen
        else { // marked stolen
          // node effectively immutable (except for `result`) - expand it
          val expanded = createExpanded()
          if (parent.casChild(this, expanded)) true // try to replace with expansion
          else expand() // failure (spurious or due to another expand) - retry
        }
      }
    }

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

    def trySteal(): Boolean = expand()

    def createExpanded() = {
      val p = /*READ*/progress
      val remaining = until - p
      val firsthalf = remaining / 2
      val secondhalf = remaining - firsthalf
      val lptr = new Ptr(null)
      val rptr = new Ptr(null)
      val lnode = new Node(null, null, lptr)(p, p, step, p + firsthalf)
      val rnode = new Node(null, null, rptr)(p + firsthalf, p + firsthalf, step, until)
      lptr.writeChild(lnode)
      rptr.writeChild(rnode)
      val nnode = new Node(lptr, rptr, parent)(start, p, step, until)
      nnode
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
      val nextprog = currprog + work.step
      if (currprog < work.until && currprog >= 0) {
        if (work.casProgress(currprog, nextprog)) sum += quickloop(currprog, nextprog)
      } else looping = false
    }
    if (work.completed) {
      work.result = Some(sum)
      true
    } else if (work.stolen) {
      if (work.isLeaf) work.expand()
      tree.child.result = Some(sum)
      false
    } else sys.error("unreachable")
  }
  
  class Worker(val root: Ptr) extends Thread {
    def findWork(tree: Ptr): Ptr = {
      val node = tree.child
      if (node.isLeaf) {
        if (node.completed) null // no further expansions
        else {
          // more work
          if (node.tryOwn(this)) tree
          else if (node.trySteal() && node.right.child.tryOwn(this)) node.right
          else findWork(tree)
        }
      } else {
        // descend deeper
        if (random.nextBoolean) {
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
        @tailrec def work(leaf: Ptr) {
          val nosteals = workloop(leaf)
          if (!nosteals) {
            if (leaf.child.left.child.tryOwn(this)) work(leaf.child.left)
          }
        }
        work(leaf)
        run()
      } else {
        // no more work
      }
    }
  }

  def run() {
    val root = new Ptr(null)
    val work = new Node(null, null, root)(0, 0, block, size)
    root.writeChild(work)

    workloop(root)
  }

}
























