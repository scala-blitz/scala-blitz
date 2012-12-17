package scala.collection.parallel



import sun.misc.Unsafe
import annotation.tailrec
import collection._



object MultiplicationLoop extends StatisticsBenchmark {

  import Workloads._

  val size = sys.props("size").toInt
  var result = 0

  def run() {
    var i = 1
    var sum = 1
    while (i < 150000000) {
      sum *= i
      i += 1
    }
    result = sum
  }
  
}


object Loop extends StatisticsBenchmark {

  import Workloads._

  val repeats = sys.props.getOrElse("repeats", "1").toInt
  val size = sys.props("size").toInt
  var result = 0

  def run() {
    var i = 0
    while (i < repeats) {
      val sum = kernel(0, size, size)
      result = sum
      i += 1
    }
  }
  
  override def runBenchmark(noTimes: Int): List[Long] = {
    val times = super.runBenchmark(noTimes)

    printStatistics("<All>", times)
    printStatistics("<Stable>", times.drop(5))

    times
  }
  
}


object AdvLoop extends StatisticsBenchmark {

  import Workloads._

  final class Work(var from: Int, val step: Int, val until: Int) {
    final def CAS(ov: Int, nv: Int) = unsafe.compareAndSwapInt(this, Work.FROM_OFFSET, ov, nv)
  }

  object Work {
    val FROM_OFFSET = unsafe.objectFieldOffset(classOf[AdvLoop.Work].getDeclaredField("from"))
  }

  val unsafe = Utils.getUnsafe()
  val size = sys.props("size").toInt
  val block = sys.props("step").toInt
  var result = 0
  var work: Work = null

  def run() {
    work = new Work(0, block, size)
    var sum = 0
    var looping = true
    while (looping) {
      val currFrom = work.from
      val nextFrom = work.from + work.step
      if (currFrom < work.until) {
        work.from = nextFrom
        sum += kernel(currFrom, nextFrom, size)
      } else looping = false
    }
    result = sum
  }
  
}


object StaticChunkingLoop extends StatisticsBenchmark {

  import Workloads._

  val size = sys.props("size").toInt
  val par = sys.props("par").toInt

  final class Worker(val idx: Int) extends Thread {
    @volatile var result = 0
    override def run() {
      var i = 0
      val total = size / par
      result = kernel(idx * total, (idx + 1) * total, size)
    }
  }

  def run() {
    val workers = for (idx <- 0 until par) yield new Worker(idx)
    for (w <- workers) w.start()
    for (w <- workers) w.join()
    workers.map(_.result).sum
  }

  override def runBenchmark(noTimes: Int): List[Long] = {
    val times = super.runBenchmark(noTimes)

    printStatistics("<All>", times)
    printStatistics("<Stable>", times.drop(5))

    times
  }

}


object BlockedSelfSchedulingLoop extends StatisticsBenchmark {

  import Workloads._

  final class Work(@volatile var from: Int, val step: Int, val until: Int) {
    final def CAS(ov: Int, nv: Int) = unsafe.compareAndSwapInt(this, Work.FROM_OFFSET, ov, nv)
  }

  object Work {
    val FROM_OFFSET = unsafe.objectFieldOffset(classOf[BlockedSelfSchedulingLoop.Work].getDeclaredField("from"))
  }

  val unsafe = Utils.getUnsafe()
  val size = sys.props("size").toInt
  val block = sys.props("step").toInt
  val par = sys.props("par").toInt
  var work: Work = null

  final class Worker extends Thread {
    @volatile var result = 0
    override def run() {
      var sum = 0
      var looping = true
      while (looping) {
        val currFrom = work.from
        val nextFrom = math.min(work.until, work.from + work.step)
        if (currFrom < work.until) {
          if (work.CAS(currFrom, nextFrom)) sum += kernel(currFrom, nextFrom, size)
        } else looping = false
        result = sum
      }
    }
  }

  def run() {
    work = new Work(0, block, size)
    val workers = for (i <- 0 until par) yield new Worker
    for (w <- workers) w.start()
    for (w <- workers) w.join()
  }

  override def runBenchmark(noTimes: Int): List[Long] = {
    val times = super.runBenchmark(noTimes)

    printStatistics("<All>", times)
    printStatistics("<Stable>", times.drop(5))

    times
  }
  
}


object StealLoop extends StatisticsBenchmark {

  import Workloads._

  final class Ptr(val up: Ptr, val level: Int)(@volatile var child: Node) {
    def casChild(ov: Node, nv: Node) = unsafe.compareAndSwapObject(this, Ptr.CHILD_OFFSET, ov, nv)
    def writeChild(nv: Node) = unsafe.putObjectVolatile(this, Ptr.CHILD_OFFSET, nv)

    /** Try to expand node and return true if node was expanded.
     *  Return false if node was completed.
     */
    @tailrec def expand(): Boolean = {
      val child_t0 = /*READ*/child
      if (!child_t0.isLeaf) true else { // already expanded
        // first set progress to -progress
        val range_t1 = /*READ*/child_t0.range
        if (Node.completed(range_t1)) false // already completed
        else {
          if (!Node.stolen(range_t1)) {
            if (child_t0.casRange(range_t1, Node.markStolen(range_t1))) expand() // marked stolen - now move on to node creation
            else expand() // wasn't marked stolen and failed marking stolen - retry
          } else { // already marked stolen
            // node effectively immutable (except for `lresult`, `rresult` and `result`) - expand it
            val expanded = child_t0.newExpanded(this)
            if (casChild(child_t0, expanded)) true // try to replace with expansion
            else expand() // failure (spurious or due to another expand) - retry
          }
        }
      }
    }

    def toString(l: Int): String = "\t" * level + "Ptr" + level + " -> " + child.toString(l)

    def balance: collection.immutable.HashMap[Owner, Int] = {
      val here = collection.immutable.HashMap(child.owner -> (Node.positiveProgress(child.range) - child.start + child.end - Node.until(child.range)))
      if (child.isLeaf) here
      else Seq(here, child.left.balance, child.right.balance).foldLeft(collection.immutable.HashMap[Owner, Int]()) {
        case (a, b) => a.merged(b) {
          case ((ak, av), (bk, bv)) => ((ak, av + bv))
        }
      }
    }

    def reduce(op: (T, T) => T): T = if (child.isLeaf) {
      op(child.lresult, child.rresult)
    } else {
      val leftsubres = child.left.reduce(op)
      val rightsubres = child.right.reduce(op)
      op(op(op(child.lresult, leftsubres), rightsubres), child.rresult)
    }

    def treeSize: Int = {
      if (child.isLeaf) 1
      else 1 + child.left.treeSize + child.right.treeSize
    }

  }

  object Ptr {
    val CHILD_OFFSET = unsafe.objectFieldOffset(classOf[StealLoop.Ptr].getDeclaredField("child"))
  }

  type T = Int
  type Owner = Worker

  trait Strategy {

    /** Finds work in the tree for the given worker, which is one out of `total` workers.
     *  This search may include stealing work.
     */
    def findWork(worker: Worker, tree: Ptr): Ptr

   /** Returns true if the worker labeled with `index` with a total of
    *  `total` workers should go left at level `level`.
    *  Returns false otherwise.
    */
    def choose(index: Int, total: Int, tree: Ptr): Boolean

    /** Which node the stealer takes at this level. */
    def chooseAsStealer(index: Int, total: Int, tree: Ptr): Ptr

    /** Which node the victim takes at this level. */
    def chooseAsVictim(index: Int, total: Int, tree: Ptr): Ptr

  }

  trait FindFirstStrategy extends Strategy {

    final def findWork(worker: Worker, tree: Ptr): Ptr = {
      val index = worker.index
      val total = worker.total
      val node = tree.child
      if (node.isLeaf) {
        if (Node.completed(node.range)) null // no further expansions
        else {
          // more work
          if (node.tryOwn(worker)) tree
          else if (node.trySteal(tree)) {
            val subnode = chooseAsStealer(index, total, tree)
            if (subnode.child.tryOwn(worker)) subnode
            else findWork(worker, tree)
          } else findWork(worker, tree)
        }
      } else {
        // descend deeper
        if (choose(index, total, tree)) {
          val ln = findWork(worker, node.left)
          if (ln != null) ln else findWork(worker, node.right)
        } else {
          val rn = findWork(worker, node.right)
          if (rn != null) rn else findWork(worker, node.left)
        }
      }
    }

  }

  object AssignTop extends FindFirstStrategy {

    /** Returns true iff the level of the tree is such that: 2^level < total */
    final def isTreeTop(total: Int, level: Int): Boolean = (1 << level) < total
    
    /** Returns true iff the worker should first go left at this level of the tree top. */
    final def chooseInTreeTop(index: Int, level: Int): Boolean = ((index >> level) & 0x1) == 0

    def choose(index: Int, total: Int, tree: Ptr): Boolean = {
      val level = tree.level
      if (isTreeTop(total, level)) {
        chooseInTreeTop(index, level)
      } else random.nextBoolean
    }

    def chooseAsStealer(index: Int, total: Int, tree: Ptr): Ptr = {
      val level = tree.level
      if (isTreeTop(total, level)) {
        if (chooseInTreeTop(index, level)) tree.child.left
        else tree.child.right
      } else tree.child.right
    }

    def chooseAsVictim(index: Int, total: Int, tree: Ptr): Ptr = {
      val level = tree.level
      if (isTreeTop(total, level)) {
        if (chooseInTreeTop(index, level)) tree.child.left
        else tree.child.right
      } else tree.child.left
    }

  }

  object AssignTopLeaf extends FindFirstStrategy {

    final def isTreeTop(total: Int, level: Int): Boolean = (1 << level) < total
    
    final def chooseInTreeTop(index: Int, level: Int): Boolean = ((index >> level) & 0x1) == 0

    def choose(index: Int, total: Int, tree: Ptr): Boolean = {
      val level = tree.level
      if (isTreeTop(total, level) && tree.child.isLeaf) {
        chooseInTreeTop(index, level)
      } else random.nextBoolean
    }

    def chooseAsStealer(index: Int, total: Int, tree: Ptr): Ptr = {
      val level = tree.level
      if (isTreeTop(total, level)) {
        if (chooseInTreeTop(index, level)) tree.child.left
        else tree.child.right
      } else tree.child.right
    }

    def chooseAsVictim(index: Int, total: Int, tree: Ptr): Ptr = {
      val level = tree.level
      if (isTreeTop(total, level)) {
        if (chooseInTreeTop(index, level)) tree.child.left
        else tree.child.right
      } else tree.child.left
    }

  }

  object Assign extends FindFirstStrategy {

    private def log2(x: Int) = {
      var v = x
      var r = -1
      while (v != 0) {
        r += 1
        v = v >>> 1
      }
      r
    }

    def choose(index: Int, total: Int, tree: Ptr): Boolean = {
      val levelmod = tree.level % log2(total)
      ((index >> levelmod) & 0x1) == 0
    }

    def chooseAsStealer(index: Int, total: Int, tree: Ptr): Ptr = {
      if (choose(index, total, tree)) tree.child.left
      else tree.child.right
    }

    def chooseAsVictim(index: Int, total: Int, tree: Ptr): Ptr = {
      if (choose(index, total, tree)) tree.child.left
      else tree.child.right
    }

  }

  object RandomWalk extends FindFirstStrategy {

    def choose(index: Int, total: Int, tree: Ptr): Boolean = random.nextBoolean

    def chooseAsStealer(index: Int, total: Int, tree: Ptr) = tree.child.right

    def chooseAsVictim(index: Int, total: Int, tree: Ptr) = tree.child.left

  }

  object RandomAll extends FindFirstStrategy {

    def choose(index: Int, total: Int, tree: Ptr): Boolean = random.nextBoolean

    def chooseAsStealer(index: Int, total: Int, tree: Ptr) = if (random.nextBoolean) tree.child.left else tree.child.right

    def chooseAsVictim(index: Int, total: Int, tree: Ptr) = if (random.nextBoolean) tree.child.left else tree.child.right

  }

  object Predefined extends FindFirstStrategy {

    def choose(index: Int, total: Int, tree: Ptr): Boolean = true

    def chooseAsStealer(index: Int, total: Int, tree: Ptr) = tree.child.right

    def chooseAsVictim(index: Int, total: Int, tree: Ptr) = tree.child.left

  }

  object FindMax extends Strategy {

    @tailrec def findWork(worker: Worker, tree: Ptr) = {
      def search(current: Ptr): Ptr = if (current.child.isLeaf) current else {
        val left = search(current.child.left)
        val rght = search(current.child.right)
        val leftwork = left.child.workRemaining
        val rghtwork = rght.child.workRemaining
        if (leftwork > rghtwork) left else rght
      }

      val max = search(tree)
      if (max.child.workRemaining > 0) {
        if (max.child.tryOwn(worker)) max
        else if (max.child.trySteal(max)) {
          val subnode = chooseAsStealer(worker.index, worker.total, max)
          if (subnode.child.tryOwn(worker)) subnode
          else findWork(worker, tree)
        } else findWork(worker, tree)
      } else null
    }

    def choose(index: Int, total: Int, tree: Ptr) = sys.error("never called")

    def chooseAsStealer(index: Int, total: Int, tree: Ptr) = tree.child.right

    def chooseAsVictim(index: Int, total: Int, tree: Ptr) = tree.child.left

  }

  val strategies = List(FindMax, AssignTopLeaf, AssignTop, Assign, RandomWalk, RandomAll, Predefined) map (x => (x.getClass.getSimpleName, x)) toMap

  final class Node(val left: Ptr, val right: Ptr)(val start: Int, val end: Int, @volatile var range: Long, @volatile var step: Int) {
    /*
     * progress: start -> x1 -> ... -> xn -> until
     *                                    -> -(xn + 1)
     * x1 > start, x(i+1) > xi, until > xn
     * 
     * owner: null -> v, v != null
     *
     * lresult, rresult: null --(if owner != null)--> x
     *
     * result: null --(if owner != null)--> None --> Some(v)
     */

    @volatile var owner: Owner = null
    @volatile var lresult: T = null.asInstanceOf[T]
    @volatile var rresult: T = null.asInstanceOf[T]
    @volatile var result: Option[T] = null
    var padding0: Int = 0 // <-- war story
    var padding1: Int = 0
    var padding2: Int = 0
    //var padding3: Int = 0
    //var padding4: Int = 0

    final def casRange(ov: Long, nv: Long) = unsafe.compareAndSwapLong(this, Node.RANGE_OFFSET, ov, nv)
    final def casOwner(ov: Owner, nv: Owner) = unsafe.compareAndSwapObject(this, Node.OWNER_OFFSET, ov, nv)
    final def casResult(ov: Option[T], nv: Option[T]) = unsafe.compareAndSwapObject(this, Node.RESULT_OFFSET, ov, nv)

    def nonEmpty = (end - start) > 0

    def isLeaf = left eq null

    def workRemaining = {
      val r = /*READ*/range
      val p = Node.progress(r)
      val u = Node.until(r)
      u - p
    }

    @tailrec def tryOwn(thiz: Owner): Boolean = {
      val currowner = /*READ*/owner
      if (currowner != null) false
      else if (casOwner(currowner, thiz)) true
      else tryOwn(thiz)
    }

    def trySteal(parent: Ptr): Boolean = parent.expand()

    def newExpanded(parent: Ptr): Node = {
      val r = /*READ*/range
      val p = Node.positiveProgress(r)
      val u = Node.until(r)
      val remaining = u - p
      val firsthalf = remaining / 2
      val secondhalf = remaining - firsthalf
      val lnode = new Node(null, null)(p, p + firsthalf, Node.range(p, p + firsthalf), initialStep)
      val rnode = new Node(null, null)(p + firsthalf, u, Node.range(p + firsthalf, u), initialStep)
      val lptr = new Ptr(parent, parent.level + 1)(lnode)
      val rptr = new Ptr(parent, parent.level + 1)(rnode)
      val nnode = new Node(lptr, rptr)(start, end, r, step)
      nnode.owner = this.owner
      nnode
    }

    def nodeString: String = "[%.2f%%] Node(%s)(%d, %d, %d, %d, %d)(lres = %s, rres = %s, res = %s) #%d".format(
      (Node.positiveProgress(range) - start + end - Node.until(range)).toDouble / size * 100,
      if (owner == null) "none" else "worker " + owner.index,
      start,
      end,
      Node.progress(range),
      Node.until(range),
      step,
      lresult,
      rresult,
      result,
      System.identityHashCode(this)
    )

    def toString(lev: Int): String = {
       nodeString + (if (!isLeaf) {
        "\n" + left.toString(lev + 1) + right.toString(lev + 1)
      } else "\n")
    }
  }

  object Node {
    val RANGE_OFFSET = unsafe.objectFieldOffset(classOf[StealLoop.Node].getDeclaredField("range"))
    val OWNER_OFFSET = unsafe.objectFieldOffset(classOf[StealLoop.Node].getDeclaredField("owner"))
    val RESULT_OFFSET = unsafe.objectFieldOffset(classOf[StealLoop.Node].getDeclaredField("result"))

    def range(p: Int, u: Int): Long = (p.toLong << 32) | u

    def stolen(r: Long): Boolean = progress(r) < 0

    def progress(r: Long): Int = {
      ((r & 0xffffffff00000000L) >>> 32).toInt
    }

    def until(r: Long): Int = {
      (r & 0x00000000ffffffffL).toInt
    }

    def completed(r: Long): Boolean = {
      val p = progress(r)
      val u = until(r)
      p == u
    }

    def positiveProgress(r: Long): Int = {
      val p = progress(r)
      if (p >= 0) p
      else -(p) - 1
    }

    def markStolen(r: Long): Long = {
      val p = progress(r)
      val u = until(r)
      val stolenp = -p - 1
      range(stolenp, u)
    }
  }

  def random = scala.concurrent.forkjoin.ThreadLocalRandom.current
  val unsafe = Utils.getUnsafe()

  val size = sys.props("size").toInt
  val initialStep = sys.props("step").toInt
  val par = sys.props("par").toInt
  val inspectgc = sys.props.getOrElse("inspectgc", "false").toBoolean
  val strategy: Strategy = strategies(sys.props("strategy"))
  val maxStep = sys.props.getOrElse("maxStep", "1024").toInt
  val repeats = sys.props.getOrElse("repeats", "1").toInt
  val incrementFrequency = 1

  /** Returns true if completed with no stealing.
   *  Returns false if steal occurred.
   */
  def workloop(tree: Ptr): Boolean = {
    // do some work
    val work = tree.child
    var lsum = 0
    var rsum = 0
    var incCount = 0
    val incFreq = incrementFrequency
    var looping = true
    while (looping) {
      val currstep = /*READ*/work.step
      val currrange = /*READ*/work.range
      val p = Node.progress(currrange)
      val u = Node.until(currrange)

      if (!Node.stolen(currrange) && !Node.completed(currrange)) {
        if (random.nextBoolean) {
          val newp = math.min(u, p + currstep)
          val newrange = Node.range(newp, u)

          // do some work on the left
          if (work.casRange(currrange, newrange)) lsum = lsum + kernel(p, newp, size)
        } else {
          val newu = math.max(p, u - currstep)
          val newrange = Node.range(p, newu)

          // do some work on the right
          if (work.casRange(currrange, newrange)) rsum = kernel(newu, u, size) + rsum
        }

        // update step
        incCount = (incCount + 1) % incFreq
        if (incCount == 0) work.step = math.min(maxStep, currstep * 2)
      } else looping = false
    }

    // complete node information
    completeNode(lsum, rsum, tree)
  }

  def completeNode(lsum: T, rsum: T, tree: Ptr): Boolean = {
    val work = tree.child

    val wasCompleted = if (Node.completed(work.range)) {
      work.lresult = lsum
      work.rresult = rsum
      while (work.result == null) work.casResult(null, None)
      //println(Thread.currentThread.getName + " -> " + work.start + " to " + work.progress + "; id=" + System.identityHashCode(work))
      true
    } else if (Node.stolen(work.range)) {
      // help expansion if necessary
      if (tree.child.isLeaf) tree.expand()
      tree.child.lresult = lsum
      tree.child.rresult = rsum
      while (tree.child.result == null) tree.child.casResult(null, None)
      //val work = tree.child
      //println(Thread.currentThread.getName + " -> " + work.start + " to " + work.progress + "; id=" + System.identityHashCode(work))
      false
    } else sys.error("unreachable: " + Node.progress(work.range) + ", " + Node.until(work.range))

    // push result up as far as possible
    pushUp(tree)

    wasCompleted
  }
  
  @tailrec def pushUp(tree: Ptr) {
    val r = /*READ*/tree.child.result
    r match {
      case null => // we're done, owner did not finish his work yet
      case Some(_) => // we're done, somebody else already pushed up
      case None =>
        val finalresult =
          if (tree.child.isLeaf) Some(tree.child.lresult + tree.child.rresult)
          else {
            // check if result already set for children
            val leftresult = /*READ*/tree.child.left.child.result
            val rightresult = /*READ*/tree.child.right.child.result
            (leftresult, rightresult) match {
              case (Some(lr), Some(rr)) =>
                Some(tree.child.lresult + lr + rr + tree.child.rresult)
              case (_, _) => // we're done, some of the children did not finish yet
                None
            }
          }

        if (finalresult.nonEmpty) if (tree.child.casResult(r, finalresult)) {
          // if at root, notify completion, otherwise go one level up
          if (tree.up == null) tree.synchronized {
            tree.notifyAll()
          } else pushUp(tree.up)
        } else pushUp(tree) // retry
    }
  }

  trait Worker {
    def index: Int
    def total: Int
    def getName: String
  }

  object Invoker extends Worker {
    def index = 0
    def total = par
    def getName = "Invoker"
  }

  var lastroot: Ptr = _
  val imbalance = collection.mutable.Map[Int, collection.mutable.ArrayBuffer[Int]]((0 until par) map (x => (x, collection.mutable.ArrayBuffer[Int]())): _*)
  val treesizes = collection.mutable.ArrayBuffer[Int]()

  @tailrec def workUntilNoWork(w: Worker, root: Ptr) {
    val leaf = strategy.findWork(w, root)
    if (leaf != null) {
      @tailrec def workAndDescend(leaf: Ptr) {
        val nosteals = workloop(leaf)
        if (!nosteals) {
          val subnode = strategy.chooseAsVictim(w.index, w.total, leaf)
          if (subnode.child.tryOwn(w)) workAndDescend(subnode)
        }
      }
      workAndDescend(leaf)
      workUntilNoWork(w, root)
    } else {
      // no more work
    }
  }

  class WorkerThread(val root: Ptr, val index: Int, val total: Int) extends Thread with Worker {
    setName("Worker: " + index)

    override final def run() {
      workUntilNoWork(this, root)
    }
  }

  def dispatchWork0(root: Ptr) {
    var i = 1
    while (i < par) {
      val w = new WorkerThread(root, i, par)
      w.start()
      i += 1
    }
  }

  def joinWork0(root: Ptr) = {
    var r = /*READ*/root.child.result
    if (r == null || r.isEmpty) root.synchronized {
      r = /*READ*/root.child.result
      while (r == null || r.isEmpty) {
        r = /*READ*/root.child.result
        root.wait()
      }
    }
  }

  import scala.concurrent.forkjoin._

  val fjpool = new ForkJoinPool()

  class WorkerTask(val root: Ptr, val index: Int, val total: Int) extends RecursiveAction with Worker {
    def getName = "WorkerTask(" + index + ")"

    def compute() {
      workUntilNoWork(this, root)
    }
  }

  def dispatchWork1(root: Ptr) {
    var i = 1
    while (i < par) {
      val w = new WorkerTask(root, i, par)
      fjpool.execute(w)
      i += 1
    }
  }

  def joinWork1(root: Ptr) = {
    var r = /*READ*/root.child.result
    if (r == null || r.isEmpty) root.synchronized {
      r = /*READ*/root.child.result
      while (r == null || r.isEmpty) {
        root.wait()
        r = /*READ*/root.child.result
      }
    }
  }

  def invokeOperation() {
    // create workstealing tree
    val work = new Node(null, null)(0, size, Node.range(0, size), initialStep)
    val root = new Ptr(null, 0)(work)
    work.tryOwn(Invoker)

    // let other workers know there's something to do
    dispatchWork1(root)

    // piggy-back the caller into doing work
    if (!workloop(root)) workUntilNoWork(Invoker, root)

    // synchronize in case there's some other worker just
    // about to complete work
    joinWork1(root)

    lastroot = root
  }

  override def runBenchmark(noTimes: Int): List[Long] = {
    val times = super.runBenchmark(noTimes)
    val stabletimes = times.drop(5)

    println("...::: Last tree :::...")
    val balance = lastroot.balance
    println(lastroot.toString(0))
    println("result: " + lastroot.reduce(_ + _))
    println(balance.toList.sortBy(_._1.getName).map(p => p._1 + ": " + p._2).mkString("...::: Work balance :::...\n", "\n", ""))
    println("total: " + balance.foldLeft(0)(_ + _._2))
    println()

    println("...::: Statistics :::...")
    println("strategy: " + strategy.getClass.getSimpleName)
    for ((workerindex, diffs) <- imbalance.toList.sortBy(_._1).headOption) printStatistics("<worker " + workerindex + " imbalance>", diffs.map(_.toLong).toList)
    printStatistics("<Tree size>", treesizes.map(_.toLong).toList)
    printStatistics("<All>", times)
    printStatistics("<Stable>", stabletimes)

    times
  }

  def run() {
    var i = 0
    while (i < repeats) {
      invokeOperation()
      i += 1
    }
  }

  override def setUp() {
    if (inspectgc) {
      println("run starting...")
      Thread.sleep(500)
    }

    lastroot = null

    // debugging
    if (debugging) {
      var i = 0
      while (i < size) {
        items(i) = 0
        i += 1
      }
    }
  }

  override def tearDown() {
    if (inspectgc) {
      Thread.sleep(500)
      println("run completed...")
    }

    val balance = lastroot.balance
    val workmean = size / par
    for ((w, workdone) <- balance; if w != null) {
      imbalance(w.index) += math.abs(workmean - workdone)
    }
    treesizes += lastroot.treeSize

    // debugging
    if (debugging) {
      assert(items.forall(_ == 1), "Each item processed: " + items.indexWhere(_ != 1) + ", " + items.find(_ != 1) + ": " + items.toSeq + "\n" + lastroot.toString(0))
      println()
      println("-----------------------------------------------------------")
      println()
    }
  }

  var debugging = false

}
























