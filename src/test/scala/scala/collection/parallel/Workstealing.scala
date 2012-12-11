package scala.collection.parallel



import sun.misc.Unsafe
import annotation.{tailrec, static}
import collection._



trait StatisticsBenchmark extends testing.Benchmark {
  protected def printStatistics(name: String, measurements: Seq[Long]) {
    val avg = (measurements.sum.toDouble / measurements.length)
    val stdev = math.sqrt(measurements.map(x => (x - avg) * (x - avg)).sum / (measurements.length - 1))

    println(name)
    println(" min:   " + measurements.min)
    println(" max:   " + measurements.max)
    println(" med:   " + measurements.sorted.apply(measurements.length / 2))
    println(" avg:   " + avg.toLong)
    println(" stdev: " + stdev)
  }
}


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


object Loop extends StatisticsBenchmark with Workloads {

  val size = sys.props("size").toInt
  var result = 0

  def run() {
    val sum = kernel(0, size, size)
    result = sum
  }
  
  override def runBenchmark(noTimes: Int): List[Long] = {
    val times = super.runBenchmark(noTimes)

    printStatistics("<All>", times)
    printStatistics("<Stable>", times.drop(5))

    times
  }
  
}


object AdvLoop extends StatisticsBenchmark with Workloads {

  final class Work(var from: Int, val step: Int, val until: Int) {
    final def CAS(ov: Int, nv: Int) = unsafe.compareAndSwapInt(this, Work.FROM_OFFSET, ov, nv)
  }

  object Work {
    @static val FROM_OFFSET = unsafe.objectFieldOffset(classOf[AdvLoop.Work].getDeclaredField("from"))
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


object StaticChunkingLoop extends StatisticsBenchmark with Workloads {

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


object BlockedSelfSchedulingLoop extends StatisticsBenchmark with Workloads {

  final class Work(@volatile var from: Int, val step: Int, val until: Int) {
    final def CAS(ov: Int, nv: Int) = unsafe.compareAndSwapInt(this, Work.FROM_OFFSET, ov, nv)
  }

  object Work {
    @static val FROM_OFFSET = unsafe.objectFieldOffset(classOf[BlockedSelfSchedulingLoop.Work].getDeclaredField("from"))
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


object StealLoop extends StatisticsBenchmark with Workloads {

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
        val range_t1 = /*READ*/child_t0.range
        if (Node.completed(range_t1)) false // already completed
        else {
          if (!Node.stolen(range_t1)) {
            if (child_t0.casRange(range_t1, Node.markStolen(range_t1))) expand() // marked stolen - now move on to node creation
            else expand() // wasn't marked stolen and failed marking stolen - retry
          } else { // already marked stolen
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
      val here = collection.immutable.HashMap(child.owner -> (Node.positiveProgress(child.range) - child.start + child.end - Node.until(child.range)))
      if (child.isLeaf) here
      else Seq(here, child.left.balance, child.right.balance).foldLeft(collection.immutable.HashMap[Owner, Int]()) {
        case (a, b) => a.merged(b) {
          case ((ak, av), (bk, bv)) => ((ak, av + bv))
        }
      }
    }

    def reduce(op: (T, T) => T): T = if (child.isLeaf) {
      child.result match {
        case (Some(lsum), Some(rsum)) => op(lsum, rsum)
      }
    } else {
      val leftres = child.left.reduce(op)
      val rightres = child.right.reduce(op)
      child.result match {
        case (Some(lsum), Some(rsum)) => op(op(op(lsum, leftres), rightres), rsum)
      }
    }

    def treeSize: Int = {
      if (child.isLeaf) 1
      else 1 + child.left.treeSize + child.right.treeSize
    }

  }

  object Ptr {
    @static val CHILD_OFFSET = unsafe.objectFieldOffset(classOf[StealLoop.Ptr].getDeclaredField("child"))
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

    def findWork(worker: Worker, tree: Ptr): Ptr = {
      val index = worker.index
      val total = worker.total
      val node = tree.child
      if (node.isLeaf) {
        if (Node.completed(node.range)) null // no further expansions
        else {
          // more work
          if (node.tryOwn(worker)) tree
          else if (node.trySteal()) {
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
        else if (max.child.trySteal()) {
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

  final class Node(val left: Ptr, val right: Ptr, val parent: Ptr)(val start: Int, val end: Int, @volatile var range: Long, @volatile var step: Int) {
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
    @volatile var result: (Option[T], Option[T]) = (None, None)
    var padding0: Int = 0 // <-- war story
    var padding1: Int = 0
    var padding2: Int = 0
    var padding3: Int = 0
    //var padding4: Int = 0

    final def casRange(ov: Long, nv: Long) = unsafe.compareAndSwapLong(this, Node.RANGE_OFFSET, ov, nv)
    final def casOwner(ov: Owner, nv: Owner) = unsafe.compareAndSwapObject(this, Node.OWNER_OFFSET, ov, nv)

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

    def trySteal(): Boolean = parent.expand()

    def newExpanded: Node = {
      val r = /*READ*/range
      val p = Node.positiveProgress(r)
      val u = Node.until(r)
      val remaining = u - p
      val firsthalf = remaining / 2
      val secondhalf = remaining - firsthalf
      val lptr = new Ptr(parent.level + 1)(null)
      val rptr = new Ptr(parent.level + 1)(null)
      val lnode = new Node(null, null, lptr)(p, p + firsthalf, Node.range(p, p + firsthalf), initialStep)
      val rnode = new Node(null, null, rptr)(p + firsthalf, u, Node.range(p + firsthalf, u), initialStep)
      lptr.writeChild(lnode)
      rptr.writeChild(rnode)
      val nnode = new Node(lptr, rptr, parent)(start, end, r, step)
      nnode.owner = this.owner
      nnode
    }

    def nodeString: String = "[%.2f%%] Node(%s)(%d, %d, %d, %d, %d)(res = %s) #%d".format(
      (Node.positiveProgress(range) - start + end - Node.until(range)).toDouble / size * 100,
      if (owner == null) "none" else "worker " + owner.index,
      start,
      end,
      Node.progress(range),
      Node.until(range),
      step,
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
    @static val RANGE_OFFSET = unsafe.objectFieldOffset(classOf[StealLoop.Node].getDeclaredField("range"))
    @static val OWNER_OFFSET = unsafe.objectFieldOffset(classOf[StealLoop.Node].getDeclaredField("owner"))

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
  var maxStep = sys.props.getOrElse("maxStep", "1024").toInt

  /** Returns true if completed with no stealing.
   *  Returns false if steal occurred.
   */
  def workloop(tree: Ptr): Boolean = {
    val work = tree.child
    var lsum = 0
    var rsum = 0
    var incCount = 0
    val incFreq = 2
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

    if (Node.completed(work.range)) {
      work.result = (Some(lsum), Some(rsum))
      //println(Thread.currentThread.getName + " -> " + work.start + " to " + work.progress + "; id=" + System.identityHashCode(work))
      true
    } else if (Node.stolen(work.range)) {
      // help expansion if necessary
      if (tree.child.isLeaf) tree.expand()
      tree.child.result = (Some(lsum), Some(rsum))
      //val work = tree.child
      //println(Thread.currentThread.getName + " -> " + work.start + " to " + work.progress + "; id=" + System.identityHashCode(work))
      false
    } else sys.error("unreachable: " + Node.progress(work.range) + ", " + Node.until(work.range))
  }
  
  class Worker(val root: Ptr, val index: Int, val total: Int) extends Thread {
    setName("Worker: " + index)

    @tailrec override final def run() {
      val leaf = strategy.findWork(this, root)
      if (leaf != null) {
        @tailrec def workAndDescend(leaf: Ptr) {
          val nosteals = workloop(leaf)
          if (!nosteals) {
            val subnode = strategy.chooseAsVictim(index, total, leaf)
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

  var root: Ptr = _
  val imbalance = collection.mutable.Map[Int, collection.mutable.ArrayBuffer[Int]]((0 until par) map (x => (x, collection.mutable.ArrayBuffer[Int]())): _*)
  val treesizes = collection.mutable.ArrayBuffer[Int]()

  def run() {
    root = new Ptr(0)(null)
    val work = new Node(null, null, root)(0, size, Node.range(0, size), initialStep)
    root.writeChild(work)

    val workers = for (i <- 0 until par) yield new Worker(root, i, par)
    for (w <- workers) w.start()
    for (w <- workers) w.join()
  }

  override def runBenchmark(noTimes: Int): List[Long] = {
    val times = super.runBenchmark(noTimes)
    val stabletimes = times.drop(5)

    println("...::: Last tree :::...")
    val balance = root.balance
    println(root.toString(0))
    println("result: " + root.reduce(_ + _))
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

  override def setUp() {
    if (inspectgc) {
      println("run starting...")
      Thread.sleep(500)
    }

    root = null

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

    val balance = root.balance
    val workmean = size / par
    for ((w, workdone) <- balance; if w != null) {
      imbalance(w.index) += math.abs(workmean - workdone)
    }
    treesizes += root.treeSize

    // debugging
    if (debugging) {
      assert(items.forall(_ == 1), "Each item processed: " + items.indexWhere(_ != 1) + ", " + items.find(_ != 1) + ": " + items.toSeq + "\n" + root.toString(0))
      println()
      println("-----------------------------------------------------------")
      println()
    }
  }

  var debugging = false

}
























