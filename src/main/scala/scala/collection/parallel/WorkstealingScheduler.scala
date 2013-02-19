package scala.collection.parallel



import sun.misc.Unsafe
import annotation.tailrec
import collection._



final class WorkstealingScheduler {

  import WorkstealingScheduler.{Worker, Ptr, Node, Strategy}
  import WorkstealingScheduler.{FindMax, AssignTopLeaf, AssignTop, Assign, RandomWalk, RandomAll, Predefined}
  import WorkstealingScheduler.{localRandom, initialStep}

  val strategies = List(FindMax, AssignTopLeaf, AssignTop, Assign, RandomWalk, RandomAll, Predefined) map (x => (x.getClass.getSimpleName, x)) toMap
  val size = sys.props("size").toInt
  val par = sys.props("par").toInt
  val inspectgc = sys.props.getOrElse("inspectgc", "false").toBoolean
  val strategy: Strategy = strategies(sys.props("strategy"))
  val maxStep = sys.props.getOrElse("maxStep", "1024").toInt
  val repeats = sys.props.getOrElse("repeats", "1").toInt
  val starterThread = sys.props("starterThread")
  val starterCooldown = sys.props("starterCooldown").toInt
  val invocationMethod = sys.props("invocationMethod")
  val incrementFrequency = 1

  def completeNode[N <: Node[N, T, R], T, R](lsum: R, rsum: R, tree: Ptr[N, T, R], kernel: Kernel[N, T, R]): Boolean = {
    val work = tree.child

    val state_t0 = work.state
    val wasCompleted = if (state_t0 eq Node.Completed) {
      work.lresult = lsum
      work.rresult = rsum
      while (work.result == null) work.casResult(null, None)
      //println(Thread.currentThread.getName + " -> " + work.start + " to " + work.progress + "; id=" + System.identityHashCode(work))
      true
    } else if (state_t0 eq Node.StolenOrExpanded) {
      // help expansion if necessary
      if (tree.child.isLeaf) tree.expand()
      tree.child.lresult = lsum
      tree.child.rresult = rsum
      while (tree.child.result == null) tree.child.casResult(null, None)
      //val work = tree.child
      //println(Thread.currentThread.getName + " -> " + work.start + " to " + work.progress + "; id=" + System.identityHashCode(work))
      false
    } else sys.error("unreachable: " + state_t0 + ", " + work.toString(this)(0))

    // push result up as far as possible
    pushUp(tree, kernel)

    wasCompleted
  }
  
  @tailrec final def pushUp[N <: Node[N, T, R], T, R](tree: Ptr[N, T, R], k: Kernel[N, T, R]) {
    val r = /*READ*/tree.child.result
    r match {
      case null =>
        // we're done, owner did not finish his work yet
      case Some(_) =>
        // we're done, somebody else already pushed up
      case None =>
        val finalresult =
          if (tree.child.isLeaf) Some(k.combine(tree.child.lresult, tree.child.rresult))
          else {
            // check if result already set for children
            val leftresult = /*READ*/tree.child.left.child.result
            val rightresult = /*READ*/tree.child.right.child.result
            (leftresult, rightresult) match {
              case (Some(lr), Some(rr)) =>
                val r = k.combine(tree.child.lresult, k.combine(lr, k.combine(rr, tree.child.rresult)))
                Some(r)
              case (_, _) => // we're done, some of the children did not finish yet
                None
            }
          }

        if (finalresult.nonEmpty) if (tree.child.casResult(r, finalresult)) {
          // if at root, notify completion, otherwise go one level up
          if (tree.up == null) tree.synchronized {
            tree.notifyAll()
          } else pushUp(tree.up, k)
        } else pushUp(tree, k) // retry
    }
  }

  object Invoker extends Worker {
    def index = 0
    def total = par
    def getName = "Invoker"
  }

  var lastroot: Ptr[_, _, _] = _
  val imbalance = collection.mutable.Map[Int, collection.mutable.ArrayBuffer[Int]]((0 until par) map (x => (x, collection.mutable.ArrayBuffer[Int]())): _*)
  val treesizes = collection.mutable.ArrayBuffer[Int]()

  @tailrec final def workUntilNoWork[N <: Node[N, T, R], T, R](w: Worker, root: Ptr[N, T, R], kernel: Kernel[N, T, R]) {
    val leaf = strategy.findWork[N, T, R](w, root)
    if (leaf != null) {
      @tailrec def workAndDescend(leaf: Ptr[N, T, R]) {
        val nosteals = kernel.workOn(this)(leaf, size)
        if (!nosteals) {
          val subnode = strategy.chooseAsVictim[N, T, R](w.index, w.total, leaf)
          if (subnode.child.tryOwn(w)) workAndDescend(subnode)
        }
      }
      workAndDescend(leaf)
      workUntilNoWork(w, root, kernel)
    } else {
      // no more work
    }
  }

  class WorkerThread[N <: Node[N, T, R], T, R](val root: Ptr[N, T, R], val index: Int, val total: Int, kernel: Kernel[N, T, R]) extends Thread with Worker {
    setName("Worker: " + index)

    override final def run() {
      workUntilNoWork(this, root, kernel)
    }
  }

  def dispatchWorkT[N <: Node[N, T, R], T, R](root: Ptr[N, T, R], kernel: Kernel[N, T, R]) {
    var i = 1
    while (i < par) {
      val w = new WorkerThread[N, T, R](root, i, par, kernel)
      w.start()
      i += 1
    }
  }

  def joinWorkT[N <: Node[N, T, R], T, R](root: Ptr[N, T, R]) = {
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

  class WorkerTask[N <: Node[N, T, R], T, R](val root: Ptr[N, T, R], val index: Int, val total: Int, kernel: Kernel[N, T, R]) extends RecursiveAction with Worker {
    def getName = "WorkerTask(" + index + ")"

    def compute() {
      workUntilNoWork(this, root, kernel)
    }
  }

  def dispatchWorkFJ[N <: Node[N, T, R], T, R](root: Ptr[N, T, R], kernel: Kernel[N, T, R]) {
    var i = 1
    while (i < par) {
      val w = new WorkerTask(root, i, par, kernel)
      fjpool.execute(w)
      i += 1
    }
  }

  def joinWorkFJ[N <: Node[N, T, R], T, R](root: Ptr[N, T, R]) = {
    var r = /*READ*/root.child.result
    if (r == null || r.isEmpty) root.synchronized {
      r = /*READ*/root.child.result
      while (r == null || r.isEmpty) {
        root.wait()
        r = /*READ*/root.child.result
      }
    }
  }

  def invokeParallelOperation[N <: Node[N, T, R], T, R](progress: Int, result: R, kernel: Kernel[N, T, R]): R = {
    // create workstealing tree
    val root = kernel.newRoot
    val work = root.child
    work.tryOwn(Invoker)

    // let other workers know there's something to do
    dispatchWorkFJ(root, kernel)

    // piggy-back the caller into doing work
    if (!kernel.workOn(this)(root, size)) workUntilNoWork(Invoker, root, kernel)

    // synchronize in case there's some other worker just
    // about to complete work
    joinWorkFJ(root)

    lastroot = root

    kernel.combine(result, root.child.result.get)
  }

}


object WorkstealingScheduler {

  type Owner = Worker

  trait Worker {
    def index: Int
    def total: Int
    def getName: String
  }

  abstract class Node[N <: Node[N, T, R], T, R](val left: Ptr[N, T, R], val right: Ptr[N, T, R]) {
    @volatile var owner: Owner = null
    @volatile var lresult: R = null.asInstanceOf[R]
    @volatile var rresult: R = null.asInstanceOf[R]
    @volatile var result: Option[R] = null

    final def casOwner(ov: Owner, nv: Owner) = unsafe.compareAndSwapObject(this, Node.OWNER_OFFSET, ov, nv)
    final def casResult(ov: Option[R], nv: Option[R]) = unsafe.compareAndSwapObject(this, Node.RESULT_OFFSET, ov, nv)

    final def isLeaf = left eq null

    @tailrec final def tryOwn(thiz: Owner): Boolean = {
      val currowner = /*READ*/owner
      if (currowner != null) false
      else if (casOwner(currowner, thiz)) true
      else tryOwn(thiz)
    }

    final def trySteal(parent: Ptr[N, T, R]): Boolean = parent.expand()

    final def toString(ws: WorkstealingScheduler)(lev: Int): String = {
       nodeString[N, T, R](ws)(this) + (if (!isLeaf) {
        "\n" + left.toString(ws)(lev + 1) + right.toString(ws)(lev + 1)
      } else "\n")
    }

    /* abstract members */

    def workRemaining: Int

    def newExpanded(parent: Ptr[N, T, R]): N

    def state: Node.State

    def advance(step: Int): Int

    def next: T

    def markStolen(): Boolean

  }

  abstract class IndexNode[IN <: IndexNode[IN, T, R], T, R](l: Ptr[IN, T, R], r: Ptr[IN, T, R])(val start: Int, val end: Int, @volatile var range: Long, @volatile var step: Int)
  extends Node[IN, T, R](l, r) {
    var padding0: Int = 0 // <-- war story
    var padding1: Int = 0
    //var padding2: Int = 0
    //var padding3: Int = 0
    //var padding4: Int = 0

    final def casRange(ov: Long, nv: Long) = unsafe.compareAndSwapLong(this, IndexNode.RANGE_OFFSET, ov, nv)

    final def workRemaining = {
      val r = /*READ*/range
      val p = IndexNode.progress(r)
      val u = IndexNode.until(r)
      u - p
    }

    final def state = {
      val range_t0 = /*READ*/range
      if (IndexNode.completed(range_t0)) Node.Completed
      else if (IndexNode.stolen(range_t0)) Node.StolenOrExpanded
      else Node.AvailableOrOwned
    }

    final def advance(step: Int): Int = {
      val range_t0 = /*READ*/range
      if (IndexNode.stolen(range_t0) || IndexNode.completed(range_t0)) -1
      else {
        val p = IndexNode.progress(range_t0)
        val u = IndexNode.until(range_t0)
        val newp = math.min(u, p + step)
        if (casRange(range_t0, IndexNode.range(newp, u))) newp - p
        else -1
      }
    }

    final def markStolen(): Boolean = {
      val range_t0 = /*READ*/range
      if (IndexNode.completed(range_t0) || IndexNode.stolen(range_t0)) false
      else casRange(range_t0, IndexNode.markStolen(range_t0))
    }

  }

  final class RangeNode[R](l: Ptr[RangeNode[R], Int, R], r: Ptr[RangeNode[R], Int, R])(s: Int, e: Int, rn: Long, st: Int)
  extends IndexNode[RangeNode[R], Int, R](l, r)(s, e, rn, st) {
    var lindex = start

    def next: Int = {
      val x = lindex
      lindex += 1
      x
    }

    def newExpanded(parent: Ptr[RangeNode[R], Int, R]): RangeNode[R] = {
      val r = /*READ*/range
      val p = IndexNode.positiveProgress(r)
      val u = IndexNode.until(r)
      val remaining = u - p
      val firsthalf = remaining / 2
      val secondhalf = remaining - firsthalf
      val lnode = new RangeNode[R](null, null)(p, p + firsthalf, IndexNode.range(p, p + firsthalf), initialStep)
      val rnode = new RangeNode[R](null, null)(p + firsthalf, u, IndexNode.range(p + firsthalf, u), initialStep)
      val lptr = new Ptr[RangeNode[R], Int, R](parent, parent.level + 1)(lnode)
      val rptr = new Ptr[RangeNode[R], Int, R](parent, parent.level + 1)(rnode)
      val nnode = new RangeNode(lptr, rptr)(start, end, r, step)
      nnode.owner = this.owner
      nnode
    }

  }

  object IndexNode {
    val RANGE_OFFSET = unsafe.objectFieldOffset(classOf[IndexNode[_, _, _]].getDeclaredField("range"))

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

  object Node {
    val OWNER_OFFSET = unsafe.objectFieldOffset(classOf[Node[_, _, _]].getDeclaredField("owner"))
    val RESULT_OFFSET = unsafe.objectFieldOffset(classOf[Node[_, _, _]].getDeclaredField("result"))

    trait State

    val Completed = new State {
      override def toString = "Completed"
    }

    val StolenOrExpanded = new State {
      override def toString = "StolenOrExpanded"
    }

    val AvailableOrOwned = new State {
      override def toString = "AvailableOrOwned"
    }
  }

  final class Ptr[N <: Node[N, T, R], T, R](val up: Ptr[N, T, R], val level: Int)(@volatile var child: N) {
    def casChild(ov: N, nv: N) = unsafe.compareAndSwapObject(this, Ptr.CHILD_OFFSET, ov, nv)
    def writeChild(nv: N) = unsafe.putObjectVolatile(this, Ptr.CHILD_OFFSET, nv)

    /** Try to expand node and return true if node was expanded.
     *  Return false if node was completed.
     */
    @tailrec def expand(): Boolean = {
      val child_t0 = /*READ*/child
      if (!child_t0.isLeaf) true else { // already expanded
        // first set progress to -progress
        val state_t1 = child_t0.state
        if (state_t1 eq Node.Completed) false // already completed
        else {
          if (state_t1 ne Node.StolenOrExpanded) {
            if (child_t0.markStolen()) expand() // marked stolen - now move on to node creation
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

    def toString(ws: WorkstealingScheduler)(l: Int): String = "\t" * level + "Ptr" + level + " -> " + child.toString(ws)(l)

    def balance: collection.immutable.HashMap[Owner, Int] = {
      val here = collection.immutable.HashMap(child.owner -> workDone(child))
      if (child.isLeaf) here
      else Seq(here, child.left.balance, child.right.balance).foldLeft(collection.immutable.HashMap[Owner, Int]()) {
        case (a, b) => a.merged(b) {
          case ((ak, av), (bk, bv)) => ((ak, av + bv))
        }
      }
    }

    def workDone(n: Node[N, T, R]) = n match {
      case rn: RangeNode[t] => (IndexNode.positiveProgress(rn.range) - rn.start + rn.end - IndexNode.until(rn.range))
      case _ => 0
    }

    def reduce(op: (R, R) => R): R = if (child.isLeaf) {
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
    val CHILD_OFFSET = unsafe.objectFieldOffset(classOf[Ptr[_, _, _]].getDeclaredField("child"))
  }

  abstract class Strategy {

    /** Finds work in the tree for the given worker, which is one out of `total` workers.
     *  This search may include stealing work.
     */
    def findWork[N <: Node[N, T, R], T, R](worker: Worker, tree: Ptr[N, T, R]): Ptr[N, T, R]

   /** Returns true if the worker labeled with `index` with a total of
    *  `total` workers should go left at level `level`.
    *  Returns false otherwise.
    */
    def choose[N <: Node[N, T, R], T, R](index: Int, total: Int, tree: Ptr[N, T, R]): Boolean

    /** Which node the stealer takes at this level. */
    def chooseAsStealer[N <: Node[N, T, R], T, R](index: Int, total: Int, tree: Ptr[N, T, R]): Ptr[N, T, R]

    /** Which node the victim takes at this level. */
    def chooseAsVictim[N <: Node[N, T, R], T, R](index: Int, total: Int, tree: Ptr[N, T, R]): Ptr[N, T, R]

  }

  abstract class FindFirstStrategy extends Strategy {

    final def findWork[N <: Node[N, T, R], T, R](worker: Worker, tree: Ptr[N, T, R]): Ptr[N, T, R] = {
      val index = worker.index
      val total = worker.total
      val node = tree.child
      if (node.isLeaf) {
        if (node.state eq Node.Completed) null // no further expansions
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

    def choose[N <: Node[N, T, R], T, R](index: Int, total: Int, tree: Ptr[N, T, R]): Boolean = {
      val level = tree.level
      if (isTreeTop(total, level)) {
        chooseInTreeTop(index, level)
      } else localRandom.nextBoolean
    }

    def chooseAsStealer[N <: Node[N, T, R], T, R](index: Int, total: Int, tree: Ptr[N, T, R]): Ptr[N, T, R] = {
      val level = tree.level
      if (isTreeTop(total, level)) {
        if (chooseInTreeTop(index, level)) tree.child.left
        else tree.child.right
      } else tree.child.right
    }

    def chooseAsVictim[N <: Node[N, T, R], T, R](index: Int, total: Int, tree: Ptr[N, T, R]): Ptr[N, T, R] = {
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

    def choose[N <: Node[N, T, R], T, R](index: Int, total: Int, tree: Ptr[N, T, R]): Boolean = {
      val level = tree.level
      if (isTreeTop(total, level) && tree.child.isLeaf) {
        chooseInTreeTop(index, level)
      } else localRandom.nextBoolean
    }

    def chooseAsStealer[N <: Node[N, T, R], T, R](index: Int, total: Int, tree: Ptr[N, T, R]): Ptr[N, T, R] = {
      val level = tree.level
      if (isTreeTop(total, level)) {
        if (chooseInTreeTop(index, level)) tree.child.left
        else tree.child.right
      } else tree.child.right
    }

    def chooseAsVictim[N <: Node[N, T, R], T, R](index: Int, total: Int, tree: Ptr[N, T, R]): Ptr[N, T, R] = {
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

    def choose[N <: Node[N, T, R], T, R](index: Int, total: Int, tree: Ptr[N, T, R]): Boolean = {
      val levelmod = tree.level % log2(total)
      ((index >> levelmod) & 0x1) == 0
    }

    def chooseAsStealer[N <: Node[N, T, R], T, R](index: Int, total: Int, tree: Ptr[N, T, R]): Ptr[N, T, R] = {
      if (choose(index, total, tree)) tree.child.left
      else tree.child.right
    }

    def chooseAsVictim[N <: Node[N, T, R], T, R](index: Int, total: Int, tree: Ptr[N, T, R]): Ptr[N, T, R] = {
      if (choose(index, total, tree)) tree.child.left
      else tree.child.right
    }

  }

  object RandomWalk extends FindFirstStrategy {

    def choose[N <: Node[N, T, R], T, R](index: Int, total: Int, tree: Ptr[N, T, R]): Boolean = localRandom.nextBoolean

    def chooseAsStealer[N <: Node[N, T, R], T, R](index: Int, total: Int, tree: Ptr[N, T, R]) = tree.child.right

    def chooseAsVictim[N <: Node[N, T, R], T, R](index: Int, total: Int, tree: Ptr[N, T, R]) = tree.child.left

  }

  object RandomAll extends FindFirstStrategy {

    def choose[N <: Node[N, T, R], T, R](index: Int, total: Int, tree: Ptr[N, T, R]): Boolean = localRandom.nextBoolean

    def chooseAsStealer[N <: Node[N, T, R], T, R](index: Int, total: Int, tree: Ptr[N, T, R]) = if (localRandom.nextBoolean) tree.child.left else tree.child.right

    def chooseAsVictim[N <: Node[N, T, R], T, R](index: Int, total: Int, tree: Ptr[N, T, R]) = if (localRandom.nextBoolean) tree.child.left else tree.child.right

  }

  object Predefined extends FindFirstStrategy {

    def choose[N <: Node[N, T, R], T, R](index: Int, total: Int, tree: Ptr[N, T, R]): Boolean = true

    def chooseAsStealer[N <: Node[N, T, R], T, R](index: Int, total: Int, tree: Ptr[N, T, R]) = tree.child.right

    def chooseAsVictim[N <: Node[N, T, R], T, R](index: Int, total: Int, tree: Ptr[N, T, R]) = tree.child.left

  }

  object FindMax extends Strategy {

    @tailrec def findWork[N <: Node[N, T, R], T, R](worker: Worker, tree: Ptr[N, T, R]) = {
      def search(current: Ptr[N, T, R]): Ptr[N, T, R] = if (current.child.isLeaf) current else {
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

    def choose[N <: Node[N, T, R], T, R](index: Int, total: Int, tree: Ptr[N, T, R]) = sys.error("never called")

    def chooseAsStealer[N <: Node[N, T, R], T, R](index: Int, total: Int, tree: Ptr[N, T, R]) = tree.child.right

    def chooseAsVictim[N <: Node[N, T, R], T, R](index: Int, total: Int, tree: Ptr[N, T, R]) = tree.child.left

  }


  val unsafe = Utils.getUnsafe()
  val initialStep = sys.props("step").toInt

  def localRandom = scala.concurrent.forkjoin.ThreadLocalRandom.current

  def nodeString[N <: Node[N, T, R], T, R](ws: WorkstealingScheduler)(n: Node[N, T, R]): String = n match {
    case rn: RangeNode[t] =>
      import rn._
      "[%.2f%%] RangeNode(%s)(%d, %d, %d, %d, %d)(lres = %s, rres = %s, res = %s) #%d".format(
        (IndexNode.positiveProgress(range) - start + end - IndexNode.until(range)).toDouble / ws.size * 100,
        if (owner == null) "none" else "worker " + owner.index,
        start,
        end,
        IndexNode.progress(range),
        IndexNode.until(range),
        step,
        lresult,
        rresult,
        result,
        System.identityHashCode(rn)
      )
    case _ =>
      "Node(%s)()(lres = %s, rres = %s, res = %s) #%d".format(
        if (n.owner == null) "none" else "worker " + n.owner.index,
        n.lresult,
        n.rresult,
        n.result,
        System.identityHashCode(n)
      )
  }

}










