package scala.collection.workstealing



import sun.misc.Unsafe
import annotation.tailrec
import scala.collection._



trait Workstealing[T] {

  import Workstealing._

  type N[R] <: Node[T, R]

  type K[R] <: Kernel[T, R]

  def size: Int

  def config: Workstealing.Config

  abstract class Node[@specialized S, R](val left: Ptr[S, R], val right: Ptr[S, R])(@volatile var step: Int) {
    @volatile var owner: Owner = null
    @volatile var lresult: R = null.asInstanceOf[R]
    @volatile var result: Option[R] = null

    def repr = this.asInstanceOf[N[R]]

    final def CAS_OWNER(ov: Owner, nv: Owner) = Utils.unsafe.compareAndSwapObject(this, OWNER_OFFSET, ov, nv)
    final def CAS_RESULT(ov: Option[R], nv: Option[R]) = Utils.unsafe.compareAndSwapObject(this, RESULT_OFFSET, ov, nv)

    final def isLeaf = left eq null

    @tailrec final def tryOwn(thiz: Owner): Boolean = {
      val currowner = /*READ*/owner
      if (currowner eq thiz) true
      else if (currowner != null) false
      else if (CAS_OWNER(currowner, thiz)) true
      else tryOwn(thiz)
    }

    final def trySteal(parent: AnyRef, kernel: AnyRef, worker: Worker): Boolean = parent.asInstanceOf[Ptr[S, R]].expand(kernel.asInstanceOf[Kernel[S, R]], worker)

    def workDone = 0

    def nodeString: String = "Node(%s)()(lres = %s, rres = %s, res = %s) #%d".format(
      if (owner == null) "none" else "worker " + owner.index,
      lresult,
      result,
      System.identityHashCode(this)
    )

    final def toString(lev: Int): String = {
      this.nodeString + (if (!isLeaf) {
        "\n" + left.toString(lev + 1) + right.toString(lev + 1)
      } else "\n")
    }

    /* abstract members */

    def elementsRemaining: Int

    def elementsCompleted: Int

    def newExpanded(parent: Ptr[S, R], worker: Worker): Node[S, R]

    def state: State

    def advance(step: Int): Int

    def next(): S

    def markCompleted(): Boolean

    def markStolen(): Boolean

  }

  final class Ptr[S, R](val up: Ptr[S, R], val level: Int)(@volatile var child: Node[S, R]) {
    def casChild(ov: Node[S, R], nv: Node[S, R]) = Utils.unsafe.compareAndSwapObject(this, CHILD_OFFSET, ov, nv)
    def writeChild(nv: Node[S, R]) = Utils.unsafe.putObjectVolatile(this, CHILD_OFFSET, nv)

    /** Try to expand node and return true if node was expanded.
     *  Return false if node was completed.
     */
    @tailrec def expand(kernel: Kernel[S, R], worker: Worker): Boolean = {
      val child_t0 = /*READ*/child
      if (!child_t0.isLeaf) true else { // already expanded
        // first set progress to -progress
        val state_t1 = child_t0.state
        if (state_t1 eq Completed) false // already completed
        else {
          if (state_t1 ne StolenOrExpanded) {
            if (child_t0.markStolen()) expand(kernel, worker) // marked stolen - now move on to node creation
            else expand(kernel, worker) // wasn't marked stolen and failed marking stolen - retry
          } else { // already marked stolen
            // node effectively immutable (except for `lresult`, `rresult` and `result`) - expand it
            val expanded = child_t0.newExpanded(this, worker)
            kernel.afterExpand(child_t0, expanded)
            if (casChild(child_t0, expanded)) true // try to replace with expansion
            else expand(kernel, worker) // failure (spurious or due to another expand) - retry
          }
        }
      }
    }

    def toString(l: Int): String = "\t" * level + "Ptr" + level + " -> " + child.toString(l)

    def balance: collection.immutable.HashMap[Owner, Int] = {
      val here = collection.immutable.HashMap(child.owner -> workDone(child))
      if (child.isLeaf) here
      else Seq(here, child.left.balance, child.right.balance).foldLeft(collection.immutable.HashMap[Owner, Int]()) {
        case (a, b) => a.merged(b) {
          case ((ak, av), (bk, bv)) => ((ak, av + bv))
        }
      }
    }

    def workDone(n: Node[S, R]) = n.workDone

    def treeSize: Int = {
      if (child.isLeaf) 1
      else 1 + child.left.treeSize + child.right.treeSize
    }

  }

  object Invoker extends Worker {
    def index = 0
    def total = config.par
    def getName = "Invoker"
  }

  @tailrec final def workUntilNoWork[R](w: Worker, root: Ptr[T, R], kernel: K[R]) {
    //println(root.toString(0))
    val leaf = config.strategy.findWork[T, R](w, root, kernel).asInstanceOf[Ptr[T, R]]
    if (leaf != null) {
      //println(leaf.toString(0))
      @tailrec def workAndDescend(leaf: Ptr[T, R]) {
        val nosteals = kernel.workOn(leaf, w)
        if (!nosteals) {
          val subnode = config.strategy.chooseAsVictim[T, R](w.index, w.total, leaf).asInstanceOf[Ptr[T, R]]
          if (subnode.child.tryOwn(w)) workAndDescend(subnode)
        }
      }
      workAndDescend(leaf)
      workUntilNoWork(w, root, kernel)
    } else {
      // no more work
    }
  }

  class WorkerThread[R](val root: Ptr[T, R], val index: Int, val total: Int, kernel: K[R]) extends Thread with Worker {
    setName("Worker: " + index)

    override final def run() {
      workUntilNoWork(this, root, kernel)
    }
  }

  def dispatchWorkT[R](root: Ptr[T, R], kernel: K[R]) {
    var i = 1
    while (i < config.par) {
      val w = new WorkerThread[R](root, i, config.par, kernel)
      w.start()
      i += 1
    }
  }

  def joinWorkT[R](root: Ptr[T, R]) = {
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

  class WorkerTask[R](val root: Ptr[T, R], val index: Int, val total: Int, kernel: K[R]) extends RecursiveAction with Worker {
    def getName = "WorkerTask(" + index + ")"

    def compute() {
      try {
        workUntilNoWork(this, root, kernel)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  def dispatchWorkFJ[R](root: Ptr[T, R], kernel: K[R]) {
    var i = 1
    while (i < config.par) {
      val w = new WorkerTask(root, i, config.par, kernel)
      fjpool.execute(w)
      i += 1
    }
  }

  def joinWorkFJ[R](root: Ptr[T, R]) = {
    var r = /*READ*/root.child.result
    if (r == null || r.isEmpty) root.synchronized {
      r = /*READ*/root.child.result
      while (r == null || r.isEmpty) {
        root.wait()
        r = /*READ*/root.child.result
      }
    }
  }

  abstract class Kernel[@specialized S, R] {
    @volatile protected var notTermFlag = true

    /** Contains `false` if the operation can complete before all the elements have been processed.
     *  This is useful for methods like `find`, `forall` and `exists`, as well as methods in which
     *  the argument function can throw an exception.
     *
     *  By default it returns the state of the `notTermFlag` that kernel implementations may set.
     */
    def notTerminated = notTermFlag

    def repr = this.asInstanceOf[K[R]]

    /** Initializes the workstealing tree node.
     *
     *  By default does nothing, but some kernels may choose to override this default behaviour
     *  to store operation-specific information into the node.
     */
    def beforeWorkOn(tree: Ptr[S, R], node: Node[S, R]) {
      node.lresult = zero
    }

    /** Initializes the workstealing tree root.
     *
     *  By default does nothing, but some kernels may choose to override this default behaviour.
     */
    def afterCreateRoot(tree: Ptr[S, R]) {}

    /** Initializes a node that has just been expanded.
     * 
     *  By default does nothing, but some kernels may choose to override this default behaviour
     *  to store operation-specific information into the node.
     */
    def afterExpand(old: Node[S, R], node: Node[S, R]) {}

    /** Stores the result of processing the node into the `lresult` field.
     *
     *  This behaviour can be overridden.
     */
    def storeLResult(node: Node[S, R], res: R) {
      node.lresult = res
    }

    /** The maximum size a chunk can have.
     */
    def maximumChunkSize = config.maxStep

    /** Returns true if completed with no stealing.
     *  Returns false if steal occurred.
     *
     *  May be overridden in subclass to specialize for better performance.
     */
    def workOn(tree: Ptr[S, R], worker: Worker): Boolean = {
      val node = /*READ*/tree.child
      beforeWorkOn(tree, node)
      var lsum = node.lresult
      var incCount = 0
      val incFreq = config.incrementFrequency
      val ms = maximumChunkSize
      var looping = true
      while (looping && notTerminated) {
        val currstep = /*READ*/node.step
        val currstate = /*READ*/node.state
  
        if (currstate != Completed && currstate != StolenOrExpanded) {
          // reserve some work
          val chunk = node.advance(currstep)

          if (chunk != -1) lsum = combine(lsum, apply(node.repr, chunk))
  
          // update step
          incCount = (incCount + 1) % incFreq
          if (incCount == 0) node.step = /*WRITE*/math.min(ms, currstep * 2)
        } else looping = false
      }

      completeIteration(node)

      // complete node information
      completeNode(lsum, tree, worker)
    }

    /** Completes the iteration in the node.
     * 
     *  Some parallel operations do not traverse all the elements in a chunk or a node. The purpose of this
     *  method is to bring the node into a Completed or Stolen state before proceeding.
     */
    protected final def completeIteration(node: Node[S, R]) {
      var state = node.state
      while (state eq AvailableOrOwned) {
        node.markCompleted()
        state = node.state
      }
    }

    private def completeNode(lsum: R, tree: Ptr[S, R], worker: Worker): Boolean = {
      val work = tree.child

      val state_t0 = work.state
      val wasCompleted = if (state_t0 eq Completed) {
        storeLResult(work, lsum)
        while (work.result == null) work.CAS_RESULT(null, None)
        //println(Thread.currentThread.getName + " -> " + work.start + " to " + work.progress + "; id=" + System.identityHashCode(work))
        true
      } else if (state_t0 eq StolenOrExpanded) {
        // help expansion if necessary
        if (tree.child.isLeaf) tree.expand(this, worker)
        storeLResult(tree.child, lsum)
        while (tree.child.result == null) tree.child.CAS_RESULT(null, None)
        //val work = tree.child
        //println(Thread.currentThread.getName + " -> " + work.start + " to " + work.progress + "; id=" + System.identityHashCode(work))
        false
      } else sys.error("unreachable: " + state_t0 + ", " + work.toString(0))
  
      // push result up as far as possible
      pushUp(tree)
  
      wasCompleted
    }
    
    @tailrec private def pushUp(tree: Ptr[S, R]) {
      val r = /*READ*/tree.child.result
      r match {
        case null =>
          // we're done, owner did not finish his work yet
        case Some(_) =>
          // we're done, somebody else already pushed up
        case None =>
          val finalresult =
            if (tree.child.isLeaf) Some(tree.child.lresult)
            else {
              // check if result already set for children
              val leftresult = /*READ*/tree.child.left.child.result
              val rightresult = /*READ*/tree.child.right.child.result
              (leftresult, rightresult) match {
                case (Some(lr), Some(rr)) =>
                  val r = combine(tree.child.lresult, combine(lr, rr))
                  Some(r)
                case (_, _) => // we're done, some of the children did not finish yet
                  None
              }
            }
  
          if (finalresult.nonEmpty) if (tree.child.CAS_RESULT(r, finalresult)) {
            // if at root, notify completion, otherwise go one level up
            if (tree.up == null) tree.synchronized {
              tree.notifyAll()
            } else pushUp(tree.up)
          } else pushUp(tree) // retry
      }
    }

    /** The neutral element of the reduction.
     */
    def zero: R
  
    /** Combines results from two chunks into the aggregate result.
     */
    def combine(a: R, b: R): R
  
    /** Processes the specified chunk.
     */
    def apply(node: N[R], chunkSize: Int): R
  }

  def newRoot[R]: Ptr[T, R]

  def invokeParallelOperation[R](kernel: Kernel[T, R]): R = {
    // create workstealing tree
    val root = newRoot[R]
    val work = root.child
    kernel.afterCreateRoot(root)
    work.tryOwn(Invoker)

    // let other workers know there's something to do
    dispatchWorkFJ(root, kernel.repr)

    // piggy-back the caller into doing work
    if (!kernel.workOn(root, Invoker)) workUntilNoWork(Invoker, root, kernel.repr)

    // synchronize in case there's some other worker just
    // about to complete work
    joinWorkFJ(root)

    if (debugMode) lastroot = root

    root.child.result.get
  }

}


object Workstealing {

  import scala.concurrent.forkjoin._

  val fjpool = new ForkJoinPool()

  type Owner = Worker

  trait Worker {
    def index: Int
    def total: Int
    def getName: String
  }
   
  val OWNER_OFFSET = Utils.unsafe.objectFieldOffset(classOf[Workstealing[_]#Node[_, _]].getDeclaredField("owner"))
  val RESULT_OFFSET = Utils.unsafe.objectFieldOffset(classOf[Workstealing[_]#Node[_, _]].getDeclaredField("result"))
  val CHILD_OFFSET = Utils.unsafe.objectFieldOffset(classOf[Workstealing[_]#Ptr[_, _]].getDeclaredField("child"))

  trait Config {
    def initialStep: Int
    def maxStep: Int
    def incrementFrequency: Int
    def par: Int
    def strategy: Strategy
  }

  object DefaultConfig extends Config {
    val initialStep = sys.props("step").toInt
    val maxStep = sys.props.getOrElse("maxStep", "1024").toInt
    val incrementFrequency = 1
    val par = sys.props("par").toInt
    val strategy: Strategy = strategies(sys.props("strategy"))
  }

  val strategies = List(FindMax, AssignTopLeaf, AssignTop, Assign, RandomWalk, RandomAll, Predefined) map (x => (x.getClass.getSimpleName, x)) toMap
  var lastroot: Workstealing[_]#Ptr[_, _] = _
  val imbalance = collection.mutable.Map[Int, collection.mutable.ArrayBuffer[Int]]((0 until sys.props("par").toInt) map (x => (x, collection.mutable.ArrayBuffer[Int]())): _*)
  val treesizes = collection.mutable.ArrayBuffer[Int]()
  var debugMode = sys.props("debug").toBoolean

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

  def localRandom = scala.concurrent.forkjoin.ThreadLocalRandom.current

  type Tree[S, R] = Workstealing[S]#Ptr[S, R]

  type Kernel[S, R] = Workstealing[S]#Kernel[S, R]

  abstract class Strategy {

    /** Finds work in the tree for the given worker, which is one out of `total` workers.
     *  This search may include stealing work.
     */
    def findWork[S, R](worker: Worker, tree: Tree[S, R], kernel: Kernel[S, R]): Tree[S, R]

    /** Returns true if the worker labeled with `index` with a total of
     *  `total` workers should go left at level `level`.
     *  Returns false otherwise.
     */
    def choose[S, R](index: Int, total: Int, tree: Tree[S, R]): Boolean

    /** Which node the stealer takes at this level. */
    def chooseAsStealer[S, R](index: Int, total: Int, tree: Tree[S, R]): Tree[S, R]

    /** Which node the victim takes at this level. */
    def chooseAsVictim[S, R](index: Int, total: Int, tree: Tree[S, R]): Tree[S, R]

  }

  abstract class FindFirstStrategy extends Strategy {

    final def findWork[S, R](worker: Worker, tree: Tree[S, R], kernel: Kernel[S, R]) = {
      val index = worker.index
      val total = worker.total
      val node = tree.child
      if (node.isLeaf) {
        if (
          ((node.state eq Completed) && node.owner != null && node.owner != worker) ||
          (node.owner != worker && node.elementsRemaining == 1)
        ) null // no further expansions
        else {
          // more work
          if (node.tryOwn(worker)) tree
          else if (node.trySteal(tree, kernel, worker)) {
            val subnode = chooseAsStealer(index, total, tree)
            if (subnode.child.tryOwn(worker)) subnode
            else findWork[S, R](worker, tree, kernel)
          } else findWork[S, R](worker, tree, kernel)
        }
      } else {
        // descend deeper
        if (choose(index, total, tree)) {
          val ln = findWork[S, R](worker, node.left, kernel)
          if (ln != null) ln else findWork[S, R](worker, node.right, kernel)
        } else {
          val rn = findWork[S, R](worker, node.right, kernel)
          if (rn != null) rn else findWork[S, R](worker, node.left, kernel)
        }
      }
    }

  }

  object AssignTop extends FindFirstStrategy {

    /** Returns true iff the level of the tree is such that: 2^level < total */
    final def isTreeTop(total: Int, level: Int): Boolean = (1 << level) < total
    
    /** Returns true iff the worker should first go left at this level of the tree top. */
    final def chooseInTreeTop(index: Int, level: Int): Boolean = ((index >> level) & 0x1) == 0

    def choose[S, R](index: Int, total: Int, tree: Tree[S, R]): Boolean = {
      val level = tree.level
      if (isTreeTop(total, level)) {
        chooseInTreeTop(index, level)
      } else localRandom.nextBoolean
    }

    def chooseAsStealer[S, R](index: Int, total: Int, tree: Tree[S, R]): Tree[S, R] = {
      val level = tree.level
      if (isTreeTop(total, level)) {
        if (chooseInTreeTop(index, level)) tree.child.left
        else tree.child.right
      } else tree.child.right
    }

    def chooseAsVictim[S, R](index: Int, total: Int, tree: Tree[S, R]): Tree[S, R] = {
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

    def choose[S, R](index: Int, total: Int, tree: Tree[S, R]): Boolean = {
      val level = tree.level
      if (isTreeTop(total, level) && tree.child.isLeaf) {
        chooseInTreeTop(index, level)
      } else localRandom.nextBoolean
    }

    def chooseAsStealer[S, R](index: Int, total: Int, tree: Tree[S, R]): Tree[S, R] = {
      val level = tree.level
      if (isTreeTop(total, level)) {
        if (chooseInTreeTop(index, level)) tree.child.left
        else tree.child.right
      } else tree.child.right
    }

    def chooseAsVictim[S, R](index: Int, total: Int, tree: Tree[S, R]): Tree[S, R] = {
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

    def choose[S, R](index: Int, total: Int, tree: Tree[S, R]): Boolean = {
      val levelmod = tree.level % log2(total)
      ((index >> levelmod) & 0x1) == 0
    }

    def chooseAsStealer[S, R](index: Int, total: Int, tree: Tree[S, R]): Tree[S, R] = {
      if (choose(index, total, tree)) tree.child.left
      else tree.child.right
    }

    def chooseAsVictim[S, R](index: Int, total: Int, tree: Tree[S, R]): Tree[S, R] = {
      if (choose(index, total, tree)) tree.child.left
      else tree.child.right
    }

  }

  object RandomWalk extends FindFirstStrategy {

    def choose[S, R](index: Int, total: Int, tree: Tree[S, R]): Boolean = localRandom.nextBoolean

    def chooseAsStealer[S, R](index: Int, total: Int, tree: Tree[S, R]) = tree.child.right

    def chooseAsVictim[S, R](index: Int, total: Int, tree: Tree[S, R]) = tree.child.left

  }

  object RandomAll extends FindFirstStrategy {

    def choose[S, R](index: Int, total: Int, tree: Tree[S, R]): Boolean = localRandom.nextBoolean

    def chooseAsStealer[S, R](index: Int, total: Int, tree: Tree[S, R]) = if (localRandom.nextBoolean) tree.child.left else tree.child.right

    def chooseAsVictim[S, R](index: Int, total: Int, tree: Tree[S, R]) = if (localRandom.nextBoolean) tree.child.left else tree.child.right

  }

  object Predefined extends FindFirstStrategy {

    def choose[S, R](index: Int, total: Int, tree: Tree[S, R]): Boolean = true

    def chooseAsStealer[S, R](index: Int, total: Int, tree: Tree[S, R]) = tree.child.right

    def chooseAsVictim[S, R](index: Int, total: Int, tree: Tree[S, R]) = tree.child.left

  }

  object FindMax extends Strategy {

    @tailrec def findWork[S, R](worker: Worker, tree: Tree[S, R], kernel: Kernel[S, R]) = {
      def search(current: Tree[S, R]): Tree[S, R] = {
        val node = /*READ*/current.child
        if (node.isLeaf) {
          if ((node.state ne Completed) && !(node.elementsRemaining > 1)) current
          else if (node.owner == null || (node.owner eq worker)) current
          else null
        } else {
          val left = search(node.left)
          val right = search(node.right)
          if (left != null && right != null) {
            val leftwork = left.child.elementsRemaining
            val rightwork = right.child.elementsRemaining
            if (leftwork > rightwork) left else right
          } else if (left != null) left else right
        }
      }

      val max = search(tree)
      if (max != null) {
        val node = /*READ*/max.child
        if (node.tryOwn(worker)) max
        else if (node.trySteal(max, kernel, worker)) {
          val subnode = chooseAsStealer(worker.index, worker.total, max)
          if (subnode.child.tryOwn(worker)) subnode
          else findWork(worker, tree, kernel)
        } else findWork(worker, tree, kernel)
      } else null
    }

    def choose[S, R](index: Int, total: Int, tree: Tree[S, R]) = sys.error("never called")

    def chooseAsStealer[S, R](index: Int, total: Int, tree: Tree[S, R]) = tree.child.right

    def chooseAsVictim[S, R](index: Int, total: Int, tree: Tree[S, R]) = tree.child.left

  }

}







