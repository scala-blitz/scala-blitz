package scala.collection.parallel



import sun.misc.Unsafe
import annotation.tailrec
import collection._



abstract class WorkstealingCollection[T] {

  import WorkstealingCollection._

  val strategies = List(FindMax, AssignTopLeaf, AssignTop, Assign, RandomWalk, RandomAll, Predefined) map (x => (x.getClass.getSimpleName, x)) toMap
  val par = sys.props("par").toInt
  val inspectgc = sys.props.getOrElse("inspectgc", "false").toBoolean
  val strategy: Strategy = strategies(sys.props("strategy"))
  val maxStep = sys.props.getOrElse("maxStep", "1024").toInt
  val repeats = sys.props.getOrElse("repeats", "1").toInt
  val starterThread = sys.props("starterThread")
  val starterCooldown = sys.props("starterCooldown").toInt
  val invocationMethod = sys.props("invocationMethod")
  val incrementFrequency = 1

  type N[R] <: Node[R]

  type K[R] <: Kernel[R]

  def size: Int

  abstract class Node[R](val left: Ptr[R], val right: Ptr[R])(@volatile var step: Int) {
    @volatile var owner: Owner = null
    @volatile var lresult: R = null.asInstanceOf[R]
    @volatile var rresult: R = null.asInstanceOf[R]
    @volatile var result: Option[R] = null

    def repr = this.asInstanceOf[N[R]]

    final def casOwner(ov: Owner, nv: Owner) = Utils.unsafe.compareAndSwapObject(this, OWNER_OFFSET, ov, nv)
    final def casResult(ov: Option[R], nv: Option[R]) = Utils.unsafe.compareAndSwapObject(this, RESULT_OFFSET, ov, nv)

    final def isLeaf = left eq null

    @tailrec final def tryOwn(thiz: Owner): Boolean = {
      val currowner = /*READ*/owner
      if (currowner != null) false
      else if (casOwner(currowner, thiz)) true
      else tryOwn(thiz)
    }

    final def trySteal(parent: Ptr[R]): Boolean = parent.expand()

    def workDone = 0

    def nodeString: String = "Node(%s)()(lres = %s, rres = %s, res = %s) #%d".format(
      if (owner == null) "none" else "worker " + owner.index,
      lresult,
      rresult,
      result,
      System.identityHashCode(this)
    )

    final def toString(lev: Int): String = {
      this.nodeString + (if (!isLeaf) {
        "\n" + left.toString(lev + 1) + right.toString(lev + 1)
      } else "\n")
    }

    /* abstract members */

    def workRemaining: Int

    def newExpanded(parent: Ptr[R]): Node[R]

    def state: State

    def advance(step: Int): Int

    def next(): T

    def markStolen(): Boolean

  }

  final class Ptr[R](val up: Ptr[R], val level: Int)(@volatile var child: Node[R]) {
    def casChild(ov: Node[R], nv: Node[R]) = Utils.unsafe.compareAndSwapObject(this, CHILD_OFFSET, ov, nv)
    def writeChild(nv: Node[R]) = Utils.unsafe.putObjectVolatile(this, CHILD_OFFSET, nv)

    /** Try to expand node and return true if node was expanded.
     *  Return false if node was completed.
     */
    @tailrec def expand(): Boolean = {
      val child_t0 = /*READ*/child
      if (!child_t0.isLeaf) true else { // already expanded
        // first set progress to -progress
        val state_t1 = child_t0.state
        if (state_t1 eq Completed) false // already completed
        else {
          if (state_t1 ne StolenOrExpanded) {
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

    def workDone(n: Node[R]) = n.workDone

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

  def completeNode[R](lsum: R, rsum: R, tree: Ptr[R], kernel: Kernel[R]): Boolean = {
    val work = tree.child

    val state_t0 = work.state
    val wasCompleted = if (state_t0 eq Completed) {
      work.lresult = lsum
      work.rresult = rsum
      while (work.result == null) work.casResult(null, None)
      //println(Thread.currentThread.getName + " -> " + work.start + " to " + work.progress + "; id=" + System.identityHashCode(work))
      true
    } else if (state_t0 eq StolenOrExpanded) {
      // help expansion if necessary
      if (tree.child.isLeaf) tree.expand()
      tree.child.lresult = lsum
      tree.child.rresult = rsum
      while (tree.child.result == null) tree.child.casResult(null, None)
      //val work = tree.child
      //println(Thread.currentThread.getName + " -> " + work.start + " to " + work.progress + "; id=" + System.identityHashCode(work))
      false
    } else sys.error("unreachable: " + state_t0 + ", " + work.toString(0))

    // push result up as far as possible
    pushUp(tree, kernel)

    wasCompleted
  }
  
  @tailrec final def pushUp[R](tree: Ptr[R], k: Kernel[R]) {
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

  var lastroot: Ptr[_] = _
  val imbalance = collection.mutable.Map[Int, collection.mutable.ArrayBuffer[Int]]((0 until par) map (x => (x, collection.mutable.ArrayBuffer[Int]())): _*)
  val treesizes = collection.mutable.ArrayBuffer[Int]()

  @tailrec final def workUntilNoWork[R](w: Worker, root: Ptr[R], kernel: K[R]) {
    val leaf = strategy.findWork[R](w, root)
    if (leaf != null) {
      @tailrec def workAndDescend(leaf: Ptr[R]) {
        val nosteals = kernel.workOn(leaf)
        if (!nosteals) {
          val subnode = strategy.chooseAsVictim[R](w.index, w.total, leaf)
          if (subnode.child.tryOwn(w)) workAndDescend(subnode)
        }
      }
      workAndDescend(leaf)
      workUntilNoWork(w, root, kernel)
    } else {
      // no more work
    }
  }

  class WorkerThread[R](val root: Ptr[R], val index: Int, val total: Int, kernel: K[R]) extends Thread with Worker {
    setName("Worker: " + index)

    override final def run() {
      workUntilNoWork(this, root, kernel)
    }
  }

  def dispatchWorkT[R](root: Ptr[R], kernel: K[R]) {
    var i = 1
    while (i < par) {
      val w = new WorkerThread[R](root, i, par, kernel)
      w.start()
      i += 1
    }
  }

  def joinWorkT[R](root: Ptr[R]) = {
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

  class WorkerTask[R](val root: Ptr[R], val index: Int, val total: Int, kernel: K[R]) extends RecursiveAction with Worker {
    def getName = "WorkerTask(" + index + ")"

    def compute() {
      workUntilNoWork(this, root, kernel)
    }
  }

  def dispatchWorkFJ[R](root: Ptr[R], kernel: K[R]) {
    var i = 1
    while (i < par) {
      val w = new WorkerTask(root, i, par, kernel)
      fjpool.execute(w)
      i += 1
    }
  }

  def joinWorkFJ[R](root: Ptr[R]) = {
    var r = /*READ*/root.child.result
    if (r == null || r.isEmpty) root.synchronized {
      r = /*READ*/root.child.result
      while (r == null || r.isEmpty) {
        root.wait()
        r = /*READ*/root.child.result
      }
    }
  }

  abstract class Kernel[R] {
    def repr = this.asInstanceOf[K[R]]

    /** Returns true if completed with no stealing.
     *  Returns false if steal occurred.
     *
     *  May be overridden in subclass to specialize for better performance.
     */
    def workOn(tree: Ptr[R]): Boolean = {
      val node = /*READ*/tree.child
      var lsum = zero
      var rsum = zero
      var incCount = 0
      val incFreq = incrementFrequency
      val ms = maxStep
      var looping = true
      while (looping) {
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
  
      // complete node information
      completeNode[R](lsum, rsum, tree, this)
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

  def newRoot[R]: Ptr[R]

  def invokeParallelOperation[R](kernel: Kernel[R]): R = {
    // create workstealing tree
    val root = newRoot[R]
    val work = root.child
    work.tryOwn(Invoker)

    // let other workers know there's something to do
    dispatchWorkFJ(root, kernel.repr)

    // piggy-back the caller into doing work
    if (!kernel.workOn(root)) workUntilNoWork(Invoker, root, kernel.repr)

    // synchronize in case there's some other worker just
    // about to complete work
    joinWorkFJ(root)

    lastroot = root

    root.child.result.get
  }

  abstract class Strategy {

    /** Finds work in the tree for the given worker, which is one out of `total` workers.
     *  This search may include stealing work.
     */
    def findWork[R](worker: Worker, tree: Ptr[R]): Ptr[R]

    /** Returns true if the worker labeled with `index` with a total of
     *  `total` workers should go left at level `level`.
     *  Returns false otherwise.
     */
    def choose[R](index: Int, total: Int, tree: Ptr[R]): Boolean

    /** Which node the stealer takes at this level. */
    def chooseAsStealer[R](index: Int, total: Int, tree: Ptr[R]): Ptr[R]

    /** Which node the victim takes at this level. */
    def chooseAsVictim[R](index: Int, total: Int, tree: Ptr[R]): Ptr[R]

  }

  abstract class FindFirstStrategy extends Strategy {

    final def findWork[R](worker: Worker, tree: Ptr[R]): Ptr[R] = {
      val index = worker.index
      val total = worker.total
      val node = tree.child
      if (node.isLeaf) {
        if (node.state eq Completed) null // no further expansions
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

    def choose[R](index: Int, total: Int, tree: Ptr[R]): Boolean = {
      val level = tree.level
      if (isTreeTop(total, level)) {
        chooseInTreeTop(index, level)
      } else localRandom.nextBoolean
    }

    def chooseAsStealer[R](index: Int, total: Int, tree: Ptr[R]): Ptr[R] = {
      val level = tree.level
      if (isTreeTop(total, level)) {
        if (chooseInTreeTop(index, level)) tree.child.left
        else tree.child.right
      } else tree.child.right
    }

    def chooseAsVictim[R](index: Int, total: Int, tree: Ptr[R]): Ptr[R] = {
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

    def choose[R](index: Int, total: Int, tree: Ptr[R]): Boolean = {
      val level = tree.level
      if (isTreeTop(total, level) && tree.child.isLeaf) {
        chooseInTreeTop(index, level)
      } else localRandom.nextBoolean
    }

    def chooseAsStealer[R](index: Int, total: Int, tree: Ptr[R]): Ptr[R] = {
      val level = tree.level
      if (isTreeTop(total, level)) {
        if (chooseInTreeTop(index, level)) tree.child.left
        else tree.child.right
      } else tree.child.right
    }

    def chooseAsVictim[R](index: Int, total: Int, tree: Ptr[R]): Ptr[R] = {
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

    def choose[R](index: Int, total: Int, tree: Ptr[R]): Boolean = {
      val levelmod = tree.level % log2(total)
      ((index >> levelmod) & 0x1) == 0
    }

    def chooseAsStealer[R](index: Int, total: Int, tree: Ptr[R]): Ptr[R] = {
      if (choose(index, total, tree)) tree.child.left
      else tree.child.right
    }

    def chooseAsVictim[R](index: Int, total: Int, tree: Ptr[R]): Ptr[R] = {
      if (choose(index, total, tree)) tree.child.left
      else tree.child.right
    }

  }

  object RandomWalk extends FindFirstStrategy {

    def choose[R](index: Int, total: Int, tree: Ptr[R]): Boolean = localRandom.nextBoolean

    def chooseAsStealer[R](index: Int, total: Int, tree: Ptr[R]) = tree.child.right

    def chooseAsVictim[R](index: Int, total: Int, tree: Ptr[R]) = tree.child.left

  }

  object RandomAll extends FindFirstStrategy {

    def choose[R](index: Int, total: Int, tree: Ptr[R]): Boolean = localRandom.nextBoolean

    def chooseAsStealer[R](index: Int, total: Int, tree: Ptr[R]) = if (localRandom.nextBoolean) tree.child.left else tree.child.right

    def chooseAsVictim[R](index: Int, total: Int, tree: Ptr[R]) = if (localRandom.nextBoolean) tree.child.left else tree.child.right

  }

  object Predefined extends FindFirstStrategy {

    def choose[R](index: Int, total: Int, tree: Ptr[R]): Boolean = true

    def chooseAsStealer[R](index: Int, total: Int, tree: Ptr[R]) = tree.child.right

    def chooseAsVictim[R](index: Int, total: Int, tree: Ptr[R]) = tree.child.left

  }

  object FindMax extends Strategy {

    @tailrec def findWork[R](worker: Worker, tree: Ptr[R]) = {
      def search(current: Ptr[R]): Ptr[R] = if (current.child.isLeaf) current else {
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

    def choose[R](index: Int, total: Int, tree: Ptr[R]) = sys.error("never called")

    def chooseAsStealer[R](index: Int, total: Int, tree: Ptr[R]) = tree.child.right

    def chooseAsVictim[R](index: Int, total: Int, tree: Ptr[R]) = tree.child.left

  }

}


object WorkstealingCollection {

  type Owner = Worker

  trait Worker {
    def index: Int
    def total: Int
    def getName: String
  }
   
  val OWNER_OFFSET = Utils.unsafe.objectFieldOffset(classOf[WorkstealingCollection[_]#Node[_]].getDeclaredField("owner"))
  val RESULT_OFFSET = Utils.unsafe.objectFieldOffset(classOf[WorkstealingCollection[_]#Node[_]].getDeclaredField("result"))
  val CHILD_OFFSET = Utils.unsafe.objectFieldOffset(classOf[WorkstealingCollection[_]#Ptr[_]].getDeclaredField("child"))

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

  val initialStep = sys.props("step").toInt

  def localRandom = scala.concurrent.forkjoin.ThreadLocalRandom.current

}







