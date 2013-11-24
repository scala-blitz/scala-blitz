package scala.collection.par



import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec



abstract class Scheduler {
  import Scheduler._

  final class Invoker extends WorkerTask {
    def index = 0
    def total = config.parallelismLevel
    def name = "Invoker"
  }

  val invoker = new Invoker

  def config: Config

  def invokeParallelOperation[T, R](stealer: Stealer[T], kernel: Kernel[T, R]): R

}

object Scheduler {

  trait Config {
    /** number of workers used in computation */
    def parallelismLevel: Int

    /**
     * How often should chunk size requested from stealer be increased
     *
     * In order to provide efficient sheduling, chunks sizes start from small and during the computation
     * value of -1 forces defaultIncrementStepFrequency to be used
     *  for more details @see <a href="https://parasol.tamu.edu/lcpc2013/papers/lcpc2013_submission_6.pdf">Near Optimal Work-Stealing Tree Scheduler for Highly Irregular Data-Parallel Workloads
     * Aleksandar Prokopec, Martin Odersky</a>
     */
    def incrementStepFrequency: Int

    /**
     * How much the requested chunksize from stealer should be increased
     *
     * Default implementation uses exponential increasing of chunk size
     * This defines exponent base
     * See also the method {@link #incrementStepFrequency()}
     */
    def incrementStepFactor: Int
    def maximumStep: Int
    def stealingStrategy: Strategy
  }

  object Config {
    class Default(val parallelismLevel: Int) extends Config {
      def runtimeParameters: Seq[String] = {
        import java.lang.management.ManagementFactory
        import java.lang.management.RuntimeMXBean
        import scala.collection.JavaConversions._
        val bean = ManagementFactory.getRuntimeMXBean()
        bean.getInputArguments()
      }

      def this() = this(Runtime.getRuntime.availableProcessors)

      def incrementStepFrequency = -1

      def incrementStepFactor = -1

      val isConditionalCardMarkingUsed = runtimeParameters.contains("-XX:+UseCondCardMark")

      def maximumStep = {
        /* see https://blogs.oracle.com/dave/entry/false_sharing_induced_by_card for details */
        if (scala.util.Properties.isJavaAtLeast("1.7") && isConditionalCardMarkingUsed) 4096 else 1000000
      }

      def stealingStrategy = FindMax
    }

    class FromExecutionContext(parlevel: Int, val ctx: scala.concurrent.ExecutionContext) extends Default(parlevel)
  }

  sealed trait Constant

  val CHILD_OFFSET = unsafe.objectFieldOffset(classOf[Ref[_, _]].getDeclaredField("child"))
  val OWNER_OFFSET = unsafe.objectFieldOffset(classOf[Node[_, _]].getDeclaredField("owner"))
  val RESULT_OFFSET = unsafe.objectFieldOffset(classOf[Node[_, _]].getDeclaredField("result"))
  object NO_RESULT extends Constant { override def toString = "NO_RESULT" }
  object INTERMEDIATE_READY extends Constant { override def toString = "INTERMEDIATE_READY" }

  /* concrete implementations */

  object Implicits {
    implicit val global = new ExecutionContext()
    implicit val sequential = Sequential

  }

  abstract class WorkstealingTree extends Scheduler {

    def dispatchWork[T, R](root: Ref[T, R], kernel: Kernel[T, R]): Unit

    def joinWork[T, R](root: Ref[T, R], kernel: Kernel[T, R]) {
      var r = root.READ.READ_RESULT
      if (r == NO_RESULT || r == INTERMEDIATE_READY) root.synchronized {
        r = root.READ.READ_RESULT
        while (r == NO_RESULT || r == INTERMEDIATE_READY) {
          root.wait()
          r = root.READ.READ_RESULT
        }
      }
    }

    @tailrec final def workUntilNoWork[T, R](w: WorkerTask, root: Ref[T, R], kernel: Kernel[T, R]) {
      val strategy = config.stealingStrategy
      val leaf = strategy.findWork[T, R](w, root, kernel)
      if (leaf != null) {
        @tailrec def workAndDescend(leaf: Ref[T, R]) {
          val nosteals = kernel.workOn(leaf, config, w)
          if (!nosteals) {
            val subnode = strategy.chooseAsVictim[T, R](w.index, w.total, leaf)
            if (subnode.child.tryOwn(w)) workAndDescend(subnode)
          }
        }
        workAndDescend(leaf)
        workUntilNoWork(w, root, kernel)
      } else {
        // no more work
      }
    }

    def invokeParallelOperation[T, R](stealer: Stealer[T], kernel: Kernel[T, R]): R = {
      // create workstealing tree
      val node = new Node[T, R](null, null)(stealer)
      val root = new Ref[T, R](null, 0)(node)
      val work = root.child
      kernel.afterCreateRoot(root)
      work.tryOwn(invoker)

      // let other workers know there's something to do
      dispatchWork(root, kernel)

      // piggy-back the caller into doing work
      if (!kernel.workOn(root, config, invoker)) workUntilNoWork(invoker, root, kernel)

      // synchronize in case there's some other worker just
      // about to complete work
      joinWork(root, kernel)

      val c = root.READ
      val r = c.READ_RESULT
      kernel.validateResult(r.asInstanceOf[R])
    }

  }

  class ExecutionContext(val config: Config.FromExecutionContext) extends WorkstealingTree {
    def this() = this(new Config.FromExecutionContext(Runtime.getRuntime.availableProcessors, scala.concurrent.ExecutionContext.Implicits.global))

    val pool = config.ctx

    class ExecutionContextWorkstealingTreeTask[T, R](val scheduler: Scheduler.WorkstealingTree, val root: Ref[T, R], val kernel: Kernel[T, R], val index: Int, val total: Int)
      extends Runnable with WorkstealingTreeTask[T, R] {
      def run() = workstealingTreeScheduling()
    }

    def dispatchWork[T, R](root: Ref[T, R], kernel: Kernel[T, R]) {
      var i = 1
      val par = config.parallelismLevel
      while (i < par) {
        val w = new ExecutionContextWorkstealingTreeTask(this, root, kernel, i, par)
        pool.execute(w)
        i += 1
      }
    }
  }

  class ForkJoin(val config: Config) extends WorkstealingTree {
    import scala.concurrent.forkjoin._

    def this() = this(new Config.Default())

    val pool = new ForkJoinPool(config.parallelismLevel)

    class ForkJoinWorkstealingTreeTask[T, R](val scheduler: Scheduler.WorkstealingTree, val root: Ref[T, R], val kernel: Kernel[T, R], val index: Int, val total: Int)
      extends RecursiveAction with WorkstealingTreeTask[T, R] {
      def compute() = workstealingTreeScheduling()
    }

    def dispatchWork[T, R](root: Ref[T, R], kernel: Kernel[T, R]) {
      var i = 1
      val par = config.parallelismLevel
      while (i < par) {
        val w = new ForkJoinWorkstealingTreeTask(this, root, kernel, i, par)
        pool.execute(w)
        i += 1
      }
    }
  }

  object Sequential extends WorkstealingTree {
    import scala.concurrent.forkjoin._

    val config = new Config.Default(1)

    class DummyTask[T, R](val scheduler: Scheduler.WorkstealingTree, val root: Ref[T, R], val kernel: Kernel[T, R])
      extends WorkstealingTreeTask[T, R] {
      val index = 1
      val total = 1
    }

    def dispatchWork[T, R](root: Ref[T, R], kernel: Kernel[T, R]) {
      val task = new DummyTask(this, root, kernel)
      root.child.owner = task
      task.workstealingTreeScheduling()
    }
  }

  /* internals */

  trait WorkstealingTreeTask[T, R] extends WorkerTask {
    val scheduler: WorkstealingTree
    val root: Ref[T, R]
    val kernel: Kernel[T, R]

    def name = "WorkerTask(" + index + ")"

    def workstealingTreeScheduling() {
      try {
        scheduler.workUntilNoWork(this, root, kernel)
      } catch {
        case t: Throwable =>
          val st = t.getStackTrace
          println("Uncaught exception in worker!")
          println("initial frames: " + st.take(20).mkString("\n"))
          println("...")
          println("last frames: " + st.takeRight(20).mkString("\n"))
          t.printStackTrace()
      }
    }
  }

  trait WorkerTask {
    def index: Int
    def total: Int
    def name: String
  }

  trait TerminationCause {
    def validateResult[R](r: R): R
  }

  class ThrowCause(t: Throwable) extends TerminationCause {
    def validateResult[R](r: R) = throw t
  }

  trait Kernel[@specialized T, @specialized R] {
    /**
     * Used for cancelling operations early (e.g. due to exceptions).
     *  Holds information on why the operation failed
     */
    protected val terminationCauseRef = new AtomicReference[TerminationCause](null)

    /**
     * Returns `true` as long as `terminationCause` is `null`.
     */
    def notTerminated = terminationCauseRef.get eq null

    @tailrec final def setTerminationCause(tc: TerminationCause) {
      val otc = terminationCauseRef.get
      if (otc == null) {
        if (!terminationCauseRef.compareAndSet(null, tc)) setTerminationCause(tc)
      }
    }

    /**
     * Returns the result if there was no early termination.
     *  Otherwise may throw an exception based on the termination cause.
     */
    def validateResult(r: R): R = {
      if (notTerminated) r
      else terminationCauseRef.get.validateResult(r)
    }

    /**
     * Initializes the workstealing tree node.
     *
     *  By default does nothing, but some kernels may choose to override this default behaviour
     *  to store operation-specific information into the node.
     */
    def beforeWorkOn(tree: Ref[T, R], node: Node[T, R]) {
      node.WRITE_INTERMEDIATE(zero)
    }

    /**
     * Initializes the workstealing tree root.
     *
     *  By default does nothing, but some kernels may choose to override this default behaviour.
     */
    def afterCreateRoot(tree: Ref[T, R]) {}

    /**
     * Initializes a node that has just been stolen.
     *
     *  By default does nothing, but some kernels may choose to override this default behaviour
     *  to store operation-specific information into the node.
     */
    def afterExpand(old: Node[T, R], node: Node[T, R]) {}

    /**
     * Stores the result of processing the node into the `lresult` field.
     *
     *  This behaviour can be overridden.
     */
    def storeIntermediateResult(node: Node[T, R], res: R) {
      node.WRITE_INTERMEDIATE(res)
    }

    def defaultIncrementStepFrequency = 1

    def incrementStepFrequency(config: Config) = {
      val isf = config.incrementStepFrequency
      if (isf == -1) defaultIncrementStepFrequency else isf
    }

    def defaultIncrementStepFactor = 2

    def incrementStepFactor(config: Config) = {
      val isf = config.incrementStepFactor
      if (isf == -1) defaultIncrementStepFactor else isf
    }

    /**
     * Returns true if completed with no stealing.
     *  Returns false if steal occurred.
     *
     *  May be overridden in subclass to specialize for better performance.
     */
    def workOn(tree: Ref[T, R], config: Config, worker: WorkerTask): Boolean = {
      import Stealer._

      // atomically read the current node and initialize
      val node = tree.READ
      val stealer = node.stealer
      beforeWorkOn(tree, node)
      var intermediate = node.READ_INTERMEDIATE
      var incCount = 0
      val incFreq = incrementStepFrequency(config)
      val incFact = incrementStepFactor(config)
      val ms = config.maximumStep

      // commit to processing chunks of the collection and process them until termination
      try {
        var looping = true
        while (looping && notTerminated) {
          val currstep = node.READ_STEP
          val currstate = stealer.state

          if (currstate != Completed && currstate != StolenOrExpanded) {
            // reserve some work
            val chunk = stealer.nextBatch(currstep)

            if (chunk != -1) intermediate = combine(intermediate, apply(node, chunk))

            // update step
            incCount = (incCount + 1) % incFreq
            if (incCount == 0) node.WRITE_STEP(math.min(ms, currstep * incFact))
          } else looping = false
        }
      } catch {
        case t: Throwable =>
          setTerminationCause(new ThrowCause(t))
      }

      completeIteration(node.stealer)

      // store into the `intermediateResult` field of the node and push result up
      completeNode(intermediate, tree, worker)
    }

    /**
     * Completes the iteration in the stealer.
     *
     *  Some parallel operations do not traverse all the elements in a chunk or a node. The purpose of this
     *  method is to bring the node into a Completed or Stolen state before proceeding.
     */
    protected final def completeIteration(stealer: Stealer[T]) {
      stealer.markCompleted()
    }

    /**
     * Completes the iteration in the node.
     *
     *  Some parallel operations do not traverse all the elements in a chunk or a node. The purpose of this
     *  method is to bring the node into a Completed or Stolen state before proceeding.
     */
    protected def completeNode(intermediate: R, tree: Ref[T, R], worker: WorkerTask): Boolean = {
      import Stealer._
      val node_t0 = tree.READ
      val state_t1 = node_t0.stealer.state
      val wasCompleted = if (state_t1 eq Completed) {
        storeIntermediateResult(node_t0, intermediate)
        while (node_t0.READ_RESULT == NO_RESULT) node_t0.CAS_RESULT(NO_RESULT, INTERMEDIATE_READY)
        true
      } else if (state_t1 eq StolenOrExpanded) {
        // help expansion if necessary
        if (tree.READ.isLeaf) tree.markStolenAndExpand(this, worker)

        val node_t2 = tree.READ
        storeIntermediateResult(node_t2, intermediate)
        while (node_t2.result == NO_RESULT) node_t2.CAS_RESULT(NO_RESULT, INTERMEDIATE_READY)
        false
      } else sys.error("unreachable: " + state_t1 + ", " + node_t0.toString(0))

      // push result up as far as possible
      pushUp(tree, worker)

      wasCompleted
    }

    /**
     * Pushes the result up the tree
     *
     * After completing node worker tryes to push the result up the tree, as far as he could
     */
    @tailrec protected final def pushUp(tree: Ref[T, R], worker: WorkerTask): Unit = {
      val node_t0 = tree.READ
      val r_t1 = node_t0.READ_RESULT
      r_t1 match {
        case NO_RESULT =>
          // we're done, owner did not finish his work yet
        case INTERMEDIATE_READY =>
          val combinedResult = try {
            if (node_t0.isLeaf) node_t0.READ_INTERMEDIATE.asInstanceOf[AnyRef]
            else {
              // check if result already set for children
              def available(r: AnyRef) = r != NO_RESULT && r != INTERMEDIATE_READY
              val lr = node_t0.left.READ.READ_RESULT
              val rr = node_t0.right.READ.READ_RESULT
              if (available(lr) && available(rr)) {
                val subr = combine(lr.asInstanceOf[R], rr.asInstanceOf[R])
                combine(node_t0.READ_INTERMEDIATE, subr).asInstanceOf[AnyRef]
              } else NO_RESULT
            }
          } catch {
            case t: Throwable =>
              setTerminationCause(new ThrowCause(t))
              null
          }

          if (combinedResult != NO_RESULT) {
            if (node_t0.CAS_RESULT(r_t1, combinedResult)) {
              // if at root, notify completion, otherwise go one level up
              if (tree.up == null) tree.synchronized {
                tree.notifyAll()
              }
              else {
                pushUp(tree.up, worker)
              }
            } else {
              pushUp(tree, worker) // retry
            }
          } // one of the children is not ready yet
        case _ =>
          // we're done, somebody else already pushed up
      }
    }

    /**
     * The neutral element of the reduction.
     */
    def zero: R

    /**
     * Combines results from two chunks into the aggregate result.
     */
    def combine(a: R, b: R): R

    /**
     * Processes the specified chunk.
     */
    def apply(node: Node[T, R], chunkSize: Int): R
  }

  final class Ref[T, R](val up: Ref[T, R], val level: Int)(@volatile var child: Node[T, R]) {
    def CAS(ov: Node[T, R], nv: Node[T, R]) = unsafe.compareAndSwapObject(this, CHILD_OFFSET, ov, nv)
    def WRITE(nv: Node[T, R]) = unsafe.putObjectVolatile(this, CHILD_OFFSET, nv)
    def READ = unsafe.getObjectVolatile(this, CHILD_OFFSET).asInstanceOf[Node[T, R]]

    /**
     * Try to mark the node as stolen, expand the node and return `true` if node was expanded.
     *  Return `false` if node was completed.
     */
    @tailrec def markStolenAndExpand(kernel: Kernel[T, R], worker: WorkerTask): Boolean = {
      import Stealer._
      val child_t0 = READ
      val stealer_t0 = child_t0.stealer
      if (!child_t0.isLeaf) true else { // already expanded
        val state_t1 = stealer_t0.state
        if (state_t1 eq Completed) false // already completed
        else {
          if (state_t1 ne StolenOrExpanded) { // mark it stolen
            if (stealer_t0.markStolen()) markStolenAndExpand(kernel, worker) // marked stolen - now move on to node creation
            else markStolenAndExpand(kernel, worker) // wasn't marked stolen and failed marking stolen - retry
          } else { // already marked stolen - expand
            // node effectively immutable (except for `intermediateResult` and `result`) - expand it
            val expanded = child_t0.newExpanded(this, worker)
            kernel.afterExpand(child_t0, expanded)
            if (CAS(child_t0, expanded)) true // try to replace with expansion
            else markStolenAndExpand(kernel, worker) // failure (spurious or due to another expand) - retry
          }
        }
      }
    }

    def toString(l: Int): String = "\t" * level + "Ref" + level + " -> " + child.toString(l)

  }

  final class Node[@specialized T, R](val left: Ref[T, R], val right: Ref[T, R])(val stealer: Stealer[T]) {
    @volatile var step: Int = 1
    @volatile var owner: WorkerTask = null
    @volatile var intermediateResult: R = _
    @volatile var result: AnyRef = NO_RESULT

    def CAS_OWNER(ov: WorkerTask, nv: WorkerTask) = unsafe.compareAndSwapObject(this, OWNER_OFFSET, ov, nv)
    def WRITE_OWNER(nv: WorkerTask) = unsafe.putObjectVolatile(this, OWNER_OFFSET, nv)
    def READ_OWNER = unsafe.getObjectVolatile(this, OWNER_OFFSET).asInstanceOf[WorkerTask]

    def CAS_RESULT(ov: AnyRef, nv: AnyRef) = unsafe.compareAndSwapObject(this, RESULT_OFFSET, ov, nv)
    def WRITE_RESULT(nv: AnyRef) = unsafe.putObjectVolatile(this, RESULT_OFFSET, nv)
    def READ_RESULT = unsafe.getObjectVolatile(this, RESULT_OFFSET).asInstanceOf[AnyRef]

    def READ_INTERMEDIATE = intermediateResult
    def WRITE_INTERMEDIATE(v: R) = intermediateResult = v

    def READ_STEP = step
    def WRITE_STEP(v: Int) = step = v

    final def isLeaf = left eq null

    @tailrec final def tryOwn(thiz: WorkerTask): Boolean = {
      val currowner = READ_OWNER
      if (currowner eq thiz) true
      else if (currowner != null) false
      else if (CAS_OWNER(currowner, thiz)) true
      else tryOwn(thiz)
    }

    final def trySteal(parent: Ref[T, R], kernel: Kernel[T, R], worker: WorkerTask): Boolean = {
      parent.markStolenAndExpand(kernel, worker)
    }

    final def newExpanded(parent: Ref[T, R], worker: WorkerTask): Node[T, R] = {
      val (lstealer, rstealer) = stealer.split

      val lnode = new Node[T, R](null, null)(lstealer)
      lnode.owner = this.READ_OWNER
      val lref = new Ref(parent, parent.level + 1)(lnode)

      val rnode = new Node[T, R](null, null)(rstealer)
      rnode.owner = if (this.READ_OWNER eq worker) null else worker
      val rref = new Ref(parent, parent.level + 1)(rnode)

      val nnode = new Node[T, R](lref, rref)(this.stealer)
      nnode.owner = this.READ_OWNER

      nnode
    }

    def isLeafEligibleForWork(worker: WorkerTask): Boolean = {
      import Stealer._
      val decision = stealer.state match {
        case Completed =>
          (owner == null) || ((owner eq worker) && result == NO_RESULT)
        case StolenOrExpanded =>
          true
        case AvailableOrOwned =>
          stealer.elementsRemainingEstimate > stealer.minimumStealThreshold || (owner == null) || (owner eq worker)
      }
      decision
    }

    def isInnerEligibleForWork(worker: WorkerTask): Boolean = {
      import Stealer._
      // we know it is StolenOrExpanded, so we do not have to check this
      (owner eq worker) && READ_RESULT == NO_RESULT
    }

    final def toString(lev: Int): String = {
      val subtree = if (!isLeaf) {
        "\n" + left.toString(lev + 1) + right.toString(lev + 1)
      } else "\n"
      stealer.toString + subtree
    }
  }

  abstract class Strategy {
    /**
     * Finds work in the tree for the given worker, which is one out of `total` workers.
     *  This search may include stealing work.
     */
    def findWork[T, R](worker: WorkerTask, tree: Ref[T, R], kernel: Kernel[T, R]): Ref[T, R]

    /**
     * Returns true if the worker labeled with `index` with a total of
     *  `total` workers should go left at level `level`.
     *  Returns false otherwise.
     */
    def choose[T, R](index: Int, total: Int, tree: Ref[T, R]): Boolean

    /** Which node the stealer takes at this level. */
    def chooseAsStealer[T, R](index: Int, total: Int, tree: Ref[T, R]): Ref[T, R]

    /** Which node the victim takes at this level. */
    def chooseAsVictim[T, R](index: Int, total: Int, tree: Ref[T, R]): Ref[T, R]
  }

  object FindMax extends Strategy {

    @tailrec def findWork[T, R](worker: WorkerTask, tree: Ref[T, R], kernel: Kernel[T, R]) = {
      def search(current: Ref[T, R]): Ref[T, R] = {
        val node = current.READ
        if (node.isLeaf) {
          if (node.isLeafEligibleForWork(worker)) current else null
        } else if (node.isInnerEligibleForWork(worker)) {
          current
        } else {
          val left = search(node.left)
          val right = search(node.right)
          if (left != null && right != null) {
            val leftwork = left.READ.stealer.elementsRemainingEstimate
            val rightwork = right.READ.stealer.elementsRemainingEstimate
            if (leftwork > rightwork) left else right
          } else if (left != null) left else right
        }
      }

      val max = search(tree)
      if (max != null) {
        val node = /*READ*/ max.child
        if (node.tryOwn(worker)) max
        else if (node.trySteal(max, kernel, worker)) {
          val subnode = chooseAsStealer(worker.index, worker.total, max)
          if (subnode.child.tryOwn(worker)) subnode
          else findWork(worker, tree, kernel)
        } else findWork(worker, tree, kernel)
      } else {
        null
      }
    }

    def choose[T, R](index: Int, total: Int, tree: Ref[T, R]) = sys.error("never called")

    def chooseAsStealer[T, R](index: Int, total: Int, tree: Ref[T, R]) = tree.READ.right

    def chooseAsVictim[T, R](index: Int, total: Int, tree: Ref[T, R]) = tree.READ.left

  }

  def localRandom = scala.concurrent.forkjoin.ThreadLocalRandom.current

  abstract class FindFirstStrategy extends Strategy {

    final def findWork[T, R](worker: WorkerTask, tree: Ref[T, R], kernel: Kernel[T, R]) = {
      val index = worker.index
      val total = worker.total
      val node = tree.READ
      if (node.isLeaf) {
        if (!node.isLeafEligibleForWork(worker)) null // no further expansions
        else {
          // more work
          if (node.tryOwn(worker)) tree
          else if (node.trySteal(tree, kernel, worker)) {
            val subnode = chooseAsStealer(index, total, tree)
            if (subnode.READ.tryOwn(worker)) subnode
            else findWork[T, R](worker, tree, kernel)
          } else findWork[T, R](worker, tree, kernel)
        }
      } else if (node.isInnerEligibleForWork(worker)) {
        tree
      } else {
        // descend deeper
        if (choose(index, total, tree)) {
          val ln = findWork[T, R](worker, node.left, kernel)
          if (ln != null) ln else findWork[T, R](worker, node.right, kernel)
        } else {
          val rn = findWork[T, R](worker, node.right, kernel)
          if (rn != null) rn else findWork[T, R](worker, node.left, kernel)
        }
      }
    }

  }

  object AssignTop extends FindFirstStrategy {

    /** Returns true iff the level of the tree is such that: 2^level < total */
    final def isTreeTop(total: Int, level: Int): Boolean = (1 << level) < total

    /** Returns true iff the worker should first go left at this level of the tree top. */
    final def chooseInTreeTop(index: Int, level: Int): Boolean = ((index >> level) & 0x1) == 0

    def choose[T, R](index: Int, total: Int, tree: Ref[T, R]): Boolean = {
      val level = tree.level
      if (isTreeTop(total, level)) {
        chooseInTreeTop(index, level)
      } else localRandom.nextBoolean
    }

    def chooseAsStealer[T, R](index: Int, total: Int, tree: Ref[T, R]): Ref[T, R] = {
      val level = tree.level
      if (isTreeTop(total, level)) {
        if (chooseInTreeTop(index, level)) tree.READ.left
        else tree.child.right
      } else tree.child.right
    }

    def chooseAsVictim[T, R](index: Int, total: Int, tree: Ref[T, R]): Ref[T, R] = {
      val level = tree.level
      if (isTreeTop(total, level)) {
        if (chooseInTreeTop(index, level)) tree.READ.left
        else tree.child.right
      } else tree.child.left
    }

  }

  object AssignTopLeaf extends FindFirstStrategy {

    final def isTreeTop(total: Int, level: Int): Boolean = (1 << level) < total

    final def chooseInTreeTop(index: Int, level: Int): Boolean = ((index >> level) & 0x1) == 0

    def choose[T, R](index: Int, total: Int, tree: Ref[T, R]): Boolean = {
      val level = tree.level
      if (isTreeTop(total, level) && tree.child.isLeaf) {
        chooseInTreeTop(index, level)
      } else localRandom.nextBoolean
    }

    def chooseAsStealer[T, R](index: Int, total: Int, tree: Ref[T, R]): Ref[T, R] = {
      val level = tree.level
      if (isTreeTop(total, level)) {
        if (chooseInTreeTop(index, level)) tree.READ.left
        else tree.child.right
      } else tree.child.right
    }

    def chooseAsVictim[T, R](index: Int, total: Int, tree: Ref[T, R]): Ref[T, R] = {
      val level = tree.level
      if (isTreeTop(total, level)) {
        if (chooseInTreeTop(index, level)) tree.READ.left
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

    def choose[T, R](index: Int, total: Int, tree: Ref[T, R]): Boolean = {
      val levelmod = tree.level % log2(total)
      ((index >> levelmod) & 0x1) == 0
    }

    def chooseAsStealer[T, R](index: Int, total: Int, tree: Ref[T, R]): Ref[T, R] = {
      if (choose(index, total, tree)) tree.READ.left
      else tree.child.right
    }

    def chooseAsVictim[T, R](index: Int, total: Int, tree: Ref[T, R]): Ref[T, R] = {
      if (choose(index, total, tree)) tree.READ.left
      else tree.child.right
    }

  }

  object RandomWalk extends FindFirstStrategy {

    def choose[T, R](index: Int, total: Int, tree: Ref[T, R]): Boolean = localRandom.nextBoolean

    def chooseAsStealer[T, R](index: Int, total: Int, tree: Ref[T, R]) = tree.READ.right

    def chooseAsVictim[T, R](index: Int, total: Int, tree: Ref[T, R]) = tree.READ.left

  }

  object RandomAll extends FindFirstStrategy {

    def choose[T, R](index: Int, total: Int, tree: Ref[T, R]): Boolean = localRandom.nextBoolean

    def chooseAsStealer[T, R](index: Int, total: Int, tree: Ref[T, R]) = if (localRandom.nextBoolean) tree.READ.left else tree.READ.right

    def chooseAsVictim[T, R](index: Int, total: Int, tree: Ref[T, R]) = if (localRandom.nextBoolean) tree.READ.left else tree.READ.right

  }

  object Predefined extends FindFirstStrategy {

    def choose[T, R](index: Int, total: Int, tree: Ref[T, R]): Boolean = true

    def chooseAsStealer[T, R](index: Int, total: Int, tree: Ref[T, R]) = tree.READ.right

    def chooseAsVictim[T, R](index: Int, total: Int, tree: Ref[T, R]) = tree.READ.left

  }

}

