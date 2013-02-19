package scala.collection.parallel






abstract class Kernel[N <: WorkstealingScheduler.Node[N, T, R], T, R] {
  /** The neutral element of the reduction.
   */
  def zero: R

  /** Combines results from two chunks into the aggregate result.
   */
  def combine(a: R, b: R): R

  /** Processes the specified chunk.
   */
  def apply(progress: Int, nextProgress: Int, totalSize: Int): R

  /** Returns true if completed with no stealing.
   *  Returns false if steal occurred.
   *
   *  May be overridden in subclass to specialize for better performance.
   */
  def workOn(ws: WorkstealingScheduler)(tree: WorkstealingScheduler.Ptr[N, T, R], size: Int): Boolean

  /** Returns the root of a fresh workstealing tree.
   */
  def newRoot: WorkstealingScheduler.Ptr[N, T, R]

  /** Problem size. */
  def size: Int

}


object Kernel {

  import WorkstealingScheduler.{Ptr, Node, IndexNode, RangeNode}

  abstract class Range[R] extends Kernel[RangeNode[R], Int, R] {
    def newRoot: Ptr[RangeNode[R], Int, R] = {
      val work = new RangeNode[R](null, null)(0, size, IndexNode.range(0, size), WorkstealingScheduler.initialStep)
      val root = new Ptr[RangeNode[R], Int, R](null, 0)(work)
      root
    }

    def workOn(ws: WorkstealingScheduler)(tree: Ptr[RangeNode[R], Int, R], size: Int): Boolean = {
      // do some work
      val node = tree.child
      var lsum = zero
      var rsum = zero
      var incCount = 0
      val incFreq = ws.incrementFrequency
      val maxStep = ws.maxStep
      var looping = true
      val rand = WorkstealingScheduler.localRandom
      while (looping) {
        val currstep = /*READ*/node.step
        val currrange = /*READ*/node.range
        val p = IndexNode.progress(currrange)
        val u = IndexNode.until(currrange)
  
        if (!IndexNode.stolen(currrange) && !IndexNode.completed(currrange)) {
          if (rand.nextBoolean) {
            val newp = math.min(u, p + currstep)
            val newrange = IndexNode.range(newp, u)
  
            // do some work on the left
            if (node.casRange(currrange, newrange)) lsum = combine(lsum, apply(p, newp, size))
          } else {
            val newu = math.max(p, u - currstep)
            val newrange = IndexNode.range(p, newu)
  
            // do some work on the right
            if (node.casRange(currrange, newrange)) rsum = combine(apply(newu, u, size), rsum)
          }
  
          // update step
          incCount = (incCount + 1) % incFreq
          if (incCount == 0) node.step = math.min(maxStep, currstep * 2)
        } else looping = false
      }
  
      // complete node information
      ws.completeNode[RangeNode[R], Int, R](lsum, rsum, tree, this)
    }
  }

}











