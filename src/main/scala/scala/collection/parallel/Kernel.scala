package scala.collection.parallel






abstract class Kernel[N <: WorkstealingScheduler.Node[N, T], T] {
  /** The neutral element of the reduction.
   */
  def zero: T

  /** Combines results from two chunks into the aggregate result.
   */
  def combine(a: T, b: T): T

  /** Processes the specified chunk.
   */
  def apply(progress: Int, nextProgress: Int, totalSize: Int): T

  /** Returns true if completed with no stealing.
   *  Returns false if steal occurred.
   *
   *  May be overridden in subclass to specialize for better performance.
   */
  def workOn(ws: WorkstealingScheduler)(tree: WorkstealingScheduler.Ptr[N, T], size: Int): Boolean

  /** Returns the root of a fresh workstealing tree.
   */
  def newRoot: WorkstealingScheduler.Ptr[N, T]

  /** Problem size. */
  def size: Int

}


object Kernel {

  import WorkstealingScheduler.{Ptr, Node, IndexNode, RangeNode}

  abstract class Range[T] extends Kernel[RangeNode[T], T] {
    def newRoot: Ptr[RangeNode[T], T] = {
      val work = new RangeNode[T](null, null)(0, size, IndexNode.range(0, size), WorkstealingScheduler.initialStep)
      val root = new Ptr[RangeNode[T], T](null, 0)(work)
      root
    }

    def workOn(ws: WorkstealingScheduler)(tree: Ptr[RangeNode[T], T], size: Int): Boolean = {
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
      ws.completeNode[RangeNode[T], T](lsum, rsum, tree, this)
    }
  }

}











