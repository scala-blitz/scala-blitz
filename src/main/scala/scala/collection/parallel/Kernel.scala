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
  def apply(node: N, chunkSize: Int): R

  /** Returns true if completed with no stealing.
   *  Returns false if steal occurred.
   *
   *  May be overridden in subclass to specialize for better performance.
   */
  def workOn(ws: WorkstealingScheduler)(tree: WorkstealingScheduler.Ptr[N, T, R]): Boolean

}


object Kernel {

  import WorkstealingScheduler.{Ptr, Node, IndexNode, RangeNode}

  abstract class Range[R] extends Kernel[RangeNode[R], Int, R] {
    def applyRange(from: Int, until: Int): R

    def workOn(ws: WorkstealingScheduler)(tree: Ptr[RangeNode[R], Int, R]): Boolean = {
      // do some work
      val node = /*READ*/tree.child
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
            if (node.casRange(currrange, newrange)) lsum = combine(lsum, applyRange(p, newp))
          } else {
            val newu = math.max(p, u - currstep)
            val newrange = IndexNode.range(p, newu)
  
            // do some work on the right
            if (node.casRange(currrange, newrange)) rsum = combine(applyRange(newu, u), rsum)
          }
  
          // update step
          incCount = (incCount + 1) % incFreq
          if (incCount == 0) node.step = /*WRITE*/math.min(maxStep, currstep * 2)
        } else looping = false
      }
  
      // complete node information
      ws.completeNode[RangeNode[R], Int, R](lsum, rsum, tree, this)
    }
  }

  abstract class Iterative[N <: Node[N, T, R], T, R] extends Kernel[N, T, R] {

    def workOn(ws: WorkstealingScheduler)(tree: Ptr[N, T, R]): Boolean = {
      // do some work
      val node = /*READ*/tree.child
      var lsum = zero
      var rsum = zero
      var incCount = 0
      val incFreq = ws.incrementFrequency
      val maxStep = ws.maxStep
      var looping = true
      while (looping) {
        val currstep = /*READ*/node.step
        val currstate = /*READ*/node.state
  
        if (currstate != Node.Completed && currstate != Node.StolenOrExpanded) {
          // reserve some work
          val chunk = node.advance(currstep)

          if (chunk != -1) lsum = combine(lsum, apply(node, chunk))
  
          // update step
          incCount = (incCount + 1) % incFreq
          if (incCount == 0) node.step = /*WRITE*/math.min(maxStep, currstep * 2)
        } else looping = false
      }
  
      // complete node information
      ws.completeNode[N, T, R](lsum, rsum, tree, this)
    }
  }

}











