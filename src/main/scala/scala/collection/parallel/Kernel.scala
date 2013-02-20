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

}


object Kernel {

  import WorkstealingScheduler.{Ptr, Node, IndexNode, RangeNode}

  abstract class Range[R] extends Kernel[RangeNode[R], Int, R] {
    def applyRange(from: Int, until: Int): R
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











