package scala.collection.parallel
package workstealing



import annotation.tailrec



abstract class WorkstealingTreeScheduler {
  import WorkstealingTreeScheduler._

  def invokeParallelOperation[T, R](kernel: Kernel[T, R]): R

}


object WorkstealingTreeScheduler {

  val CHILD_OFFSET = unsafe.objectFieldOffset(classOf[Ptr[_, _]].getDeclaredField("child"))

  type Owner = Null

  trait Kernel[@specialized T, R] {
    /** Initializes a node that has just been expanded.
     * 
     *  By default does nothing, but some kernels may choose to override this default behaviour
     *  to store operation-specific information into the node.
     */
    def afterExpand(old: Node[T, R], node: Node[T, R]) {}
  }

  final class Ptr[T, R](val up: Ptr[T, R], val level: Int)(@volatile var child: Node[T, R]) {
    def CAS(ov: Node[T, R], nv: Node[T, R]) = unsafe.compareAndSwapObject(this, CHILD_OFFSET, ov, nv)
    def WRITE(nv: Node[T, R]) = unsafe.putObjectVolatile(this, CHILD_OFFSET, nv)
    def READ(nv: Node[T, R]) = unsafe.getObjectVolatile(this, CHILD_OFFSET).asInstanceOf[Node[T, R]]

    /** Try to expand node and return true if node was expanded.
     *  Return false if node was completed.
     */
    @tailrec def markExpand(kernel: Kernel[T, R], worker: Owner): Boolean = {
      import Stealer._
      val child_t0 = READ(child)
      val stealer_t0 = child_t0.stealer
      if (!child_t0.isLeaf) true else { // already expanded
        // first set progress to -progress
        val state_t1 = stealer_t0.state
        if (state_t1 eq Completed) false // already completed
        else {
          if (state_t1 ne StolenOrExpanded) {
            if (stealer_t0.markStolen()) markExpand(kernel, worker) // marked stolen - now move on to node creation
            else markExpand(kernel, worker) // wasn't marked stolen and failed marking stolen - retry
          } else { // already marked stolen
            // node effectively immutable (except for `lresult`, `rresult` and `result`) - expand it
            val expanded = child_t0.newExpanded(this, worker)
            kernel.afterExpand(child_t0, expanded)
            if (CAS(child_t0, expanded)) true // try to replace with expansion
            else markExpand(kernel, worker) // failure (spurious or due to another expand) - retry
          }
        }
      }
    }

  }

  final class Node[@specialized T, R](val left: Ptr[T, R], val right: Ptr[T, R])(val stealer: Stealer[T]) {
    @volatile var step: Int = 1

    final def isLeaf = left eq null

    final def newExpanded(ptr: Ptr[T, R], worker: Owner) = {
      ???
    }
  }

}

