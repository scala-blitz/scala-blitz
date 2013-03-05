package scala.collection.workstealing



import sun.misc.Unsafe
import annotation.tailrec
import scala.collection._



trait IndexedWorkstealing[T] extends Workstealing[T] {

  import IndexedWorkstealing._

  type N[R] <: IndexNode[T, R]

  type K[R] <: IndexKernel[T, R]

  abstract class IndexNode[@specialized S, R](l: Ptr[S, R], r: Ptr[S, R])(val start: Int, val end: Int, @volatile var range: Long, st: Int)
  extends Node[S, R](l, r)(st) {
    @volatile var rresult: R = null.asInstanceOf[R]
    var padding0: Int = 0 // <-- war story
    var padding1: Int = 0
    //var padding2: Int = 0
    //var padding3: Int = 0
    //var padding4: Int = 0

    override def workDone = positiveProgress(range) - start + end - until(range)

    override def nodeString = "[%.2f%%] IndexNode(%s)(%d, %d, %d, %d, %d)(lres = %s, res = %s) #%d".format(
      (positiveProgress(range) - start + end - until(range)).toDouble / size * 100,
      if (owner == null) "none" else "worker " + owner.index,
      start,
      end,
      progress(range),
      until(range),
      step,
      lresult,
      result,
      System.identityHashCode(this)
    )

    final def CAS_RANGE(ov: Long, nv: Long) = Utils.unsafe.compareAndSwapLong(this, RANGE_OFFSET, ov, nv)

    /* node interface */

    final def elementsRemaining = {
      val r = /*READ*/range
      val p = positiveProgress(r)
      val u = until(r)
      u - p
    }

    final def elementsCompleted = {
      val r = /*READ*/range
      val p = positiveProgress(r)
      val u = until(r)
      (p - start) + (end - u)
    }

    final def state = {
      val range_t0 = /*READ*/range
      if (completed(range_t0)) Workstealing.Completed
      else if (stolen(range_t0)) Workstealing.StolenOrExpanded
      else Workstealing.AvailableOrOwned
    }

    final def advance(step: Int): Int = {
      val range_t0 = /*READ*/range
      if (stolen(range_t0) || completed(range_t0)) -1
      else {
        val p = progress(range_t0)
        val u = until(range_t0)
        val newp = math.min(u, p + step)
        if (CAS_RANGE(range_t0, createRange(newp, u))) newp - p
        else -1
      }
    }

    final def markCompleted(): Boolean = {
      val range_t0 = /*READ*/range
      if (completed(range_t0) || stolen(range_t0)) false
      else CAS_RANGE(range_t0, createCompleted(range_t0))
    }

    final def markStolen(): Boolean = {
      val range_t0 = /*READ*/range
      if (completed(range_t0) || stolen(range_t0)) false
      else CAS_RANGE(range_t0, createStolen(range_t0))
    }

  }

  abstract class IndexKernel[@specialized S, R] extends Kernel[S, R] {
    def applyIndex(n: N[R], from: Int, until: Int): R

    def apply(node: N[R], chunkSize: Int) = ???

    def isNotRandom = false

    override def workOn(tree: Ptr[S, R]): Boolean = {
      val rawn = /*READ*/tree.child
      beforeWorkOn(tree, rawn)
      val node = rawn.repr
      var lsum = zero
      var rsum = zero
      var incCount = 0
      val incFreq = config.incrementFrequency
      val ms = config.maxStep
      var looping = true
      val rand = Workstealing.localRandom
      while (looping && notTerminated) {
        val currstep = /*READ*/node.step
        val currrange = /*READ*/node.range
        val p = progress(currrange)
        val u = until(currrange)
  
        if (!stolen(currrange) && !completed(currrange)) {
          if (isNotRandom || rand.nextBoolean) {
            val newp = math.min(u, p + currstep)
            val newrange = createRange(newp, u)

            // do some work on the left
            if (node.CAS_RANGE(currrange, newrange)) lsum = combine(lsum, applyIndex(node, p, newp))
          } else {
            val newu = math.max(p, u - currstep)
            val newrange = createRange(p, newu)

            // do some work on the right
            if (node.CAS_RANGE(currrange, newrange)) rsum = combine(applyIndex(node, newu, u), rsum)
          }
  
          // update step
          incCount = (incCount + 1) % incFreq
          if (incCount == 0) node.step = /*WRITE*/math.min(ms, currstep * 2)
        } else looping = false
      }

      completeIteration(rawn)

      // complete node information
      completeNode(lsum, rsum, tree)
    }

    private def completeNode(lsum: R, rsum: R, tree: Ptr[S, R]): Boolean = {
      val work = tree.child.repr

      val range_t0 = work.range
      val wasCompleted = if (completed(range_t0)) {
        work.lresult = lsum
        work.rresult = rsum
        while (work.result == null) work.CAS_RESULT(null, None)
        //println(Thread.currentThread.getName + " -> " + work.start + " to " + work.progress + "; id=" + System.identityHashCode(work))
        true
      } else if (stolen(range_t0)) {
        // help expansion if necessary
        if (tree.child.isLeaf) tree.expand(this)
        tree.child.repr.lresult = lsum
        tree.child.repr.rresult = rsum
        while (tree.child.result == null) tree.child.CAS_RESULT(null, None)
        //val work = tree.child
        //println(Thread.currentThread.getName + " -> " + work.start + " to " + work.progress + "; id=" + System.identityHashCode(work))
        false
      } else sys.error("unreachable: " + progress(range_t0) + ", " + until(range_t0) + ", " + work.state + ", " + work.toString(0))
  
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
            if (tree.child.isLeaf) Some(combine(tree.child.repr.lresult, tree.child.repr.rresult))
            else {
              // check if result already set for children
              val leftresult = /*READ*/tree.child.left.child.result
              val rightresult = /*READ*/tree.child.right.child.result
              (leftresult, rightresult) match {
                case (Some(lr), Some(rr)) =>
                  val r = combine(tree.child.lresult, combine(lr, combine(rr, tree.child.repr.rresult)))
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

  }

}


object IndexedWorkstealing {
  val RANGE_OFFSET = Utils.unsafe.objectFieldOffset(classOf[IndexedWorkstealing[_]#IndexNode[_, _]].getDeclaredField("range"))

  def createRange(p: Int, u: Int): Long = (p.toLong << 32) | u

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

  def createStolen(r: Long): Long = {
    val p = progress(r)
    val u = until(r)
    val stolenp = -p - 1
    createRange(stolenp, u)
  }

  def createCompleted(r: Long): Long = {
    val p = progress(r)
    val u = until(r)
    val completedp = u
    createRange(completedp, u)
  }
}






