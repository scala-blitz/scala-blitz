package scala.collection.parallel



import sun.misc.Unsafe
import annotation.tailrec
import collection._



abstract class IndexedWorkstealingCollection[T] extends WorkstealingCollection[T] {

  import IndexedWorkstealingCollection._

  def size: Int

  abstract class IndexNode[@specialized(Int) S, R](l: Ptr[S, R], r: Ptr[S, R])(val start: Int, val end: Int, @volatile var range: Long, st: Int)
  extends Node[S, R](l, r)(st) {
    var padding0: Int = 0 // <-- war story
    var padding1: Int = 0
    //var padding2: Int = 0
    //var padding3: Int = 0
    //var padding4: Int = 0

    override def workDone = positiveProgress(range) - start + end - until(range)

    override def nodeString = "[%.2f%%] RangeNode(%s)(%d, %d, %d, %d, %d)(lres = %s, rres = %s, res = %s) #%d".format(
      (positiveProgress(range) - start + end - until(range)).toDouble / size * 100,
      if (owner == null) "none" else "worker " + owner.index,
      start,
      end,
      progress(range),
      until(range),
      step,
      lresult,
      rresult,
      result,
      System.identityHashCode(this)
    )

    final def casRange(ov: Long, nv: Long) = Utils.unsafe.compareAndSwapLong(this, RANGE_OFFSET, ov, nv)

    final def workRemaining = {
      val r = /*READ*/range
      val p = progress(r)
      val u = until(r)
      u - p
    }

    final def state = {
      val range_t0 = /*READ*/range
      if (completed(range_t0)) WorkstealingCollection.Completed
      else if (stolen(range_t0)) WorkstealingCollection.StolenOrExpanded
      else WorkstealingCollection.AvailableOrOwned
    }

    final def advance(step: Int): Int = {
      val range_t0 = /*READ*/range
      if (stolen(range_t0) || completed(range_t0)) -1
      else {
        val p = progress(range_t0)
        val u = until(range_t0)
        val newp = math.min(u, p + step)
        if (casRange(range_t0, createRange(newp, u))) newp - p
        else -1
      }
    }

    final def markStolen(): Boolean = {
      val range_t0 = /*READ*/range
      if (completed(range_t0) || stolen(range_t0)) false
      else casRange(range_t0, createStolen(range_t0))
    }

  }

}


object IndexedWorkstealingCollection {
  val RANGE_OFFSET = Utils.unsafe.objectFieldOffset(classOf[IndexedWorkstealingCollection[_]#IndexNode[_, _]].getDeclaredField("range"))

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
}






