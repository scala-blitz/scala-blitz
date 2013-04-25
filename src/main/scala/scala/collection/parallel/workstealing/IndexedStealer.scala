package scala.collection.parallel
package workstealing



import scala.annotation.tailrec



abstract class IndexedStealer[@specialized T](val startIndex: Int, val untilIndex: Int) extends PreciseStealer[T] {
  import IndexedStealer._

  type StealerType <: IndexedStealer[T]

  @volatile var progress: Int = startIndex
  var nextProgress: Int = _
  var nextUntil: Int = _

  def READ_PROGRESS = unsafe.getIntVolatile(this, PROGRESS_OFFSET)
  def WRITE_PROGRESS(nv: Int) = unsafe.putIntVolatile(this, PROGRESS_OFFSET, nv)
  def CAS_PROGRESS(ov: Int, nv: Int) = unsafe.compareAndSwapInt(this, PROGRESS_OFFSET, ov, nv)

  def newStealer(start: Int, until: Int): StealerType

  final def state: Stealer.State = {
    val p_t0 = READ_PROGRESS
    if (p_t0 == untilIndex) Stealer.Completed
    else if (p_t0 < 0) Stealer.StolenOrExpanded
    else Stealer.AvailableOrOwned
  }

  def hasNext: Boolean = nextProgress < nextUntil

  @tailrec final def advance(step: Int): Int = {
    val p_t0 = READ_PROGRESS
    if (p_t0 == untilIndex || p_t0 < 0) -1
    else {
      val np = math.min(p_t0 + step, untilIndex)
      if (CAS_PROGRESS(p_t0, np)) {
        nextProgress = p_t0
        nextUntil = np
        np - p_t0
      } else advance(step)
    }
  }

  @tailrec final def markStolen(): Boolean = {
    val p_t0 = READ_PROGRESS
    if (p_t0 == untilIndex) false // completed
    else if (p_t0 < 0) true // stolen already
    else {
      val np = -p_t0 - 1
      if (CAS_PROGRESS(p_t0, np)) true
      else markStolen()
    }
  }

  @tailrec final def markCompleted(): Boolean = {
    val p_t0 = READ_PROGRESS
    if (p_t0 == untilIndex) true // already completed
    else if (p_t0 < 0) false // stolen
    else {
      val np = untilIndex
      if (CAS_PROGRESS(p_t0, np)) true
      else markCompleted()
    }
  }

  def psplit(leftsize: Int): (StealerType, StealerType) = {
    val p = decode(READ_PROGRESS)
    val mid = math.min(p + leftsize, untilIndex)

    val left = newStealer(p, mid)
    val right = newStealer(mid, untilIndex)

    (left, right)
  }

  def split: (StealerType, StealerType) = psplit(elementsRemainingEstimate / 2)

  def elementsRemainingEstimate: Int = untilIndex - decode(READ_PROGRESS)

  private def decode(p: Int) = if (p >= 0) p else -p - 1

  override def toString = {
    val p = READ_PROGRESS
    val dp = decode(p)
    "IndexedStealer(%d, %d, %d)".format(startIndex, p, dp, untilIndex)
  }

}


object IndexedStealer {

  val PROGRESS_OFFSET = unsafe.objectFieldOffset(classOf[IndexedStealer[_]].getDeclaredField("progress"))

}