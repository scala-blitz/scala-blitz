package scala.collection.parallel
package workstealing



import scala.annotation.tailrec



abstract class IndexedStealer[@specialized T](val startIndex: Int, val untilIndex: Int) extends PreciseStealer[T] {
  import IndexedStealer._

  type StealerType <: IndexedStealer[T]

  @volatile var progress: Int = startIndex
  var nextProgress: Int = _
  var nextUntil: Int = _

  final def READ_PROGRESS = unsafe.getIntVolatile(this, PROGRESS_OFFSET)
  final def WRITE_PROGRESS(nv: Int) = unsafe.putIntVolatile(this, PROGRESS_OFFSET, nv)
  final def CAS_PROGRESS(ov: Int, nv: Int) = unsafe.compareAndSwapInt(this, PROGRESS_OFFSET, ov, nv)

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
  import WorkstealingTreeScheduler._

  val PROGRESS_OFFSET = unsafe.objectFieldOffset(classOf[IndexedStealer[_]].getDeclaredField("progress"))

  trait IndexedKernel[@specialized T, @specialized R] extends Kernel[T, R] {
    override def workOn(tree: Ref[T, R], config: Config, worker: Worker): Boolean = {
      import Stealer._

      // atomically read the current node and initialize
      val node = tree.READ
      val stealer = node.stealer.asInstanceOf[IndexedStealer[T]]
      beforeWorkOn(tree, node)
      var intermediate = node.READ_INTERMEDIATE
      var incCount = 0
      val incFreq = config.incrementStepFrequency
      val ms = config.maximumStep

      // commit to processing chunks of the collection and process them until termination
      val until = stealer.untilIndex
      var looping = true
      while (looping && notTerminated) {
        val currstep = node.READ_STEP
        val currprog = stealer.READ_PROGRESS
  
        if (currprog >= 0 && currprog < until) {
          // reserve some work
          val nprog = math.min(currprog + currstep, until)
          
          if (stealer.CAS_PROGRESS(currprog, nprog)) {
            stealer.nextProgress = currprog
            stealer.nextUntil = nprog
            intermediate = combine(intermediate, apply(node, nprog - currprog))

            // update step
            node.WRITE_STEP(math.min(ms, currstep * 2))
          }
        } else looping = false
      }

      completeIteration(node.stealer)

      // store into the `intermediateResult` field of the node and push result up
      completeNode(intermediate, tree, worker)
    }
  }

}








