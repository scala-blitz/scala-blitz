package scala.collection.parallel
package workstealing



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.collection.parallel.generic._



object Ranges {

  trait Scope {
    implicit def rangeOps(r: Par[collection.immutable.Range]) = new Ranges.Ops(r.xs)

    implicit def canMergeRange[T]: CanMergeFrom[Par[Range], Int, Par[Range]] = ???
  }

  class Ops(val r: collection.immutable.Range) extends AnyVal with Zippable.OpsLike[Int, Par[collection.immutable.Range]] {
    def stealer: Stealer[Int] = new RangeStealer(r, 0, r.length)
    override def reduce[U >: Int](op: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler): U = macro methods.RangesMacros.reduce[U]
  }

  /* stealer implementation */

  import WorkstealingTreeScheduler.{ Kernel, Node }

  class RangeStealer(val range: collection.immutable.Range, start: Int, end: Int) extends IndexedStealer[Int](start, end) {
    type StealerType = RangeStealer

    var padding8: Int = _
    var padding9: Int = _
    var padding10: Int = _
    var padding11: Int = _
    var padding12: Int = _
    var padding13: Int = _
    var padding14: Int = _
    var padding15: Int = _

    def next(): Int = {
      val idx = nextProgress
      nextProgress += 1
      range.apply(idx)
    }

    def newStealer(s: Int, u: Int) = new RangeStealer(range, s, u)
  }

  abstract class RangeKernel[R] extends Kernel[Int, R] {
    def apply(node: Node[Int, R], chunkSize: Int): R = {
      val stealer = node.stealer.asInstanceOf[RangeStealer]
      val startIndex = stealer.nextProgress
      val endIndex = stealer.nextUntil
      val range = stealer.range
      val from = range.apply(startIndex)

      if (startIndex == endIndex) apply0(from)
      else {
        val to = range.apply(endIndex - 1)
        val step = range.step

        if (step == 1) apply1(from, to)
        else applyN(from, to, step)
      }
    }
    def apply0(at: Int): R
    def apply1(from: Int, to: Int): R
    def applyN(from: Int, to: Int, step: Int): R
  }

  val EMPTY_RESULT = new AnyRef

}


