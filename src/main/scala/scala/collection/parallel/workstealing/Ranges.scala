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
    override def reduce[U >: Int](op: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler): U = macro methods.RangesMacros.reduce2[U]
    override def fold[U >: Int](z: => U)(op: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler): U = macro methods.RangesMacros.fold[U]
    def aggregate[S](z: S)(combop: (S, S) => S)(seqop: (S, Int) => S)(implicit ctx: WorkstealingTreeScheduler): S = macro methods.RangesMacros.aggregate[S]
    def sum[U >: Int](implicit num: Numeric[U], ctx: WorkstealingTreeScheduler): U = macro methods.RangesMacros.sum[U]
    def product[U >: Int](implicit num: Numeric[U], ctx: WorkstealingTreeScheduler): U = macro methods.RangesMacros.product[U]
    def min[U >: Int](implicit ord: Ordering[U], ctx: WorkstealingTreeScheduler): Int = macro methods.RangesMacros.min[U]
    def max[U >: Int](implicit ord: Ordering[U], ctx: WorkstealingTreeScheduler): Int = macro methods.RangesMacros.max[U]

  }

  /* stealer implementation */

  import WorkstealingTreeScheduler.{ Kernel, Node }

  class RangeStealer(val range: collection.immutable.Range, start: Int, end: Int) extends IndexedStealer.Flat[Int](start, end) {
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

  abstract class RangeKernel[@specialized R, M <: R] extends IndexedStealer.IndexedKernel[Int, R] {
    def apply(node: Node[Int, R], chunkSize: Int): R = {
      val stealer = node.stealer.asInstanceOf[RangeStealer]
      val nextProgress = stealer.nextProgress
      val nextUntil = stealer.nextUntil
      val range = stealer.range
      val from = range.apply(nextProgress)

      if (nextProgress == nextUntil) apply0(node, from)
      else {
        val to = range.apply(nextUntil - 1)
        val step = range.step

        if (step == 1) apply1(node, from, to)
        else applyN(node, from, to, step)
      }
    }
    def apply0(node: Node[Int, R], at: Int): R
    def apply1(node: Node[Int, R], from: Int, to: Int): M
    def applyN(node: Node[Int, R], from: Int, to: Int, stride: Int): M
  }

  val EMPTY_RESULT = new AnyRef

}

