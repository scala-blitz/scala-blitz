package scala.collection.par
package workstealing



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.collection.par.generic._
import scala.reflect.ClassTag



object Ranges {

  trait Scope {
    implicit def rangeOps[T <: Range](r: Par[T]) = new Ranges.Ops(r)
    implicit def canMergeRange(implicit ctx: Scheduler): CanMergeFrom[Range, Int, Par[Array[Int]]] = new CanMergeFrom[Range, Int, Par[Array[Int]]] {
      def apply(from: Range) = new Arrays.ArrayMerger[Int](ctx)
      def apply() = new Arrays.ArrayMerger[Int](ctx)
    }
    implicit def canMergeParRange[T <: Range](implicit ctx: Scheduler): CanMergeFrom[Par[T], Int, Par[Array[Int]]] = new 
CanMergeFrom[Par[T], Int, Par[Array[Int]]] {
      def apply(from: Par[T]) = new Arrays.ArrayMerger[Int](ctx)
      def apply() = new Arrays.ArrayMerger[Int](ctx)
    }
    implicit def rangeIsZippable[T <: Range](implicit ctx: Scheduler) = new IsZippable[T, Int] {
      def apply(pr: Par[T]) = new Zippable[Int]{
      def iterator = pr.seq.iterator
      def stealer = new RangeStealer(pr.seq, 0, pr.seq.length)
      def newMerger = new Arrays.ArrayMerger2ZippableMergerConvertor[Int](new Arrays.ArrayMerger[Int](ctx))
      }
    }
  }

  class Ops(val range: Par[collection.immutable.Range]) extends AnyVal with Zippables.OpsLike[Int, Par[collection.immutable.Range]] {
    def r = range.seq
    def stealer: PreciseStealer[Int] = new RangeStealer(r, 0, r.length)
    override def reduce[U >: Int](operator: (U, U) => U)(implicit ctx: Scheduler): U = macro internal.RangesMacros.reduce[U]
    override def mapReduce[R](mapper: Int => R)(reducer: (R, R) => R)(implicit ctx: Scheduler): R = macro internal.RangesMacros.mapReduce[Int,R]
    override def count[U >: Int](p: U => Boolean)(implicit ctx: Scheduler): Int = macro internal.RangesMacros.count[U]
    override def fold[U >: Int](z: => U)(op: (U, U) => U)(implicit ctx: Scheduler): U = macro internal.RangesMacros.fold[U]
    override def aggregate[S](z: S)(combop: (S, S) => S)(seqop: (S, Int) => S)(implicit ctx: Scheduler): S = macro internal.RangesMacros.aggregate[S]
    override def sum[U >: Int](implicit num: Numeric[U], ctx: Scheduler): U = macro internal.RangesMacros.sum[U]
    override def product[U >: Int](implicit num: Numeric[U], ctx: Scheduler): U = macro internal.RangesMacros.product[U]
    override def min[U >: Int](implicit ord: Ordering[U], ctx: Scheduler): Int = macro internal.RangesMacros.min[U]
    override def foreach[U >: Int](action: U => Unit)(implicit ctx: Scheduler): Unit = macro internal.RangesMacros.foreach[U]
    override def max[U >: Int](implicit ord: Ordering[U], ctx: Scheduler): Int = macro internal.RangesMacros.max[U]
    override def find[U >: Int](p: U => Boolean)(implicit ctx: Scheduler): Option[Int] = macro internal.RangesMacros.find[U]
    override def exists[U >: Int](p: U => Boolean)(implicit ctx: Scheduler): Boolean = macro internal.RangesMacros.exists[U]
    override def forall[U >: Int](p: U => Boolean)(implicit ctx: Scheduler): Boolean = macro internal.RangesMacros.forall[U]
    override def map[S, That](func: Int => S)(implicit cmf: CanMergeFrom[Par[Range], S, That], ctx: Scheduler) = macro internal.RangesMacros.map[Int, S, That]
    override def copyToArray[U >: Int](arr: Array[U], start: Int, len: Int)(implicit ctx:Scheduler): Unit = macro internal.RangesMacros.copyToArray[U]
    def copyToArray[U >: Int](arr: Array[U], start: Int)(implicit ctx: Scheduler): Unit = macro internal.RangesMacros.copyToArray2[U]
    def copyToArray[U >: Int](arr: Array[U])(implicit ctx: Scheduler): Unit = macro internal.RangesMacros.copyToArray3[U]
    override def flatMap[S, That](func: Int => TraversableOnce[S])(implicit cmf: CanMergeFrom[Par[Range], S, That], ctx: Scheduler) = macro internal.RangesMacros.flatMap[Int, S, That]
    override def filter[That](pred: Int => Boolean)(implicit cmf: CanMergeFrom[Par[Range], Int, That], ctx: Scheduler) = macro internal.RangesMacros.filter[That]
    def seq = range
    def classTag = implicitly[ClassTag[Int]]
  }

  /* stealer implementation */

  import Scheduler.{ Kernel, Node }

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
    override def toString = {
    val p = READ_PROGRESS
    val dp = decode(p)
    "RangeStealer(id:%d, startIndex:%d, progress:%d, decodedProgress:%d, UntilIndex:%d, nextProgress:%d, nextUntil:%d)".format(java.lang.System.identityHashCode(this), startIndex, p, dp, untilIndex, nextProgress, nextUntil) 
  }
  }

  abstract class RangeKernel[@specialized R] extends IndexedStealer.IndexedKernel[Int, R] {
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
    def apply1(node: Node[Int, R], from: Int, to: Int): R
    def applyN(node: Node[Int, R], from: Int, to: Int, stride: Int): R
  }

  abstract class CopyMapRangeKernel[@specialized S] extends IndexedStealer.IndexedKernel[Int, Unit] {
    import scala.collection.par.Scheduler.{ Ref, Node }
    def zero: Unit = ()
    def combine(a: Unit, b: Unit) = a
    def resultArray: Array[S]
    def apply(node: Node[Int, Unit], chunkSize: Int): Unit = {
      val stealer = node.stealer.asInstanceOf[RangeStealer]
      val nextProgress = stealer.nextProgress
      val nextUntil = stealer.nextUntil
      val range = stealer.range
      val from = range.apply(nextProgress)

      if (nextProgress == nextUntil) applyN(node, from, from, 1)
      else {
        val to = range.apply(nextUntil - 1)
        val step = range.step

        if (step == 1) applyN(node, from, to, 1)
        else if (step == -1) applyN(node, from, to, -1)
        else applyN(node, from, to, step)
      }
    }
    @inline def applyN(node: Node[Int, Unit], from: Int, to: Int, stride: Int): Unit
  }

  val EMPTY_RESULT = new AnyRef

  def newMerger(pa: Par[Range])(implicit ctx: Scheduler): Arrays.ArrayMerger[Int] = {
    val am = pa.seq match {
      case x: Range => new Arrays.ArrayMerger[Int](ctx)
      case null => throw new NullPointerException
    }
    am
  }

}

