package scala.collection.par
package workstealing



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.reflect.ClassTag
import scala.collection.par.generic._



object Arrays {

  import Scheduler.{ Kernel, Node }

  trait Scope {
    implicit def arrayOps[T](a: Par[Array[T]]) = new Arrays.Ops(a)

    implicit def canMergeArray[@specialized(Int, Long, Float, Double) T: ClassTag](implicit ctx: Scheduler): CanMergeFrom[Par[Array[_]], T, Par[Array[T]]] = new CanMergeFrom[Par[Array[_]], T, Par[Array[T]]] {
      def apply(from: Par[Array[_]]) = new ArrayMerger[T](ctx)
      def apply() = new ArrayMerger[T](ctx)
    }

    implicit def arrayIsZippable[T](implicit ctx: Scheduler): IsZippable[Array[T], T] = new Array2ZippableConvertor[T]()
  }

  class Ops[T](val array: Par[Array[T]]) extends AnyVal with Zippables.OpsLike[T, Par[Array[T]]] {
    def stealer: PreciseStealer[T] = new ArrayStealer(array.seq, 0, array.seq.length)
    override def aggregate[S](z: S)(combop: (S, S) => S)(seqop: (S, T) => S)(implicit ctx: Scheduler) = macro internal.ArraysMacros.aggregate[T, S]
    def accumulate[S](merger: Merger[T, S])(implicit ctx: Scheduler): S = macro internal.ArraysMacros.accumulate[T, S]
    override def foreach[U >: T](action: U => Unit)(implicit ctx: Scheduler): Unit = macro internal.ArraysMacros.foreach[T, U]
    override def mapReduce[R](mapper: T => R)(reducer: (R, R) => R)(implicit ctx: Scheduler): R = macro internal.ArraysMacros.mapReduce[T, T, R]
    override def reduce[U >: T](operator: (U, U) => U)(implicit ctx: Scheduler) = macro internal.ArraysMacros.reduce[T, U]
    override def fold[U >: T](z: => U)(op: (U, U) => U)(implicit ctx: Scheduler): U = macro internal.ArraysMacros.fold[T, U]
    override def sum[U >: T](implicit num: Numeric[U], ctx: Scheduler): U = macro internal.ArraysMacros.sum[T, U]
    override def product[U >: T](implicit num: Numeric[U], ctx: Scheduler): U = macro internal.ArraysMacros.product[T, U]
    override def min[U >: T](implicit ord: Ordering[U], ctx: Scheduler): U = macro internal.ArraysMacros.min[T, U]
    override def max[U >: T](implicit ord: Ordering[U], ctx: Scheduler): U = macro internal.ArraysMacros.max[T, U]
    override def find[U >: T](p: U => Boolean)(implicit ctx: Scheduler): Option[T] = macro internal.ArraysMacros.find[T, U]
    override def exists[U >: T](p: U => Boolean)(implicit ctx: Scheduler): Boolean = macro internal.ArraysMacros.exists[T, U]
    override def forall[U >: T](p: U => Boolean)(implicit ctx: Scheduler): Boolean = macro internal.ArraysMacros.forall[T, U]
    override def count[U >: T](p: U => Boolean)(implicit ctx: Scheduler): Int = macro internal.ArraysMacros.count[T, U]
    override def map[S, That](func: T => S)(implicit cmf: CanMergeFrom[Par[Array[T]], S, That], ctx: Scheduler) = macro internal.ArraysMacros.map[T, S, That]
    override def filter[That](pred: T => Boolean)(implicit cmf: CanMergeFrom[Par[Array[T]], T, That], ctx: Scheduler) = macro internal.ArraysMacros.filter[T, That]
    override def flatMap[S, That](func: T => TraversableOnce[S])(implicit cmf: CanMergeFrom[Par[Array[T]], S, That], ctx: Scheduler) = macro internal.ArraysMacros.flatMap[T, S, That]
    def seq = array
  }

  class Array2ZippableConvertor[T](implicit ctx: Scheduler) extends IsZippable[Array[T], T] {
    def apply(pa: Par[Array[T]]) = new Zippable[T] {
      def iterator = pa.seq.iterator
      def stealer = new ArrayStealer(pa.seq, 0, pa.seq.length)
      def newMerger = new ArrayMerger2ZippableMergerConvertor[T](Arrays.newArrayMerger(pa))
    }

  }

  final class ArrayMerger[@specialized(Int, Long, Float, Double) T: ClassTag](
    private[par] val maxChunkSize: Int,
    private[par] var conc: Conc[T],
    private[par] var lastChunk: Array[T],
    private[par] var lastSize: Int,
    private val ctx: Scheduler) extends Conc.BufferLike[T, Par[Array[T]], ArrayMerger[T]] with collection.par.Merger[T, Par[Array[T]]] {
    def classTag = implicitly[ClassTag[T]]

    def this(mcs: Int, ctx: Scheduler) = this(mcs, Conc.Zero, new Array[T](Conc.INITIAL_SIZE), 0, ctx)

    def this(ctx: Scheduler) = this(Conc.DEFAULT_MAX_SIZE, ctx)

    def newBuffer(conc: Conc[T]) = new ArrayMerger(maxChunkSize, conc, new Array[T](Conc.INITIAL_SIZE), 0, ctx)

    def concToTo(c: Conc[T]) = {
      val array = new Array[T](c.size)
      c.toPar.genericCopyToArray(array, 0, array.length)(ctx)
      new Par(array)
    }

    final def +=(elem: T) = if (lastSize < lastChunk.length) {
      lastChunk(lastSize) = elem
      lastSize += 1
      this
    } else {
      expand()
      this += elem
    }

  }

  class ArrayMerger2ZippableMergerConvertor[T](val merger: ArrayMerger[T])(implicit ctx: Scheduler) extends Merger[T, Zippable[T]] {
    def +=(elem: T) = { merger += (elem); this }
    def result = par2zippable(merger.result)(new Array2ZippableConvertor[T])
    def clear() = merger.clear()
    def merge(that: Merger[T, Zippable[T]]) = {
      that match {
        case x: ArrayMerger2ZippableMergerConvertor[T] => new ArrayMerger2ZippableMergerConvertor(merger.merge(x.merger))
        case _ =>
          that.result.iterator.foreach { merger += _ } //todo: unefficient
          this
      }
    }
  }

  def newArrayMerger[T](pa: Par[Array[T]])(implicit ctx: Scheduler): ArrayMerger[T] = {
    val am = pa.seq match {
      case x: Array[AnyRef] => new ArrayMerger[AnyRef](ctx)
      case x: Array[Int] => new ArrayMerger[Int](ctx)
      case x: Array[Double] => new ArrayMerger[Double](ctx)
      case x: Array[Long] => new ArrayMerger[Long](ctx)
      case x: Array[Float] => new ArrayMerger[Float](ctx)
      case x: Array[Char] => new ArrayMerger[Char](ctx)
      case x: Array[Byte] => new ArrayMerger[Byte](ctx)
      case x: Array[Short] => new ArrayMerger[Short](ctx)
      case x: Array[Boolean] => new ArrayMerger[Boolean](ctx)
      case x: Array[Unit] => new ArrayMerger[Unit](ctx)
      case null => throw new NullPointerException
    }
    am.asInstanceOf[ArrayMerger[T]]
  }

  def isArrayMerger[S, That](m: Merger[S, That]) = m.isInstanceOf[ArrayMerger[S]]

  class ArrayStealer[@specialized(Specializable.AllNumeric) T](val array: Array[T], sidx: Int, eidx: Int) extends IndexedStealer.Flat[T](sidx, eidx) {
    var padding8: Int = _
    var padding9: Int = _
    var padding10: Int = _
    var padding11: Int = _
    var padding12: Int = _
    var padding13: Int = _
    var padding14: Int = _
    var padding15: Int = _

    type StealerType = ArrayStealer[T]

    def newStealer(start: Int, until: Int) = new ArrayStealer(array, start, until)

    def next(): T = if (hasNext) {
      val res = array(nextProgress)
      nextProgress += 1
      res
    } else throw new NoSuchElementException
  }

  abstract class ArrayKernel[@specialized(Specializable.AllNumeric) T, @specialized(Specializable.AllNumeric) R] extends IndexedStealer.IndexedKernel[T, R] {
    def apply(node: Node[T, R], chunkSize: Int): R = {
      val stealer = node.stealer.asInstanceOf[ArrayStealer[T]]
      apply(node, stealer.nextProgress, stealer.nextUntil)
    }
    def apply(node: Node[T, R], from: Int, to: Int): R
  }

  type CopyProgress = ProgressStatus

  abstract class CopyMapArrayKernel[T, @specialized(Specializable.AllNumeric) S] extends scala.collection.par.workstealing.Arrays.ArrayKernel[T, Unit] {
    def zero: Unit = ()
    def combine(a: Unit, b: Unit) = a
    def resultArray: Array[S]
  }

}

