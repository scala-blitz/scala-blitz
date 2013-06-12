package scala.collection.parallel
package workstealing



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.reflect.ClassTag
import scala.collection.parallel.generic._



object Arrays {

  import WorkstealingTreeScheduler.{ Kernel, Node }

  trait Scope {
    implicit def arrayOps[T](a: Par[Array[T]]) = new Arrays.Ops(a)
    implicit def canMergeArray[T: ClassTag](implicit ctx: WorkstealingTreeScheduler): CanMergeFrom[Par[Array[_]], T, Par[Array[T]]] = new CanMergeFrom[Par[Array[_]], T, Par[Array[T]]] {
      def apply(from: Par[Array[_]]) = new ArrayMerger[T](ctx)
      def apply() = new ArrayMerger[T](ctx)
    }
    implicit def arrayIsZippable[T] = new IsZippable[Array[T], T] {
      def apply(pa: Par[Array[T]]) = ??? // TODO
    }
  }

  class Ops[T](val array: Par[Array[T]]) extends AnyVal with Zippables.OpsLike[T, Par[Array[T]]] {
    def stealer: PreciseStealer[T] = new ArrayStealer(array.seq, 0, array.seq.length)
    def aggregate[S](z: S)(combop: (S, S) => S)(seqop: (S, T) => S)(implicit ctx: WorkstealingTreeScheduler) = macro methods.ArraysMacros.aggregate[T, S]
    def foreach[U >: T](action: U => Unit)(implicit ctx: WorkstealingTreeScheduler): Unit = macro methods.ArraysMacros.foreach[T, U]
    def mapReduce[R](mapper: T => R)(reducer: (R, R) => R)(implicit ctx: WorkstealingTreeScheduler): R = macro methods.ArraysMacros.mapReduce[T,T,R]
    override def reduce[U >: T](operator: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler) = macro methods.ArraysMacros.reduce[T, U]
    override def fold[U >: T](z: => U)(op: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler): U = macro methods.ArraysMacros.fold[T, U]
    def sum[U >: T](implicit num: Numeric[U], ctx: WorkstealingTreeScheduler): U = macro methods.ArraysMacros.sum[T, U]
    def product[U >: T](implicit num: Numeric[U], ctx: WorkstealingTreeScheduler): U = macro methods.ArraysMacros.product[T, U]
    def min[U >: T](implicit ord: Ordering[U], ctx: WorkstealingTreeScheduler): U = macro methods.ArraysMacros.min[T, U]
    def max[U >: T](implicit ord: Ordering[U], ctx: WorkstealingTreeScheduler): U = macro methods.ArraysMacros.max[T, U]
    def find[U >: T](p: U => Boolean)(implicit ctx: WorkstealingTreeScheduler): Option[T] = macro methods.ArraysMacros.find[T, U]
    def exists[U >: T](p: Int => Boolean)(implicit ctx: WorkstealingTreeScheduler): Boolean = macro methods.ArraysMacros.exists[T, U]
    def forall[U >: T](p: Int => Boolean)(implicit ctx: WorkstealingTreeScheduler): Boolean = macro methods.ArraysMacros.forall[T, U]
    def count[U >: T](p: U => Boolean)(implicit ctx: WorkstealingTreeScheduler): Int = macro methods.ArraysMacros.count[T, U]
    override def map[S, That](func: T => S)(implicit cmf: CanMergeFrom[Par[Array[T]], S, That], ctx: WorkstealingTreeScheduler) = macro methods.ArraysMacros.map[T, S, That]
    def filter(pred: T => Boolean)(implicit ctx: WorkstealingTreeScheduler) = macro methods.ArraysMacros.filter[T]
    def flatMap[S, That](func: T => TraversableOnce[S])(implicit cmf: CanMergeFrom[Par[Array[T]], S, That], ctx: WorkstealingTreeScheduler) = macro methods.ArraysMacros.flatMap[T, S, That]
  }

  final class ArrayMerger[@specialized(Int, Long, Float, Double) T: ClassTag](
    private[parallel] val maxChunkSize: Int,
    private[parallel] var conc: Conc[T],
    private[parallel] var lastChunk: Array[T],
    private[parallel] var lastSize: Int,
    private val ctx: WorkstealingTreeScheduler) extends Conc.BufferLike[T, Par[Array[T]], ArrayMerger[T]] with collection.parallel.Merger[T, Par[Array[T]]] {
    def classTag = implicitly[ClassTag[T]]

    def this(mcs: Int, ctx: WorkstealingTreeScheduler) = this(mcs, Conc.Zero, new Array[T](Conc.INITIAL_SIZE), 0, ctx)

    def this(ctx: WorkstealingTreeScheduler) = this(Conc.DEFAULT_MAX_SIZE, ctx)

    def newBuffer(conc: Conc[T]) = new ArrayMerger(maxChunkSize, conc, new Array[T](Conc.INITIAL_SIZE), 0, ctx)

    def concToTo(c: Conc[T]) = {
      import workstealing.Ops._
      import Par._

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

  def newArrayMerger[T](pa: Par[Array[T]])(implicit ctx: WorkstealingTreeScheduler): ArrayMerger[T] = {
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

  abstract class CopyMapArrayKernel[T, @specialized S] extends scala.collection.parallel.workstealing.Arrays.ArrayKernel[T, Unit] {
    import scala.collection.parallel.workstealing.WorkstealingTreeScheduler.{ Ref, Node }
    def zero: Unit = ()
    def combine(a: Unit, b: Unit) = a
    def resultArray: Array[S]
  }

}

