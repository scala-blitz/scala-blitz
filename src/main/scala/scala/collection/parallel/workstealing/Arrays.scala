package scala.collection.parallel
package workstealing



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.reflect.ClassTag
import scala.collection.parallel.generic._



object Arrays {

  import WorkstealingTreeScheduler.{ Kernel, Node }

  trait Scope {
    implicit def arrayOps[T](a: Par[Array[T]]) = new Arrays.Ops(a.seq)
    implicit def canMergeArray[T]: CanMergeFrom[Par[Array[_]], T, Par[Array[T]]] = ???
    implicit def arrayIsZippable[T] = new IsZippable[Array[T], T] {
      def apply(pr: Par[Array[T]]) = ???
    }
  }

  class Ops[T](val array: Array[T]) extends AnyVal with Zippables.OpsLike[T, Par[Conc[T]]] {
    def stealer: PreciseStealer[T] = new ArrayStealer(array, 0, array.length)
    def aggregate[S](z: S)(combop: (S, S) => S)(seqop: (S, T) => S)(implicit ctx: WorkstealingTreeScheduler) = macro methods.ArraysMacros.aggregate[T, S]
    override def reduce[U >: T](operator: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler) = macro methods.ArraysMacros.reduce[T, U]
    override def fold[U >: T](z: => U)(op: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler): U = macro methods.ArraysMacros.fold[T,U]
    def sum[U >: T](implicit num: Numeric[U], ctx: WorkstealingTreeScheduler): U = macro methods.ArraysMacros.sum[T,U]
    def product[U >: T](implicit num: Numeric[U], ctx: WorkstealingTreeScheduler): U = macro methods.ArraysMacros.product[T,U]
    def count(p:T=> Boolean)(implicit ctx:WorkstealingTreeScheduler): Int = macro methods.ArraysMacros.count[T]
  }
  
  final class Merger[@specialized(Int, Long, Float, Double) T: ClassTag](
    private[parallel] val maxChunkSize: Int,
    private[parallel] var conc: Conc[T],
    private[parallel] var lastChunk: Array[T],
    private[parallel] var lastSize: Int,
    private val ctx: WorkstealingTreeScheduler
  ) extends Conc.BufferLike[T, Array[T], Merger[T]] {
    def classTag = implicitly[ClassTag[T]]

    def this(mcs: Int, ctx: WorkstealingTreeScheduler) = this(mcs, Conc.Zero, new Array[T](Conc.INITIAL_SIZE), 0, ctx)

    def this(ctx: WorkstealingTreeScheduler) = this(Conc.DEFAULT_MAX_SIZE, ctx)

    def newBuffer(conc: Conc[T]) = new Merger(maxChunkSize, conc, new Array[T](Conc.INITIAL_SIZE), 0, ctx)

    final def +=(elem: T) = if (lastSize < lastChunk.length) {
      lastChunk(lastSize) = elem
      lastSize += 1
      this
    } else {
      expand()
      this += elem
    }

    def result: Array[T] = {
      import workstealing.Ops._
      import Par._

      pack()
      val c = conc
      clear()

      val array = new Array[T](c.size)
      c.toPar.genericCopyToArray(array, 0, array.length)(ctx)
      array
    }
  }

  class ArrayStealer[@specialized(Specializable.AllNumeric) T](val array: Array[T], sidx: Int, eidx: Int) extends IndexedStealer[T](sidx, eidx) {
    var padding8: Int = _
    var padding9: Int = _
    var padding10: Int = _
    var padding11: Int = _
    var padding12: Int = _
    var padding13: Int = _
    var padding14: Int = _
    var padding15: Int = _

    def next(): T = if (hasNext) {
      val res = array(nextProgress)
      nextProgress += 1
      res
    } else throw new NoSuchElementException
  
    def hasNext: Boolean = nextProgress < nextUntil
  
    def split: (ArrayStealer[T], ArrayStealer[T]) = {
      val total = elementsRemainingEstimate
      psplit(total / 2)
    }
  
    def psplit(leftSize: Int): (ArrayStealer[T], ArrayStealer[T]) = {
      val ls = decode(READ_PROGRESS)
      val lu = ls + leftSize
      val rs = lu
      val ru = untilIndex

      (new ArrayStealer[T](array, ls, lu), new ArrayStealer[T](array, rs, ru))
    }
  }

  abstract class ArrayKernel[@specialized(Specializable.AllNumeric) T, @specialized(Specializable.AllNumeric) R] extends IndexedStealer.IndexedKernel[T, R] {
    def apply(node: Node[T, R], chunkSize: Int): R = {
      val stealer = node.stealer.asInstanceOf[ArrayStealer[T]]
      apply(node, stealer.nextProgress, stealer.nextUntil)
    }
    def apply(node: Node[T, R], from: Int, to: Int): R
  }

}




