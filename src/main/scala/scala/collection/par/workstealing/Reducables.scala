package scala.collection.par
package workstealing



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.collection.par.generic._
import scala.collection.par.workstealing._
import scala.collection.par.generic._
import scala.collection.par.Par
import scala.collection.par.workstealing._
import scala.reflect.ClassTag
import scala.collection.mutable


object Reducibles {

  trait Scope {
    implicit def reducibleOps[T](r: Reducible[T]) = new collection.par.workstealing.Reducibles.Ops[T](r)
    implicit def canMergeReducibleInt(implicit ctx: Scheduler): CanMergeFrom[Reducible[_], Int, Par[Array[Int]]] = new CanMergeFrom[Reducible[_], Int, Par[Array[Int]]] {
      def apply(from: Reducible[_]) = new Arrays.ArrayMerger[Int](ctx)
      def apply() = new Arrays.ArrayMerger[Int](ctx)
    }

    implicit def canMergeReducible[@specialized(Int, Long, Float, Double) T: ClassTag](implicit ctx: Scheduler): CanMergeFrom[Reducible[_], T, Par[Array[T]]] = new CanMergeFrom[Reducible[_], T, Par[Array[T]]] {
      def apply(from: Reducible[_]) = new Arrays.ArrayMerger[T](ctx)
      def apply() = new Arrays.ArrayMerger[T](ctx)
    }

  }

  trait OpsLike[+T, +Repr] extends Any /*with ReducibleOps[T, Repr, Scheduler]*/ {
    def stealer: Stealer[T]
    def seq: Repr
    def reduce[U >: T](op: (U, U) => U)(implicit ctx: Scheduler): U = macro internal.ReduciblesMacros.reduce[T, U, Repr]
    def mapReduce[R](mapper: T => R)(reducer: (R, R) => R)(implicit ctx: Scheduler): R = macro internal.ReduciblesMacros.mapReduce[T, T, R, Repr]
    def fold[U >: T](z: => U)(op: (U, U) => U)(implicit ctx: Scheduler): U = macro internal.ReduciblesMacros.fold[T, U, Repr]
    def aggregate[S](z: S)(combop: (S, S) => S)(seqop: (S, T) => S)(implicit ctx: Scheduler): S = macro internal.ReduciblesMacros.aggregate[T, S, Repr]
    def foreach[U >: T](action: U => Unit)(implicit ctx: Scheduler): Unit = macro internal.ReduciblesMacros.foreach[T, U, Repr]
    def sum[U >: T](implicit num: Numeric[U], ctx: Scheduler): U = macro internal.ReduciblesMacros.sum[T, U, Repr]
    def product[U >: T](implicit num: Numeric[U], ctx: Scheduler): U = macro internal.ReduciblesMacros.product[T, U, Repr]
    def count[U >: T](p: U => Boolean)(implicit ctx: Scheduler): Int = macro internal.ReduciblesMacros.count[T, U, Repr]
    def min[U >: T](implicit ord: Ordering[U], ctx: Scheduler): U = macro internal.ReduciblesMacros.min[T, U, Repr]
    def max[U >: T](implicit ord: Ordering[U], ctx: Scheduler): U = macro internal.ReduciblesMacros.max[T, U, Repr]
    def find[U >: T](p: U => Boolean)(implicit ctx: Scheduler): Option[T] = macro internal.ReduciblesMacros.find[T, U, Repr]
    def exists[U >: T](p: U => Boolean)(implicit ctx: Scheduler): Boolean = macro internal.ReduciblesMacros.exists[T, U, Repr]
    def forall[U >: T](p: U => Boolean)(implicit ctx: Scheduler): Boolean = macro internal.ReduciblesMacros.forall[T, U, Repr]
    def map[S, That](func: T => S)(implicit cmf: CanMergeFrom[Repr, S, That], ctx: Scheduler): That = macro internal.ReduciblesMacros.map[T, S, That, Repr]
    def flatMap[S, That](func: T => TraversableOnce[S])(implicit cmf: CanMergeFrom[Repr, S, That], ctx: Scheduler): That = macro internal.ReduciblesMacros.flatMap[T, S, That, Repr]
    def filter[That](pred: T => Boolean)(implicit cmf: CanMergeFrom[Repr, T, That], ctx: Scheduler): That = macro internal.ReduciblesMacros.filter[T, That, Repr]
    def groupMapAggregate[K, M](gr: T => K)(mp: T => M)(aggr: (M, M) => M)(implicit kClassTag: ClassTag[K], mClassTag: ClassTag[M], ctx: Scheduler): Par[mutable.HashMap[K,M]] = macro internal.ReduciblesMacros.groupMapAggregate[T, K, M, Repr]
    def groupBy[K, That <: AnyRef](gr: T => K)(implicit kClassTag: ClassTag[K], tClassTag: ClassTag[T], ctx: Scheduler, cmf: CanMergeFrom[Repr, T, That]): Par[mutable.HashMap[K,That]] = macro internal.ReduciblesMacros.groupBy[T, K, Repr, That]
  }

  class Ops[T](val r: Reducible[T]) extends AnyVal with OpsLike[T, Reducible[T]] {
    def stealer = r.stealer
    def seq = r
  }

  import Scheduler._

  trait ReducibleKernel[@specialized T, @specialized R] extends Kernel[T, R] {
    override def workOn(tree: Ref[T, R], config: Config, worker: WorkerTask): Boolean = {
      // atomically read the current node and initialize
      val node = tree.READ
      val stealer = node.stealer
      beforeWorkOn(tree, node)
      var intermediate = node.READ_INTERMEDIATE
      val incFreq = config.incrementStepFrequency //not used?
      val ms = config.maximumStep

      var looping = true
      while (looping && notTerminated) {
        val currstep = node.READ_STEP

        val elementsToGet = stealer.nextBatch(currstep)

        if (elementsToGet > 0) {
          intermediate = combine(intermediate, apply(node, elementsToGet))
          node.WRITE_STEP(math.min(ms, currstep * 2))
        } else looping = false
      }

      completeIteration(node.stealer)

      // store into the `intermediateResult` field of the node and push result up
      completeNode(intermediate, tree, worker)
    }
  }

  abstract class CopyMapReducibleKernel[T, @specialized(Specializable.AllNumeric) S] extends ReducibleKernel[T, Unit] {
    import scala.collection.par.Scheduler.{ Ref, Node }
    def zero: Unit = ()
    def combine(a: Unit, b: Unit) = a
    def resultArray: Array[S]
  }

}

