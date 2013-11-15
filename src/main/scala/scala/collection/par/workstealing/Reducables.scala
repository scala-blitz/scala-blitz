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



object Reducables {

  trait Scope {
    implicit def reducableOps[T](r: Reducable[T]) = new collection.par.workstealing.Reducables.Ops[T](r)
    implicit def canMergeReducableInt(implicit ctx: Scheduler): CanMergeFrom[Reducable[_], Int, Par[Array[Int]]] = new CanMergeFrom[Reducable[_], Int, Par[Array[Int]]] {
      def apply(from: Reducable[_]) = new Arrays.ArrayMerger[Int](ctx)
      def apply() = new Arrays.ArrayMerger[Int](ctx)
    }

    implicit def canMergeReducable[@specialized(Int, Long, Float, Double) T: ClassTag](implicit ctx: Scheduler): CanMergeFrom[Reducable[_], T, Par[Array[T]]] = new CanMergeFrom[Reducable[_], T, Par[Array[T]]] {
      def apply(from: Reducable[_]) = new Arrays.ArrayMerger[T](ctx)
      def apply() = new Arrays.ArrayMerger[T](ctx)
    }

  }

  trait OpsLike[+T, +Repr] extends Any /*with ReducableOps[T, Repr, Scheduler]*/ {
    def stealer: Stealer[T]
    def seq: Repr
    def reduce[U >: T](op: (U, U) => U)(implicit ctx: Scheduler): U = macro internal.ReducablesMacros.reduce[T, U, Repr]
    def mapReduce[R](mapper: T => R)(reducer: (R, R) => R)(implicit ctx: Scheduler): R = macro internal.ReducablesMacros.mapReduce[T, T, R, Repr]
    def fold[U >: T](z: => U)(op: (U, U) => U)(implicit ctx: Scheduler): U = macro internal.ReducablesMacros.fold[T, U, Repr]
    def aggregate[S](z: S)(combop: (S, S) => S)(seqop: (S, T) => S)(implicit ctx: Scheduler) = macro internal.ReducablesMacros.aggregate[T, S, Repr]
    def foreach[U >: T](action: U => Unit)(implicit ctx: Scheduler): Unit = macro internal.ReducablesMacros.foreach[T, U, Repr]
    def sum[U >: T](implicit num: Numeric[U], ctx: Scheduler): U = macro internal.ReducablesMacros.sum[T, U, Repr]
    def product[U >: T](implicit num: Numeric[U], ctx: Scheduler): U = macro internal.ReducablesMacros.product[T, U, Repr]
    def count[U >: T](p: U => Boolean)(implicit ctx: Scheduler): Int = macro internal.ReducablesMacros.count[T, U, Repr]
    def min[U >: T](implicit ord: Ordering[U], ctx: Scheduler): U = macro internal.ReducablesMacros.min[T, U, Repr]
    def max[U >: T](implicit ord: Ordering[U], ctx: Scheduler): U = macro internal.ReducablesMacros.max[T, U, Repr]
    def find[U >: T](p: U => Boolean)(implicit ctx: Scheduler): Option[T] = macro internal.ReducablesMacros.find[T, U, Repr]
    def exists[U >: T](p: U => Boolean)(implicit ctx: Scheduler): Boolean = macro internal.ReducablesMacros.exists[T, U, Repr]
    def forall[U >: T](p: U => Boolean)(implicit ctx: Scheduler): Boolean = macro internal.ReducablesMacros.forall[T, U, Repr]
    def map[S, That](func: T => S)(implicit cmf: CanMergeFrom[Repr, S, That], ctx: Scheduler): That = macro internal.ReducablesMacros.map[T, S, That, Repr]
    def flatMap[S, That](func: T => TraversableOnce[S])(implicit cmf: CanMergeFrom[Repr, S, That], ctx: Scheduler) = macro internal.ReducablesMacros.flatMap[T, S, That, Repr]
    def filter[That](pred: T => Boolean)(implicit cmf: CanMergeFrom[Repr, T, That], ctx: Scheduler) = macro internal.ReducablesMacros.filter[T, That, Repr]
    def groupMapAggregate[K, M](gr:T => K)(mp:T => M)(aggr:(M,M) => M)(implicit kClassTag:ClassTag[K], mClassTag:ClassTag[M],  ctx: Scheduler) = macro internal.ReducablesMacros.groupMapAggregate[T, K, M, Repr]
    def groupBy[K, That <: AnyRef](gr:T => K)(implicit kClassTag:ClassTag[K], tClassTag:ClassTag[T],  ctx: Scheduler, cmf: CanMergeFrom[Repr, T, That]) = macro internal.ReducablesMacros.groupBy[T, K, Repr,That]
  }

  class Ops[T](val r: Reducable[T]) extends AnyVal with OpsLike[T, Reducable[T]] {
    def stealer = r.stealer
    def seq = r
  }

  import Scheduler._

  trait ReducableKernel[@specialized T, @specialized R] extends Kernel[T, R] {
    override def workOn(tree: Ref[T, R], config: Config, worker: Worker): Boolean = {
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

        val elementsToGet = stealer.advance(currstep)

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

  abstract class CopyMapReducableKernel[T, @specialized(Specializable.AllNumeric) S] extends ReducableKernel[T, Unit] {
    import scala.collection.par.workstealing.Scheduler.{ Ref, Node }
    def zero: Unit = ()
    def combine(a: Unit, b: Unit) = a
    def resultArray: Array[S]
  }

}

