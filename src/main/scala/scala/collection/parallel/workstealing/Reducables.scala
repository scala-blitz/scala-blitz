package scala.collection.parallel
package workstealing

import scala.language.experimental.macros
import scala.reflect.macros._
import scala.collection.parallel.generic._
import scala.collection.parallel.workstealing._
import scala.collection.parallel.generic._
import scala.collection.parallel.Par
import scala.collection.parallel.workstealing._
import scala.reflect.ClassTag

object Reducables {

  trait Scope {
    implicit def reducableOps[T](r: Reducable[T]) = new collection.parallel.workstealing.Reducables.Ops[T](r)
    implicit def canMergeReducable[T:ClassTag](implicit ctx: WorkstealingTreeScheduler): CanMergeFrom[Reducable[_], T, Par[Array[T]]] = new CanMergeFrom[Reducable[_], T, Par[Array[T]]] {
      def apply(from: Reducable[_]) = new Arrays.ArrayMerger[T](ctx)
      def apply() = new Arrays.ArrayMerger[T](ctx)
    }

  }

  trait OpsLike[+T , +Repr] extends Any /*with ReducableOps[T, Repr, WorkstealingTreeScheduler]*/ {
    def stealer: Stealer[T]
    def seq: Repr

    def reduce[U >: T](op: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler): U = macro methods.ReducablesMacros.reduce[T, U, Repr]
    def mapReduce[R](mapper: T => R)(reducer: (R, R) => R)(implicit ctx: WorkstealingTreeScheduler): R = macro methods.ReducablesMacros.mapReduce[T, T, R, Repr]
    def fold[U >: T](z: => U)(op: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler): U = macro methods.ReducablesMacros.fold[T, U, Repr]
    def aggregate[S](z: S)(combop: (S, S) => S)(seqop: (S, T) => S)(implicit ctx: WorkstealingTreeScheduler) = macro methods.ReducablesMacros.aggregate[T, S, Repr]
    def foreach[U >: T](action: U => Unit)(implicit ctx: WorkstealingTreeScheduler): Unit = macro methods.ReducablesMacros.foreach[T, U, Repr]
    def sum[U >: T](implicit num: Numeric[U], ctx: WorkstealingTreeScheduler): U = macro methods.ReducablesMacros.sum[T, U, Repr]
    def product[U >: T](implicit num: Numeric[U], ctx: WorkstealingTreeScheduler): U = macro methods.ReducablesMacros.product[T, U, Repr]
    def count[U >: T](p: U => Boolean)(implicit ctx: WorkstealingTreeScheduler): Int = macro methods.ReducablesMacros.count[T, U, Repr]
    def min[U >: T](implicit ord: Ordering[U], ctx: WorkstealingTreeScheduler): U = macro methods.ReducablesMacros.min[T, U, Repr]
    def max[U >: T](implicit ord: Ordering[U], ctx: WorkstealingTreeScheduler): U = macro methods.ReducablesMacros.max[T, U, Repr]
    def find[U >: T](p: U => Boolean)(implicit ctx: WorkstealingTreeScheduler): Option[T] = macro methods.ReducablesMacros.find[T, U, Repr]
    def exists[U >: T](p: U => Boolean)(implicit ctx: WorkstealingTreeScheduler): Boolean = macro methods.ReducablesMacros.exists[T, U, Repr]
    def forall[U >: T](p: U => Boolean)(implicit ctx: WorkstealingTreeScheduler): Boolean = macro methods.ReducablesMacros.forall[T, U, Repr]
    def map[S, That](func: T => S)(implicit cmf: CanMergeFrom[Repr, S, That], ctx: WorkstealingTreeScheduler): That = macro methods.ReducablesMacros.map[T,S,That,Repr]
    def flatMap[S, That](func: T => TraversableOnce[S])(implicit cmf: CanMergeFrom[Repr, S, That], ctx: WorkstealingTreeScheduler) = macro methods.ReducablesMacros.flatMap[T, S, That, Repr]
    def filter(pred: T => Boolean)(implicit ctx: WorkstealingTreeScheduler, ev:ClassTag[T]) = macro methods.ReducablesMacros.filter[T,Repr]
  }

  class Ops[T](val r: Reducable[T]) extends AnyVal with OpsLike[T, Reducable[T]] {
    def stealer = r.stealer
    def seq = r
  }


  import WorkstealingTreeScheduler._

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
    import scala.collection.parallel.workstealing.WorkstealingTreeScheduler.{ Ref, Node }
    def zero: Unit = ()
    def combine(a: Unit, b: Unit) = a
    def resultArray: Array[S]
  }

}

