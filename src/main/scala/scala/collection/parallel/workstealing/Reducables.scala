package scala.collection.parallel
package workstealing

import scala.language.experimental.macros
import scala.reflect.macros._
import scala.collection.parallel.generic._
import scala.collection.parallel.workstealing._
import scala.collection.parallel.generic._
import scala.collection.parallel.Par
import scala.collection.parallel.workstealing._

object Reducables {

  trait Scope {
    implicit def reducableOps[T](r: Reducable[T]) = new collection.parallel.workstealing.Reducables.Ops[T](r)
    implicit def canMergeReducable[T]: CanMergeFrom[Reducable[_], Int, Reducable[T]] = ???
  }

  trait OpsLike[+T, +Repr] extends Any /*with ReducableOps[T, Repr, WorkstealingTreeScheduler]*/ {
    def reduce[U >: T](op: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler): U = macro methods.ReducablesMacros.reduce[T, U, Repr]
    def mapReduce[R](mapper: T => R)(reducer: (R, R) => R)(implicit ctx: WorkstealingTreeScheduler): R = macro methods.ReducablesMacros.mapReduce[T, T, R, Repr]
    def fold[U >: T](z: => U)(op: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler): U = macro methods.ReducablesMacros.fold[T, U, Repr]
    def aggregate[S](z: S)(combop: (S, S) => S)(seqop: (S, T) => S)(implicit ctx: WorkstealingTreeScheduler) = macro methods.ReducablesMacros.aggregate[T, S, Repr]
    def foreach[U >: T](action: U => Unit)(implicit ctx: WorkstealingTreeScheduler): Unit = macro methods.ReducablesMacros.foreach[T, U, Repr]
    def sum[U >: T](implicit num: Numeric[U], ctx: WorkstealingTreeScheduler): U = macro methods.ReducablesMacros.sum[T, U, Repr]
    def product[U >: T](implicit num: Numeric[U], ctx: WorkstealingTreeScheduler): U = macro methods.ReducablesMacros.product[T, U, Repr]
    def count[U >: T](p: U => Boolean)(implicit ctx: WorkstealingTreeScheduler): Int = macro methods.ReducablesMacros.count[T, U, Repr]
    //def min[U >: T](implicit ord: Ordering[U], ctx: WorkstealingTreeScheduler): U = macro methods.ReducablesMacros.min[T, U, Repr]
    //def max[U >: T](implicit ord: Ordering[U], ctx: WorkstealingTreeScheduler): U = macro methods.ReducablesMacros.max[T, U, Repr]

    def map[S, That](f: T => S)(implicit cmf: CanMergeFrom[Repr, S, That], ctx: WorkstealingTreeScheduler): That = ???
    def stealer: Stealer[T]
  }

  class Ops[T](val r: Reducable[T]) extends AnyVal with OpsLike[T, Par[Reducable[T]]] {
    def stealer = r.stealer
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

}

