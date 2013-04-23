package scala.collection.parallel
package workstealing



import scala.collection.parallel.generic._



object Reducable {

  trait OpsLike[+T, +Repr] extends Any with ReducableOps[T, Repr, WorkstealingTreeScheduler] {
    def reduce[U >: T](op: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler): U = ???
    def map[S, That](f: T => S)(implicit cbf: CanMergeFrom[Repr, S, That], ctx: WorkstealingTreeScheduler) = ???
  }

  class Ops[T](val r: Reducable[T]) extends AnyVal with OpsLike[T, Reducable[T]]

  trait Scope {
    implicit def reducableOps[T](r: Reducable[T]) = new collection.parallel.workstealing.Reducable.Ops[T](r)

    implicit def canMergeReducable[T]: CanMergeFrom[Reducable[_], Int, Reducable[T]] = ???
  }

}


