package scala.collection.parallel
package workstealing






object Reducable {

  trait OpsLike[+T, +Repr] extends Any with ReducableOps[T, Repr, WorkstealingTreeScheduler] {
    def reduce[U >: T](op: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler): U = ???
  }

  class Ops[T](val r: Reducable[T]) extends AnyVal with OpsLike[T, Reducable[T]]

}


