package scala.collection.parallel
package workstealing



import scala.collection.parallel.generic._



object Zippable {

  trait OpsLike[+T, +Repr] extends Any with ZippableOps[T, Repr, WorkstealingTreeScheduler] with Reducable.OpsLike[T, Repr] {
    def copyToArray[U >: T](arr: Array[U], start: Int, len: Int)(implicit ctx: WorkstealingTreeScheduler): Unit = ???
  }

  class Ops[T](val z: Zippable[T]) extends AnyVal with OpsLike[T, Zippable[T]]

  trait Scope extends Reducable.Scope {
    implicit def zippableOps[T](z: Zippable[T]) = new collection.parallel.workstealing.Zippable.Ops[T](z)

    implicit def canMergeZippable[T]: CanMergeFrom[Zippable[_], Int, Zippable[T]] = ???
  }

}


