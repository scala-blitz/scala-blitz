package scala.collection.parallel
package workstealing



import scala.collection.parallel.generic._



object Zippables {

  trait Scope extends Reducables.Scope {
    implicit def zippableOps[T](z: Zippable[T]) = new collection.parallel.workstealing.Zippables.Ops[T](z)

    implicit def canMergeZippable[T]: CanMergeFrom[Zippable[_], Int, Zippable[T]] = ???
  }

  trait OpsLike[+T, +Repr] extends Any /*with ZippableOps[T, Repr, WorkstealingTreeScheduler]*/ with Reducables.OpsLike[T, Repr] {
    def copyToArray[U >: T](arr: Array[U], start: Int, len: Int)(implicit ctx: WorkstealingTreeScheduler): Unit = ???
  }

  class Ops[T](val z: Zippable[T]) extends AnyVal with OpsLike[T, Zippable[T]]

}


