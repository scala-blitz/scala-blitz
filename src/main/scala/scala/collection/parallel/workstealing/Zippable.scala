package scala.collection.parallel
package workstealing






object Zippable {

  trait OpsLike[+T, +Repr] extends Any with ZippableOps[T, Repr, WorkstealingTreeScheduler] with Reducable.OpsLike[T, Repr] {
    def copyToArray[U >: T](arr: Array[U], start: Int, len: Int)(implicit ctx: WorkstealingTreeScheduler): Unit = ???
  }

  class Ops[T](val z: Zippable[T]) extends AnyVal with OpsLike[T, Zippable[T]]

}


