package scala.collection.par
package workstealing



import scala.collection.par.generic._



object Zippables {

  trait Scope extends Reducables.Scope {
    implicit def zippableOps[T](z: Zippable[T]) = new collection.par.workstealing.Zippables.Ops[T](z)
    implicit def canMergeZippable[T]: CanMergeFrom[Zippable[_], Int, Zippable[T]] = ???
  }

  trait OpsLike[+T, +Repr] extends Any /*with ZippableOps[T, Repr, Scheduler]*/ with Reducables.OpsLike[T, Repr] {
    def stealer: PreciseStealer[T]
    def copyToArray[U >: T](arr: Array[U], start: Int, len: Int)(implicit ctx: Scheduler): Unit = ???
    def zip[S, R](arr: Zippable[S])(f: (T, S) => R): Zippable[R] = ???
  }

  class Ops[T](val z: Zippable[T]) extends AnyVal with OpsLike[T, Zippable[T]] {
    def stealer = z.stealer
    def seq = z
  }

}


