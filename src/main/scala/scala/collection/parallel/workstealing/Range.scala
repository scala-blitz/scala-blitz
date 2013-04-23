package scala.collection.parallel
package workstealing



import scala.collection.parallel.generic._



object Range {

  class Ops(val r: collection.immutable.Range) extends AnyVal with Zippable.OpsLike[Int, Par[collection.immutable.Range]] {
    override def reduce[U >: Int](op: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler): U = ???
  } 

  trait Scope {
    implicit def rangeOps(r: Par[collection.immutable.Range]) = new Range.Ops(r.xs)

    implicit def range2zippable(r: Par[collection.immutable.Range]): Zippable[Int] = ???

    implicit def canMergeRange[T]: CanMergeFrom[Par[Range], Int, Par[Range]] = ???
  }

}