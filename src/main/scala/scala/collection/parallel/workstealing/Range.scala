package scala.collection.parallel
package workstealing






object Range {

  class Ops(val r: collection.immutable.Range) extends AnyVal with Zippable.OpsLike[Int, collection.immutable.Range] {
    override def reduce[U >: Int](op: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler): U = ???
  } 

}