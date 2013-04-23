package scala.collection.parallel



import scala.collection.parallel.generic._



trait ReducableOps[+T, +Repr, Ctx] extends Any {

  def reduce[U >: T](op: (U, U) => U)(implicit ctx: Ctx): U

  def map[S, That](f: T => S)(implicit cbf: CanMergeFrom[Repr, S, That], ctx: Ctx)

}


