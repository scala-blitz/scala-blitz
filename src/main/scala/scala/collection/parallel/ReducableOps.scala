package scala.collection.parallel






trait ReducableOps[T, Repr, Ctx] extends Any {

  def reduce[U >: T](op: (U, U) => U)(implicit ctx: Ctx): U

}


