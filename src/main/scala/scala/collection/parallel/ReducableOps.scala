package scala.collection.parallel






trait ReducableOps[T, Repr, Sch] extends Any {

  def reduce[U >: T](op: (U, U) => U)(implicit s: Sch): U

}


