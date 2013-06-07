package scala.collection.parallel






trait ZippableOps[+T, +Repr, Ctx] extends Any with ReducableOps[T, Repr, Ctx] {

  def copyToArray[U >: T](arr: Array[U], start: Int, len: Int)(implicit ctx: Ctx): Unit

}


