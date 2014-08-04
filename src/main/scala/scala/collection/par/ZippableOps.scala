package scala.collection.par






trait ZippableOps[+T, +Repr, Ctx] extends Any with ReducibleOps[T, Repr, Ctx] {

  def copyToArray[U >: T](arr: Array[U], start: Int, len: Int)(implicit ctx: Ctx): Unit

}


