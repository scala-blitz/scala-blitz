package scala.collection.parallel






class Par[Repr](val data: Repr) extends AnyVal


object Par {

  implicit class Ops[Repr](val xs: Repr) extends AnyVal {
    def data: Par[Repr] = new Par(xs)
  }

}