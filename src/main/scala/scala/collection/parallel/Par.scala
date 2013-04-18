package scala.collection.parallel






class Par[Repr](val data: Repr) extends AnyVal


object Par {

  implicit class Ops[Repr](val xs: Repr) extends AnyVal {
    def toPar: Par[Repr] = new Par(xs)
  }

}