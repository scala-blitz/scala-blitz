package scala.collection.parallel



import generic._



class Par[+Repr](val seq: Repr)


object Par {

  implicit class ops[Repr](val seq: Repr) {
    def toPar = new Par(seq)
  }

  implicit def par2zippable[T, Repr](r: Par[Repr])(implicit isZippable: IsZippable[Repr, T]): Zippable[T] = isZippable(r)

}


