package scala.collection.par



import generic.IsZippable



trait ParDefs {

  class ops[Repr](val seq: Repr) {
    def toPar = new Par(seq)
  }

  implicit def seq2ops[Repr](seq: Repr) = new ops(seq)

  implicit def par2zippable[T, Repr](r: Par[Repr])(implicit isZippable: IsZippable[Repr, T]): Zippable[T] = isZippable(r)

  class assume extends scala.annotation.StaticAnnotation

}


