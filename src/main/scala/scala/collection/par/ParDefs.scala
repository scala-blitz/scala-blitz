package scala.collection.par



import generic.IsZippable



trait ParDefs {

  implicit def seq2ops[Repr](seq: Repr) = new ParDefs.ops(seq)

  implicit def par2zippable[T, Repr](r: Par[Repr])(implicit isZippable: IsZippable[Repr, T]): Zippable[T] = isZippable(r)

  class assume extends scala.annotation.StaticAnnotation

}


object ParDefs {

  class ops[Repr](val seq: Repr) extends AnyVal {
    def toPar = new Par(seq)
  }

}