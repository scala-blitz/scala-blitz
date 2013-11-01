package scala.collection.par



import generic.{ IsZippable, IsReducable }



trait ParDefs {

  implicit def seq2ops[Repr](seq: Repr) = new ParDefs.ops(seq)

  implicit def par2zippable[T, Repr](r: Par[Repr])(implicit isZippable: IsZippable[Repr, T]): Zippable[T] = isZippable(r)
  implicit def par2reducable[T, Repr](r: Par[Repr])(implicit isReducable: IsReducable[Repr, T]): Reducable[T] = isReducable(r)

  class assume extends scala.annotation.StaticAnnotation

}


object ParDefs {

  class ops[Repr](val seq: Repr) extends AnyVal {
    def toPar = new Par(seq)
  }

}
