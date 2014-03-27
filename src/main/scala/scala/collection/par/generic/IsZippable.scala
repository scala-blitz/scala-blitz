package scala.collection.par.generic



import scala.collection.par.{Par, Zippable}



trait IsZippable[Repr, T] extends IsReducable[Repr, T]{
  def apply(r: Par[Repr]): Zippable[T]
}
