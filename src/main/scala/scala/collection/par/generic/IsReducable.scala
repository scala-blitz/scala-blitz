package scala.collection.par.generic



import scala.collection.par.{Par, Reducible}



trait IsReducible[Repr, T] {
  def apply(r: Par[Repr]): Reducible[T]
}