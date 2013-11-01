package scala.collection.par.generic



import scala.collection.par.{Par, Reducable}



trait IsReducable[Repr, T] {
  def apply(r: Par[Repr]): Reducable[T]
}