package scala.collection.parallel.generic



import scala.collection.parallel.{Par, Reducable}



trait IsReducable[Repr, T] {
  def apply(r: Par[Repr]): Reducable[T]
}