package scala.collection.parallel.generic



import scala.collection.parallel.{Par, Zippable}



trait IsZippable[Repr, T] {
  def apply(r: Par[Repr]): Zippable[T]
}