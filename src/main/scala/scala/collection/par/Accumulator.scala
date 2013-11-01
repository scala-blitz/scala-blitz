package scala.collection.par






trait Accumulator[@specialized -T, +To <: Merger[T, To]] extends Merger[T, To] with MergerLike[T, To, To] {
  def result = this.asInstanceOf[To]
}
