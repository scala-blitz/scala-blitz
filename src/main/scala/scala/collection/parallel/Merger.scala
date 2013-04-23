package scala.collection.parallel



import scala.annotation.unchecked.uncheckedVariance



trait Merger[@specialized -T, +To] extends MergerLike[T, To, Combiner[T, To]]


object Merger


