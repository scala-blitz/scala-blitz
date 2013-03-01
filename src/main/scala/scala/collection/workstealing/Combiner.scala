package scala.collection.workstealing



import annotation.unchecked.uncheckedVariance



trait Combiner[@specialized -T, +To] extends CombinerLike[T, To, Combiner[T, To]]


object Combiner


