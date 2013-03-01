package scala.collection.workstealing



import annotation.unchecked.uncheckedVariance



trait CombinerLike[@specialized -T, +To, +Repr] {
  def +=(elem: T): Repr
  def result: To
  def clear(): Unit
  def combine(that: Repr @uncheckedVariance): Repr
}

