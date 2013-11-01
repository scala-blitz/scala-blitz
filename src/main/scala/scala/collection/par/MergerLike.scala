package scala.collection.par



import scala.annotation.unchecked.uncheckedVariance



trait MergerLike[@specialized -T, +To, +Repr] {
  def +=(elem: T): Repr
  def result: To
  def clear(): Unit
  def merge(that: Repr @uncheckedVariance): Repr
}

