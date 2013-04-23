package scala.collection.parallel



import scala.annotation.unchecked.uncheckedVariance



trait MergerLike[@specialized -T, +To, +Repr] {
  def +=(elem: T): Repr
  def result: To
  def clear(): Unit
  def combine(that: Repr @uncheckedVariance): Repr
}

