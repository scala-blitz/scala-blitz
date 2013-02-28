package scala.collection.workstealing



import scala.collection.mutable.Builder



trait Combiner[@specialized -T, +To, Repr] {

  def +=(elem: T): Combiner[T, To, Repr]

  def result: To

  def clear(): Unit

  def combine(that: Repr): Repr

}


