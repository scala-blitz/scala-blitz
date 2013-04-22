package scala.collection.parallel






trait PreciseStealer[@specialized T] extends Stealer[T] {

  def next(): T

  def hasNext: Boolean

  def advance(step: Int): Int

  def split: (PreciseStealer[T], PreciseStealer[T])

  def psplit(leftSize: Int, rightSize: Int): (PreciseStealer[T], PreciseStealer[T])

}


object PreciseStealer {

}