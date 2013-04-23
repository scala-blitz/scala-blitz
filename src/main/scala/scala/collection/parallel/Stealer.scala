package scala.collection.parallel






trait Stealer[@specialized T] {

  def next(): T

  def hasNext: Boolean

  def state: Stealer.State

  def advance(step: Int): Int

  def markCompleted(): Boolean

  def markStolen(): Boolean

  def split: (Stealer[T], Stealer[T])

  def elementsRemainingEstimate: Int

  def minimumStealThreshold: Int = 1

}


object Stealer {

  trait State

  val Completed = new State {
    override def toString = "Completed"
  }

  val StolenOrExpanded = new State {
    override def toString = "StolenOrExpanded"
  }

  val AvailableOrOwned = new State {
    override def toString = "AvailableOrOwned"
  }

}