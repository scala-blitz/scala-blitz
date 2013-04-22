package scala.collection.parallel






trait Reducable[@specialized T] {

  def iterator: Iterator[T]

  def splitter: Splitter[T]

  def stealer: Stealer[T]

}