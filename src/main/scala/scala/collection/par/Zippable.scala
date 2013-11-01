package scala.collection.par



import scala.collection.parallel.PreciseSplitter



trait Zippable[@specialized T] extends Reducable[T] {

  def iterator: Iterator[T]

  def splitter: PreciseSplitter[T]

  def stealer: PreciseStealer[T]

}
