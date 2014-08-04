package scala.collection.par



import scala.collection.parallel.PreciseSplitter



trait Zippable[@specialized T] extends Reducible[T] {

  def iterator: Iterator[T]

  def stealer: PreciseStealer[T]

}
