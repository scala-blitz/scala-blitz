package scala.collection.par



import scala.collection.parallel.PreciseSplitter



trait Zippable[@specialized T] extends Reducable[T] {

  def iterator: Iterator[T]

  def stealer: PreciseStealer[T]

}
