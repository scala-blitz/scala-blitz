package scala.collection.par



import scala.reflect.ClassTag
import scala.collection.parallel.Splitter



trait Reducible[@specialized T] {

  def iterator: Iterator[T]

  def stealer: Stealer[T]
}
