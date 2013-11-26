package scala.collection.par



import scala.reflect.ClassTag
import scala.collection.parallel.Splitter



trait Reducable[@specialized T] {

  def iterator: Iterator[T]

  def stealer: Stealer[T]
}
