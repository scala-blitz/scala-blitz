package scala.collection.parallel
import scala.reflect.ClassTag





trait Reducable[@specialized T] {

  def iterator: Iterator[T]

  def splitter: Splitter[T]

  def stealer: Stealer[T]

}
