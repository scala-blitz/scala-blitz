package scala.collection.parallel
package generic



import scala.annotation.implicitNotFound
import scala.reflect.ClassTag



@implicitNotFound(msg = "Cannot construct a parallel collection of type ${To} with elements of type ${Elem} based on a collection of type ${From}.")
trait CanMergeFrom[-From, @specialized(Int, Long, Float, Double) -Elem, +To] {
  def apply(from: From): Merger[Elem, To]
  def apply(): Merger[Elem, To]
}

