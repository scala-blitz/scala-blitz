package scala.collection.parallel
package workstealing






object Range {

  class Ops[T](val r: collection.immutable.Range) extends AnyVal with Zippable.OpsLike[Int, collection.immutable.Range]

}