package scala.collection.parallel
package workstealing






object Ops {

  type Scheduler = WorkstealingTreeScheduler

  implicit def reducableOps[T](r: Reducable[T]) = new collection.parallel.workstealing.Reducable.Ops[T](r)

  implicit def zippableOps[T](z: Zippable[T]) = new collection.parallel.workstealing.Zippable.Ops[T](z)

  implicit def rangeOps(r: Par[collection.immutable.Range]) = new Range.Ops(r.xs)

  implicit def range2zippable(r: Par[collection.immutable.Range]): Zippable[Int] = ???

  implicit def arrayOps[T](a: Par[Array[T]]) = ???

  implicit def array2zippable[T](a: Par[Array[T]]) = ???

}

