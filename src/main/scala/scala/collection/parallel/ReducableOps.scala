package scala.collection.parallel






trait ReducableOps[T, Repr, Scheduler] extends Any {

  def reduce[U >: T](op: (U, U) => U)(implicit sched: Scheduler): U

}


