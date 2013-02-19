package scala.collection.parallel






trait Workstealing[N <: WorkstealingScheduler.Node[N, T, R], T, R] {

  def newRoot: WorkstealingScheduler.Ptr[N, T, R]

}


