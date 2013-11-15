package org.scala.optimized.test.par     




import scala.reflect.ClassTag
import scala.collection.par._
import scala.collection.immutable.TreeSet
import workstealing.Scheduler
import workstealing.Scheduler.Config




trait ParImmutableTreeSetSnippets {

  def aggregateSequential(ts: TreeSet[String]) = {
    var sum = 0
    var it = ts.iterator
    while (it.hasNext) {
      sum += it.next().length
    }
    sum
  }

  def aggregateParallel(ts: TreeSet[String])(implicit s: Scheduler) = ts.toPar.aggregate(0)(_ + _) {
    _ + _.length
  }

}









