package scala.collection.parallel



import scala.reflect.ClassTag
import Par._
import workstealing.WorkstealingTreeScheduler
import workstealing.WorkstealingTreeScheduler.Config
import workstealing.Ops._
import collection.immutable.HashSet



trait ParHashSetSnippets {

  def aggregateSequential(hm: HashSet[Int]) = {
    val it = hm.iterator
    var sum = 0
    while (it.hasNext) {
      val k = it.next()
      sum += k
    }
    sum
  }

  def aggregateParallel(hm: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = hm.toPar.aggregate(0)(_ + _)(_ + _)

}









