package scala.collection.parallel



import scala.reflect.ClassTag
import Par._
import workstealing.WorkstealingTreeScheduler
import workstealing.WorkstealingTreeScheduler.Config
import workstealing.Ops._
import collection.mutable.HashSet



trait ParHashSetSnippets {

  def aggregateSequential(hs: HashSet[Int]) = {
    val it = hs.iterator
    var sum = 0
    while (it.hasNext) {
      val k = it.next()
      sum += k
    }
    sum
  }

  def aggregateParallel(hs: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = hs.toPar.aggregate(0)(_ + _)(_ + _)

  def mapSequential(hm: HashSet[Int]) = {
    val it = hm.iterator
    val res = HashSet.empty[Int]
    while (it.hasNext) {
      val k = it.next()
      res += k * 2
    }
    res
  }

  def mapParallel(hm: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = hm.toPar.map(_ * 2)
}









