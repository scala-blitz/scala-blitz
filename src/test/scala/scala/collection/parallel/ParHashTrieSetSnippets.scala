package scala.collection.parallel



import scala.reflect.ClassTag
import Par._
import workstealing.WorkstealingTreeScheduler
import workstealing.WorkstealingTreeScheduler.Config
import workstealing.Ops._
import collection.immutable.HashSet



trait ParHashTrieSetSnippets {

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

  def mapReduceParallel(hm: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = hm.toPar.mapReduce(_ + 1)(_ + _)

  def mapReduceSequential(hm: HashSet[Int]) = {
    val it = hm.iterator
    var sum = 0
    while (it.hasNext) {
      val k = it.next() + 1
      sum += k
    }
    sum
  }

  def aggregateParallelUnion(hm: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = hm.toPar.aggregate(new HashSet[Int])(_ ++ _)(_ + _)

  def mapSequential(hm: HashSet[Int]) = {
    val it = hm.iterator
    var res = HashSet.empty[Int]
    while (it.hasNext) {
      val k = it.next()
      res += k * 2
    }
    res
  }

  def mapParallel(hm: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = hm.toPar.map(_ * 2)

}









