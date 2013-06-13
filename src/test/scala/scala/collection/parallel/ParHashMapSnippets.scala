package scala.collection.parallel



import scala.reflect.ClassTag
import Par._
import workstealing.WorkstealingTreeScheduler
import workstealing.WorkstealingTreeScheduler.Config
import workstealing.Ops._



trait ParHashMapSnippets {

  def aggregateSequential(hm: collection.mutable.HashMap[Int, Int]) = {
    val it = hm.iterator
    var sum = 0
    while (it.hasNext) {
      val kv = it.next()
      sum += kv._2
    }
    sum
  }

  def aggregateParallel(hm: collection.mutable.HashMap[Int, Int])(implicit s: WorkstealingTreeScheduler) = hm.toPar.aggregate(0)(_ + _)(_ + _._2)

  def countSequential(hm: collection.mutable.HashMap[Int, Int]) = {
    val it = hm.iterator
    var cnt = 0
    while (it.hasNext) {
      val kv = it.next()
      if (kv._2 % 2 == 0) 
      cnt += 1
    }
    cnt
  }

  def countParallel(hm: collection.mutable.HashMap[Int, Int])(implicit s: WorkstealingTreeScheduler) = hm.toPar.count(_._2 % 2 == 0)

  def filterParallel(hm: collection.mutable.HashMap[Int, Int])(implicit s: WorkstealingTreeScheduler) = hm.toPar.filter(_._2 % 5 != 0)

}









