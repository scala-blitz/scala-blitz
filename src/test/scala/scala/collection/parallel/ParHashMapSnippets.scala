package scala.collection.parallel



import scala.reflect.ClassTag
import Par._
import workstealing.WorkstealingTreeScheduler
import workstealing.WorkstealingTreeScheduler.Config
import workstealing.Ops._
import collection.mutable.HashMap



trait ParHashMapSnippets {

  def aggregateSequential(hm: HashMap[Int, Int]) = {
    val it = hm.iterator
    var sum = 0
    while (it.hasNext) {
      val kv = it.next()
      sum += kv._2
    }
    sum
  }

  def aggregateParallel(hm: HashMap[Int, Int])(implicit s: WorkstealingTreeScheduler) = hm.toPar.aggregate(0)(_ + _)(_ + _._2)

  def countSequential(hm: HashMap[Int, Int]) = {
    val it = hm.iterator
    var cnt = 0
    while (it.hasNext) {
      val kv = it.next()
      if (kv._2 % 2 == 0) cnt += 1
    }
    cnt
  }

  def countParallel(hm: HashMap[Int, Int])(implicit s: WorkstealingTreeScheduler) = hm.toPar.count(_._2 % 2 == 0)

  def filterSequential(hm: HashMap[Int, Int]) = {
    var res = new HashMap[Int, Int]
    val it = hm.iterator
    while (it.hasNext) {
      val kv = it.next()
      if (kv._2 % 5 != 0) res.put(kv._1, kv._2)
    }
    res
  }

  def filterParallel(hm: HashMap[Int, Int])(implicit s: WorkstealingTreeScheduler) = hm.toPar.filter(_._2 % 5 != 0)

}









