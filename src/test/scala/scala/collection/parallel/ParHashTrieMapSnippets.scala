package scala.collection.parallel



import scala.reflect.ClassTag
import Par._
import workstealing.WorkstealingTreeScheduler
import workstealing.WorkstealingTreeScheduler.Config
import workstealing.Ops._
import collection.immutable.HashMap



trait ParHashTrieMapSnippets {

  def aggregateSequential(hm: HashMap[Int, Int]) = {
    val it = hm.iterator
    var sum = 0
    while (it.hasNext) {
      val k = it.next()._1
      sum += k
    }
    sum
  }

  def aggregateParallel(hm: HashMap[Int, Int])(implicit s: WorkstealingTreeScheduler) = hm.toPar.aggregate(0)(_ + _)(_ + _._1)

  def mapSequential(hm: HashMap[Int, Int]) = {
    val it = hm.iterator
    var res = HashMap.empty[Int, Int]
    while (it.hasNext) {
      val kv = it.next()
      res += ((kv._1 * 2, kv._2))
    }
    res
  }

  def mapParallel(hm: HashMap[Int, Int])(implicit s: WorkstealingTreeScheduler) = hm.toPar.map(kv => (kv._1 * 2, kv._2))

}









