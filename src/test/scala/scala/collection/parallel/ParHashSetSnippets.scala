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
  def aggregateReducable(hs: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = hashSetIsReducable(hs.toPar).aggregate(0)(_ + _)(_ + _)

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

 
  def reduceSequential(a: HashSet[Int]) = aggregateSequential(a)

  def reduceParallel(a: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.reduce(_ + _)

  def reduceReducable(a: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = hashSetIsReducable(a.toPar).reduce(_ + _)

  def mapReduceParallel(hm: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = hm.toPar.mapReduce(_ + 1)(_ + _)

  def mapReduceReducable(hm: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = hashSetIsReducable(hm.toPar).mapReduce(_ + 1)(_ + _)

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

 
  def foldProductSequential(a: HashSet[Int]) = {
    val it = a.iterator
    var sum = 1
    while (it.hasNext) {
      sum *= it.next
    }
    sum
  }

  def foldProductParallel(a: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.fold(1)(_ * _)

  def foldParallel(a: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.fold(0)(_ + _)

  def sumSequential(a: HashSet[Int]) = {
    val it = a.iterator
    var sum = 0
    while (it.hasNext) {
      sum += it.next
    }
    if (sum == 0) ???
    sum
  }

  def sumParallel(a: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.sum

  def sumParallel(a: HashSet[Int], customNum: Numeric[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.sum(customNum, s)

  def productSequential(a: HashSet[Int]) = foldProductSequential(a)

  def productParallel(a: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.product

  def productParallel(a: HashSet[Int], customNum: Numeric[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.product(customNum, s)

  def countSquareMod3Sequential(a: HashSet[Int]) = {
    val it = a.iterator
    var count = 0
    while (it.hasNext) {
      val el = it.next
      if ((el * el) % 3 == 1) { count += 1 }
    }
    count
  }

  def countSquareMod3Parallel(a: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.count(x => (x * x) % 3 == 0)

  def countParallel(a: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.count(_ % 2 == 0)

  def minParallel(a: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.min

  def minParallel(a: HashSet[Int], ord: Ordering[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.min(ord, s)

  def maxParallel(a: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.max

  def maxParallel(a: HashSet[Int], ord: Ordering[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.max(ord, s)

  def foreachSequential(a: HashSet[Int]) = {
    val ai = new java.util.concurrent.atomic.AtomicLong(0)
    a.foreach(x=>  if (x % 500 == 0) ai.incrementAndGet())
    ai.get
  }

  def foreachParallel(a: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = {
    val ai = new java.util.concurrent.atomic.AtomicLong(0)
    a.toPar.foreach(x=> if (x % 500 == 0) ai.incrementAndGet())
    ai.get
  }

  def foreachReducable(a: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = {
    val ai = new java.util.concurrent.atomic.AtomicLong(0)
    hashSetIsReducable(a.toPar).foreach(x=> if (x % 500 == 0) ai.incrementAndGet())
    ai.get
  }

  def findParallel(a: HashSet[Int], elem: Int)(implicit s: WorkstealingTreeScheduler) = a.toPar.find(x => x == elem)

  def findReducable(a: HashSet[Int], elem: Int)(implicit s: WorkstealingTreeScheduler) = hashSetIsReducable(a.toPar).find(x => x == elem)
 
  def existsParallel(a: HashSet[Int], elem: Int)(implicit s: WorkstealingTreeScheduler) = a.toPar.exists(x => x == elem)

  def forallSmallerParallel(a: HashSet[Int], elem: Int)(implicit s: WorkstealingTreeScheduler) = a.toPar.forall(x => x < elem)
 
}









