package scala.collection.parallel



import scala.reflect.ClassTag
import Par._
import workstealing.WorkstealingTreeScheduler
import workstealing.WorkstealingTreeScheduler.Config
import workstealing.Ops._
import collection.mutable.HashSet



trait ParHashSetSnippets {

  def aggregateSequential(hs: HashSet[Int]) = {
    var sum = 0
    val it = hs.iterator
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

  
  def reduceSequential(hs: HashSet[Int]) = aggregateSequential(hs)

  def reduceParallel(hs: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = hs.toPar.reduce(_ + _)

  def mapReduceSequential(hs: HashSet[Int]) = {
    var sum = 0
    val it = hs.iterator
    while (it.hasNext) {
      sum += it.next + 1
    }
    sum
  }

  def mapReduceParallel(hs: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = hs.toPar.mapReduce(_ + 1)(_ + _)

  def sumSequential(hs: HashSet[Int]) = reduceSequential(hs)

  def sumParallel(a: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.sum

  def sumParallel(a: HashSet[Int], customNum: Numeric[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.sum(customNum, s)

  def productSequential(hs: HashSet[Int]) ={
    var sum = 1
    val it = hs.iterator
    while (it.hasNext) {
      val k = it.next()
      sum *= k
    }
    sum
  }

  def productParallel(a: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.product

  def productParallel(a: HashSet[Int], customNum: Numeric[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.product(customNum, s)

  def countSquareMod3Sequential(hs: HashSet[Int]) = {
    var count = 0
    val it = hs.iterator
    while (it.hasNext) {
      val el = it.next
      if ((el*el) % 3 == 1) { count += 1 }
    }
    count
  }

  def countSquareMod3Parallel(a: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.count(x => (x * x) % 3 == 0)

  def countParallel(a: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.count(_ % 2 == 0)

  def minParallel(a: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.min

  def minParallel(a: HashSet[Int], ord: Ordering[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.min(ord, s)

  def maxParallel(a: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.max

  def maxParallel(a: HashSet[Int], ord: Ordering[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.max(ord, s)

  def findSinSequential(hs: HashSet[Int]) = {
    var found = false
    var result = -1
    val it = hs.iterator
    while (it.hasNext) {
      val el = it.next
      if (2.0 == math.sin(el)) {
        found = true
        result = el
      }
    }
    result
  }

  def findSinParallel(a: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.find(x => math.sin(x) == 2.0)

  def findParallel(a: HashSet[Int], elem: Int)(implicit s: WorkstealingTreeScheduler) = a.toPar.find(x => x == elem)

  def existsParallel(a: HashSet[Int], elem: Int)(implicit s: WorkstealingTreeScheduler) = a.toPar.exists(_ == elem)

  def forallSmallerParallel(a: HashSet[Int], elem: Int)(implicit s: WorkstealingTreeScheduler) = a.toPar.forall(_ < elem)


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

  def filterMod3Sequential(hs: HashSet[Int]) = {
    val ib = new SimpleBuffer[Int]
    val it = hs.iterator
    while (it.hasNext) {
      val elem = it.next
      if (elem % 3 == 0) ib.pushback(elem)
    }
    ib.narr
  }

  def filterMod3Parallel(a: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.filter(_ % 3 == 0)

  def filterCosSequential(hs: HashSet[Int]) = {
    val ib = new SimpleBuffer[Int]
    val it = hs.iterator
    while (it.hasNext) {
      val elem = it.next
      if (math.cos(elem) > 0.0) ib.pushback(elem)
    }
    ib.narr
  }

  def filterCosParallel(a: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.filter(x => math.cos(x) > 0.0)

  val other = List(2, 3)

  def flatMapSequential(hs: HashSet[Int]) = {
    val ib = new SimpleBuffer[Int]
    val it = hs.iterator
    while (it.hasNext) {
      val elem = it.next
      other.foreach(y => ib.pushback(elem * y))
    }
    ib.narr
  }

  def flatMapParallel(a: HashSet[Int])(implicit s: WorkstealingTreeScheduler) = {
    val pa = a.toPar
    for {
      x <- pa
      y <- other
    } yield {
      x * y
    }: @unchecked
  }

}









