package org.scala.optimized.test.par     



import scala.reflect.ClassTag
import scala.collection.par._
import workstealing.Scheduler
import workstealing.Scheduler.Config



trait ParRangeSnippets {

  def reduceSequential(r: Range) = {
    var i = r.head
    val to = r.last
    var sum = 0
    while (i <= to) {
      sum += i
      i += 1
    }
    if (sum == 0) ???
  }

  def mapReduceSequential(r: Range) = {
    var i = r.head
    val to = r.last
    var sum = 0
    while (i <= to) {
      sum += i + 1
      i += 1
    }
    if (sum == 0) ???
  }

  def mapReduceSequentialCollections(r: Range) =     r.map(_+1).reduce(_+_)  

  def reduceParallel(r: Range)(implicit s: Scheduler) = r.toPar.reduce(_ + _)

  def mapReduceParallel(r: Range)(implicit s: Scheduler) = r.toPar.mapReduce(_ + 1)(_ + _)
  def mapReduceParallelNotFused(r: Range)(implicit s: Scheduler) = r.toPar.map(_ + 1).reduce(_ + _)

  def aggregateSequential(r: Range) = {
    var i = r.head
    val until = r.last
    var sum = 0
    while (i < until) {
      sum += i
      i += 1
    }
    sum
  }

  def aggregateParallel(r: Range)(implicit s: Scheduler) = r.toPar.aggregate(0)(_ + _)(_ + _)

  def foldSequential(r: Range) = {
    var i = r.head
    val until = r.last
    var sum = 0
    while (i < until) {
      sum += i
      i += 1
    }
    sum
  }

  def foldParallel(r: Range)(implicit s: Scheduler) = r.toPar.fold(0)(_ + _)

  def minSequential(r: Range) = {
    var i = r.head
    val to = r.last
    var min = Int.MaxValue
    while (i <= to) {
      if (i < min) min = i
      i += 1
    }
    min
  }

  def minParallel(r: Range)(implicit s: Scheduler) = r.toPar.min

  def minParallel(r: Range, ord: Ordering[Int])(implicit s: Scheduler) = r.toPar.min(ord, s)

  def maxSequential(r: Range) = {
    var i = r.head
    val to = r.last
    var max = Int.MinValue
    while (i <= to) {
      if (i > max) max = i
      i += 1
    }
    max
  }

  def maxParallel(r: Range)(implicit s: Scheduler) = r.toPar.max

  def maxParallel(r: Range, ord: Ordering[Int])(implicit s: Scheduler) = r.toPar.max(ord, s)

  def sumSequential(r: Range) = {
    var i = r.head
    val until = r.last
    var sum = 0
    while (i < until) {
      sum += i
      i += 1
    }
    if (sum == 0) ???
    sum
  }

  def sumParallel(r: Range)(implicit s: Scheduler) = r.toPar.sum

  def sumParallel(r: Range, customNum: Numeric[Int])(implicit s: Scheduler) = r.toPar.sum(customNum, s)

  def productSequential(r: Range) = {
    var i = r.head
    val until = r.last
    var sum = 1
    while (i < until) {
      sum *= i
      i += 1
    }
    if (sum == 1) ???
    sum
  }

  def productParallel(r: Range)(implicit s: Scheduler) = r.toPar.product

  def productParallel(r: Range, customNum: Numeric[Int])(implicit s: Scheduler) = r.toPar.product(customNum, s)

  def countSequential(r: Range) = {
    var i = r.head
    val until = r.last
    var count = 0
    while (i < until) {
      if ((i & 0x1) == 0) { count += 1 }
      i += 1
    }
    count
  }

  def countParallel(r: Range)(implicit s: Scheduler) = r.toPar.count(x => (x & 0x1) == 0)

  def findLastSequential(r: Range) = {
    var i = r.head
    val to = r.last
    var found = false
    var result = -1
    while (i <= to && !found) {
      if (i == to) {
        found = true
        result = i
      }
      i += 1
    }
    result
  }

  def findNotExistingSequential(r: Range) = {
    var i = r.head
    val to = r.last
    var found = false
    var result = -1
    while (i <= to && !found) {
      if (i == to) {
        found = true
        result = i
      }
      i += 1
    }
    result
  }

  def findLastParallel(r: Range)(implicit s: Scheduler) = {
    val mx = r.last
    r.toPar.find(_ == mx)
  }

  def findNotExistingParallel(r: Range)(implicit s: Scheduler) = {
    val mx = r.max + 1
    r.toPar.find(_ == mx)
  }

  def existsSequential(r: Range) = {
    var i = r.head
    val to = r.last
    var result = false
    while (i <= to && (!result)) {
      if (to == i) {
        result = true
      }
      i += 1
    }
    if (!result) ???
    else result
  }

  def existsParallel(r: Range)(implicit s: Scheduler) = {
    val mx = r.last + 1
    r.toPar.exists(_ == mx)
  }

  def forallSequential(r: Range) = {
    var i = r.head
    val to = r.last
    var result = true
    while (i <= to && result) {
      result = i < Int.MaxValue
      i += 1
    }
    if (!result) ???
    else result
  }

  def forallParallel(r: Range)(implicit s: Scheduler) = {
    r.toPar.forall(_ < Int.MaxValue)
  }

  def copyAllToArraySequential(ra: (Range,Array[Int]))  = {
    val r = ra._1
    val a = ra._2
    r.copyToArray(a)
    a
  }

  def copyAllToArrayParallel(ra: (Range,Array[Int]))(implicit s: Scheduler) = {
    val a = ra._2
    val r = ra._1
    r.toPar.copyToArray(a)
    a
  }

  def copyPartToArraySequential(r: Range)  = {
    val start = r.size / 7
    val len = 5 * (r.size / 7)
    val dest = new Array[Int](len)
    r.copyToArray(dest, start, len)
    dest
  }

  def copyPartToArrayParallel(r: Range)(implicit s: Scheduler) = {
    val start = r.size / 7
    val len = 5 * (r.size / 7)
    val dest = new Array[Int](len)
    r.toPar.copyToArray(dest, start, len)
    dest
  }

  def mapParallel(r: Range)(implicit s: Scheduler) = r.toPar.map(_ + 1)
  def groupMapAggregateParallel(r: Range)(implicit s: Scheduler) = r.toPar.groupMapAggregate(x=>x%15)(x=>x)((x,y)=>x+y)
  def mapParallel[Repr](r: Range, customCmf: collection.par.generic.CanMergeFrom[Par[Range], Int, Par[Repr]])(implicit s: Scheduler) = r.toPar.map(_ + 1)(customCmf, s)

  def filterMod3Sequential(r: Range) = {
    var i = r.head
    val until = r.last + r.step
    val ib = new SimpleBuffer[Int]
    while (i != until) {
      val elem = i
      if (elem % 3 == 0) ib.pushback(elem)
      i += r.step
    }
    ib.narr
  }

  def foreachSequential(r:Range) = {
    val ai = new java.util.concurrent.atomic.AtomicLong(0)
    r.foreach(x=>  if (x % 500 == 0) ai.incrementAndGet())
    ai.get
  }

  def foreachParallel(r: Range)(implicit s: Scheduler) = {
    val ai = new java.util.concurrent.atomic.AtomicLong(0)
    r.toPar.foreach(x=> if (x % 500 == 0) ai.incrementAndGet())
    ai.get
  }

  def filterMod3Parallel(r: Range)(implicit s: Scheduler) = r.toPar.filter(_ % 3 == 0)

  def filterCosSequential(r: Range) = {
    var i = r.head
    val until = r.last + r.step
    val ib = new SimpleBuffer[Int]
    while (i != until) {
      val elem = i
      if (math.cos(elem) > 0.0) ib.pushback(elem)
      i += r.step
    }
    ib.narr
  }

  def filterCosParallel(r: Range)(implicit s: Scheduler) = r.toPar.filter(x => math.cos(x) > 0.0)

  val other = List(2, 3)

  def flatMapSequential(r: Range) = {
    var i = r.head
    val until = r.last + r.step
    val ib = new SimpleBuffer[Int]
    while (i < until) {
      val elem = i
      other.foreach(y => ib.pushback(elem * y))
      i += r.step
    }
    ib.narr
  }

  def flatMapParallel(r: Range)(implicit s: Scheduler) = {
    val pr = r.toPar
    for {
      x <- pr
      y <- other
    } yield {
      x * y
    }: @unchecked
  }

  def mapSqrtSequential(r: Range) = {
    var i = r.head
    val until = r.last
    val narr = new Array[Int](r.size)
    while (i <= until) {
      narr(i - r.head) = math.sqrt(i).toInt
      i += 1
    }
    narr
  }

  def mapSqrtParallel(r: Range)(implicit s: Scheduler) = r.toPar.map(x => math.sqrt(x).toInt)


}









