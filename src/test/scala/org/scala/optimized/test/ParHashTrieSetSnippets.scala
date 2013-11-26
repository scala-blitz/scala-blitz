package org.scala.optimized.test.par     


import scala.reflect.ClassTag
import scala.collection.par._
import Scheduler.Config
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

  def aggregateParallel(hm: HashSet[Int])(implicit s: Scheduler) = hm.toPar.aggregate[Int](0){(x : Int, y : Int   ) => x + y}(_ + _)   

  def aggregateReducable(hm: HashSet[Int])(implicit s: Scheduler) = hashTrieSetIsReducable(hm.toPar).aggregate(0)(_ + _)(_ + _)

  def reduceSequential(a: HashSet[Int]) = aggregateSequential(a)

  def reduceParallel(a: HashSet[Int])(implicit s: Scheduler) = a.toPar.reduce(_ + _)

  def reduceReducable(a: HashSet[Int])(implicit s: Scheduler) = hashTrieSetIsReducable(a.toPar).reduce(_ + _)






  def mapReduceParallel(hm: HashSet[Int])(implicit s: Scheduler) = hm.toPar.mapReduce(_ + 1)(_ + _)

  def mapReduceSequential(hm: HashSet[Int]) = {
    val it = hm.iterator
    var sum = 0
    while (it.hasNext) {
      val k = it.next() + 1
      sum += k
    }
    sum
  }
  
  def aggregateParallelUnion(hm: HashSet[Int])(implicit s: Scheduler) = hm.toPar.aggregate(new HashSet[Int])(_ ++ _)(_ + _)
  def aggregateReducableUnion(hm: HashSet[Int])(implicit s: Scheduler) = hashTrieSetIsReducable(hm.toPar).aggregate(new HashSet[Int])(_ ++ _)(_ + _)

  def mapSequential(hm: HashSet[Int]) = {
    val it = hm.iterator
    var res = HashSet.empty[Int]
    while (it.hasNext) {
      val k = it.next()
      res += k * 2
    }
    res
  }

  def mapParallel(hm: HashSet[Int])(implicit s: Scheduler) = hm.toPar.map(_ * 2)


  def mapReducable(hm: HashSet[Int])(implicit s: Scheduler) = hashTrieSetIsReducable(hm.toPar).map(_ * 2)

  def foldProductSequential(a: HashSet[Int]) = {
    val it = a.iterator
    var sum = 1
    while (it.hasNext) {
      sum *= it.next
    }
    sum
  }

  def foldProductParallel(a: HashSet[Int])(implicit s: Scheduler) = a.toPar.fold(1)(_ * _)

  def foldParallel(a: HashSet[Int])(implicit s: Scheduler) = a.toPar.fold(0)(_ + _)
  def foldSequential(a: HashSet[Int]) = sumSequential(a)

  def sumSequential(a: HashSet[Int]) = {
    val it = a.iterator
    var sum = 0
    while (it.hasNext) {
      sum += it.next
    }
    if (sum == 0) ???
    sum
  }

  def sumParallel(a: HashSet[Int])(implicit s: Scheduler) = a.toPar.sum

  def sumParallel(a: HashSet[Int], customNum: Numeric[Int])(implicit s: Scheduler) = a.toPar.sum(customNum, s)

  def productSequential(a: HashSet[Int]) = foldProductSequential(a)

  def productParallel(a: HashSet[Int])(implicit s: Scheduler) = a.toPar.product

  def productParallel(a: HashSet[Int], customNum: Numeric[Int])(implicit s: Scheduler) = a.toPar.product(customNum, s)

  def countSquareMod3Sequential(a: HashSet[Int]) = {
    val it = a.iterator
    var count = 0
    while (it.hasNext) {
      val el = it.next
      if ((el * el) % 3 == 1) { count += 1 }
    }
    count
  }

  def countSquareMod3Parallel(a: HashSet[Int])(implicit s: Scheduler) = a.toPar.count(x => (x * x) % 3 == 0)

  def countParallel(a: HashSet[Int])(implicit s: Scheduler) = a.toPar.count(_ % 2 == 0)

  def minParallel(a: HashSet[Int])(implicit s: Scheduler) = a.toPar.min

  def minParallel(a: HashSet[Int], ord: Ordering[Int])(implicit s: Scheduler) = a.toPar.min(ord, s)

  def maxParallel(a: HashSet[Int])(implicit s: Scheduler) = a.toPar.max

  def maxParallel(a: HashSet[Int], ord: Ordering[Int])(implicit s: Scheduler) = a.toPar.max(ord, s)

  def foreachSequential(a: HashSet[Int]) = {
    val ai = new java.util.concurrent.atomic.AtomicLong(0)
    a.foreach(x=>  if (x % 500 == 0) ai.incrementAndGet())
    ai.get
  }

  def foreachParallel(a: HashSet[Int])(implicit s: Scheduler) = {
    val ai = new java.util.concurrent.atomic.AtomicLong(0)
    a.toPar.foreach(x=> if (x % 500 == 0) ai.incrementAndGet())
    ai.get
  }

  def findParallel(a: HashSet[Int], elem: Int)(implicit s: Scheduler) = a.toPar.find(x => x == elem)

  def filterMod3Parallel(a: HashSet[Int])(implicit s: Scheduler) = a.toPar.filter(x => x % 3 == 1)
  def filterSinParallel(a: HashSet[Int])(implicit s: Scheduler) = a.toPar.filter(x => math.sin(x) == 1)
  def filterMod3Sequential(a: HashSet[Int]) = {
    val b = new SimpleBuffer[Int]()
    val it = a.iterator
    while (it.hasNext) {
      val el = it.next
      if (el % 3 == 1) b.pushback(el)
    }
    b.narr  
  }

  def filterSinSequential(a: HashSet[Int]) = {
    val b = new SimpleBuffer[Int]()
    val it = a.iterator
    while (it.hasNext) {
      val el = it.next
      if (math.sin(el)  == 1) b.pushback(el)
    }
    b.narr  
  }

  def findSequential(a: HashSet[Int], elem: Int) = a.find(x => x == elem)

  def findSinSequential(a: HashSet[Int]) = a.find(x => math.sin(x) == 2)
  def findSinParallel(a: HashSet[Int])(implicit s: Scheduler) = a.toPar.find(x => math.sin(x) == 2)

  def findReducable(a: HashSet[Int], elem: Int)(implicit s: Scheduler) = hashTrieSetIsReducable(a.toPar).find(x => x == elem)

  def existsParallel(a: HashSet[Int], elem: Int)(implicit s: Scheduler) = a.toPar.exists(x => x == elem)

  def existsReducable(a: HashSet[Int], elem: Int)(implicit s: Scheduler) = hashTrieSetIsReducable(a.toPar).exists(x => x == elem)

  def forallSmallerParallel(a: HashSet[Int], elem: Int)(implicit s: Scheduler) = a.toPar.forall(x => x < elem)

  def forallSmallerReducable(a: HashSet[Int], elem: Int)(implicit s: Scheduler) = hashTrieSetIsReducable(a.toPar).forall(x => x < elem)

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

  def flatMapParallel(a: HashSet[Int])(implicit s: Scheduler) = {
    val pa = a.toPar
    for {
      x <- pa
      y <- other
    } yield {
      x * y
    }: @unchecked
  }

}









