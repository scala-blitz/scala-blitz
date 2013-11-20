package org.scala.optimized.test.par     


import scala.reflect.ClassTag
import scala.collection.par._
import Scheduler.Config
import collection.immutable.HashMap



trait ParHashTrieMapSnippets {

  def aggregateSequential(hm: HashMap[Int, Int]) = {
    val it = hm.iterator
    var sum = (0,0)
    while (it.hasNext) {
      val k = it.next()
      sum = sumPairs(sum, k)
    }
    sum
  }

  def sumPairs = ((x:(Int, Int), y:(Int, Int)) => (x._1 + y._1, x._2 + y._2))

  def aggregateParallel(hm: HashMap[Int, Int])(implicit s: Scheduler) = hm.toPar.aggregate(0)(_ + _)(_ + _._1)

  def mapSequential(hm: HashMap[Int, Int]) = {
    val it = hm.iterator
    var res = HashMap.empty[Int, Int]
    while (it.hasNext) {
      val kv = it.next()
      res += ((kv._1 * 2, kv._2))
    }
    res
  }

  def mapParallel(hm: HashMap[Int, Int])(implicit s: Scheduler) = hm.toPar.map(kv => (kv._1 * 2, kv._2))
  def foldParallel(hs:HashMap[Int, Int])(implicit s: Scheduler) = hs.toPar.fold((0,0))(sumPairs)
  def foldSequentical(hs: HashMap[Int, Int]) = aggregateSequential(hs)
  
  def reduceSequential(hs: HashMap[Int, Int]) = aggregateSequential(hs)

  def reduceParallel(hs: HashMap[Int, Int])(implicit s: Scheduler) = hs.toPar.reduce(sumPairs)

  def mapReduceSequential(hs: HashMap[Int, Int]) = {
    var sum = 0
    val it = hs.iterator
    while (it.hasNext) {
      sum += it.next._1 + 1
    }
    sum
  }

  def mapReduceParallel(hs: HashMap[Int, Int])(implicit s: Scheduler) = hs.toPar.mapReduce(_._1 + 1)(_ + _)

  object customSumNum extends Numeric[(Int, Int)] {
      def fromInt(x: Int): (Int, Int) = ???
      def minus(x: (Int, Int), y: (Int, Int)): (Int, Int) = ???
      def negate(x: (Int, Int)): (Int, Int) = ???
      def plus(x: (Int, Int), y: (Int, Int)): (Int, Int) = sumPairs(x, y)
      def times(x: (Int, Int), y: (Int, Int)): (Int, Int) = ???
      def toDouble(x: (Int, Int)): Double = ???
      def toFloat(x: (Int, Int)): Float = ???
      def toInt(x: (Int, Int)): Int = ???
      def toLong(x: (Int, Int)): Long = ???
      override def zero = (0, 0)
      override def one = ???
      def compare(x: (Int, Int), y: (Int, Int)): Int = ???
    }
  def sumSequential(hs: HashMap[Int, Int]) = hs.sum(customSumNum)

  def sumParallel(a: HashMap[Int, Int])(implicit s: Scheduler) = a.toPar.sum(customSumNum, s)

  object customProdNum extends Numeric[(Int, Int)] {
      def fromInt(x: Int): (Int, Int) = ???
      def minus(x: (Int, Int), y: (Int, Int)): (Int, Int) = ???
      def negate(x: (Int, Int)): (Int, Int) = ???
      def times(x: (Int, Int), y: (Int, Int)): (Int, Int) = sumPairs(x, y)
      def plus(x: (Int, Int), y: (Int, Int)): (Int, Int) = ???
      def toDouble(x: (Int, Int)): Double = ???
      def toFloat(x: (Int, Int)): Float = ???
      def toInt(x: (Int, Int)): Int = ???
      def toLong(x: (Int, Int)): Long = ???
      override def one = (0, 0)
      override def zero = ???
      def compare(x: (Int, Int), y: (Int, Int)): Int = ???
    }

  def productSequential(hs: HashMap[Int, Int]) = hs.product(customProdNum)

  def productParallel(a: HashMap[Int, Int])(implicit s: Scheduler) = a.toPar.product(customProdNum, s)

  def countSquareMod3Sequential(hs: HashMap[Int, Int]) = {
    var count = 0
    val it = hs.iterator
    while (it.hasNext) {
      val el = it.next._1
      if ((el*el) % 3 == 0) { count += 1 }
    }
    count
  }

  def countSquareMod3Parallel(a: HashMap[Int, Int])(implicit s: Scheduler) = a.toPar.count{x => val el = x._1;  (el * el) % 3 == 0}


  def minParallel(a: HashMap[Int, Int])(implicit s: Scheduler) = a.toPar.min

  def maxParallel(a: HashMap[Int, Int])(implicit s: Scheduler) = a.toPar.max

  def findSinSequential(hs: HashMap[Int, Int]) = {
    var found = false
    var result = -1
    val it = hs.iterator
    while (it.hasNext) {
      val el = it.next._1
      if (2.0 == math.sin(el)) {
        found = true
        result = el
      }
    }
    result
  }

  def findSinParallel(a: HashMap[Int, Int])(implicit s: Scheduler) = a.toPar.find(x => math.sin(x._1) == 2.0)

  def findParallel(a: HashMap[Int, Int], elem: Int)(implicit s: Scheduler) = a.toPar.find(x => x._1 == elem && x._2 == elem)

  def existsParallel(a: HashMap[Int, Int], elem: Int)(implicit s: Scheduler) = a.toPar.exists(x => x._1 == elem && x._2 == elem)

  def forallSmallerParallel(a: HashMap[Int, Int], elem: Int)(implicit s: Scheduler) = a.toPar.forall(x => x._1 < elem && x._2 < elem)


  def foreachSequential(a: HashMap[Int, Int]) = {
    val ai = new java.util.concurrent.atomic.AtomicLong(0)
    a.foreach(x=>  if (x._1 % 500 == 0) ai.incrementAndGet())
    ai.get
  }
  def foreachParallel(a: HashMap[Int, Int])(implicit s: Scheduler) = {
    val ai = new java.util.concurrent.atomic.AtomicLong(0)
    a.toPar.foreach(x=> if (x._1 % 500 == 0) ai.incrementAndGet())
    ai.get
  }

  def filterCosSequential(hs: HashMap[Int, Int]) = {
    var ib = HashMap[Int, Int]()
    val it = hs.iterator
    while (it.hasNext) {
      val elem = it.next
      if (math.cos(elem._1) > 0.0) ib+=elem
    }
    ib
  }


  def filterCosParallel(a: HashMap[Int, Int])(implicit s: Scheduler) = a.toPar.filter(x => math.cos(x._1) > 0.0)

  val other = List((2, 2), (3, 3))

  def flatMapSequential(hs: HashMap[Int, Int]) = {
    var ib = HashMap[Int, Int]()
    val it = hs.iterator
    while (it.hasNext) {
      val elem = it.next
      other.foreach(y => ib+= sumPairs(y, elem))
    }
    ib
  }

  def flatMapParallel(a: HashMap[Int, Int])(implicit s: Scheduler) = {
    val pa = a.toPar
    for {
      x <- pa
      y <- other
    } yield {
      sumPairs(x, y)
    }: @unchecked
  }

}









