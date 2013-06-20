package scala.collection.parallel



import scala.reflect.ClassTag
import Par._
import workstealing.WorkstealingTreeScheduler
import workstealing.WorkstealingTreeScheduler.Config
import workstealing.Ops._



trait ParArraySnippets {

  def foldProductSequential(a: Array[Int]) = {
    var i = 0
    val until = a.length
    var sum = 1
    while (i < until) {
      sum *= a(i)
      i += 1
    }
    sum
  }

  def foldProductParallel(a: Array[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.fold(1)(_ * _)

  def foldParallel(a: Array[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.fold(0)(_ + _)

  def reduceSequential(a: Array[Int]) = {
    var i = 0
    val until = a.length
    var sum = 1
    while (i < until) {
      sum += a(i)
      i += 1
    }
    sum
  }

  def reduceParallel(a: Array[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.reduce(_ + _)

  def mapReduceSequential(a: Array[Int]) = {
    var i = 0
    val until = a.length
    var sum = 1
    while (i < until) {
      sum += a(i) + 1
      i += 1
    }
    sum
  }

  def mapReduceParallel(a: Array[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.mapReduce(_ + 1)(_ + _)

  def aggregateSequential(a: Array[Int]) = {
    var i = 0
    val until = a.length
    var sum = 0
    while (i < until) {
      sum += a(i)
      i += 1
    }
    sum
  }

  def aggregateParallel(a: Array[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.aggregate(0)(_ + _)(_ + _)

  def sumSequential(a: Array[Int]) = {
    var i = 0
    val until = a.length
    var sum = 0
    while (i < until) {
      sum += a(i)
      i += 1
    }
    if (sum == 0) ???
    sum
  }

  def sumParallel(a: Array[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.sum

  def sumParallel(a: Array[Int], customNum: Numeric[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.sum(customNum, s)

  def productSequential(a: Array[Int]) = {
    var i = 0
    val until = a.length
    var sum = 1
    while (i < until) {
      sum *= a(i)
      i += 1
    }
    if (sum == 1) ???
    sum
  }

  def productParallel(a: Array[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.product

  def productParallel(a: Array[Int], customNum: Numeric[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.product(customNum, s)

  def countSquareMod3Sequential(a: Array[Int]) = {
    var i = 0
    val until = a.length
    var count = 0
    while (i < until) {
      if ((a(i) * a(i)) % 3 == 1) { count += 1 }
      i += 1
    }
    count
  }

  def countSquareMod3Parallel(a: Array[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.count(x => (x * x) % 3 == 0)

  def countParallel(a: Array[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.count(_ % 2 == 0)

  def minParallel(a: Array[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.min

  def minParallel(a: Array[Int], ord: Ordering[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.min(ord, s)

  def maxParallel(a: Array[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.max

  def maxParallel(a: Array[Int], ord: Ordering[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.max(ord, s)

  def findSinSequential(a: Array[Int]) = {
    var i = 0
    val to = a.length
    var found = false
    var result = -1
    while (i < to && !found) {
      if (2.0 == math.sin(a(i))) {
        found = true
        result = i
      }
      i += 1
    }
    result
  }

  def findSinParallel(a: Array[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.find(x => math.sin(x) == 2.0)

  def findParallel(a: Array[Int], elem: Int)(implicit s: WorkstealingTreeScheduler) = a.toPar.find(x => x == elem)

  def existsParallel(a: Array[Int], elem: Int)(implicit s: WorkstealingTreeScheduler) = a.toPar.exists(_ == elem)

  def forallSmallerParallel(a: Array[Int], elem: Int)(implicit s: WorkstealingTreeScheduler) = a.toPar.forall(_ < elem)

  def mapSqrtSequential(a: Array[Int]) = {
    var i = 0
    val until = a.length
    val narr = new Array[Int](until)
    while (i < until) {
      narr(i) = math.sqrt(a(i)).toInt
      i += 1
    }
    narr
  }

  def foreachSequential(a: Array[Int]) = {
    val ai = new java.util.concurrent.atomic.AtomicLong(0)
    a.foreach(x=>  if (x % 500 == 0) ai.incrementAndGet())
    ai.get
  }
  def foreachParallel(a: Array[Int])(implicit s: WorkstealingTreeScheduler) = {
    val ai = new java.util.concurrent.atomic.AtomicLong(0)
    a.toPar.foreach(x=> if (x % 500 == 0) ai.incrementAndGet())
    ai.get
  }

  def mapSqrtParallel(a: Array[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.map(x => math.sqrt(x).toInt)

  def mapParallel(a: Array[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.map(_ + 1)

  def mapParallel[Repr](a: Array[Int], customCmf: collection.parallel.generic.CanMergeFrom[Par[Array[_]], Int, Par[Repr]])(implicit s: WorkstealingTreeScheduler) = a.toPar.map(_ + 1)(customCmf, s)

  def filterMod3Sequential(a: Array[Int]) = {
    var i = 0
    val until = a.length
    val ib = new SimpleBuffer[Int]
    while (i < until) {
      val elem = a(i)
      if (elem % 3 == 0) ib.pushback(elem)
      i += 1
    }
    ib.narr
  }

  def filterMod3Parallel(a: Array[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.filter{x :Int=>x % 3 == 0}

  def filterCosSequential(a: Array[Int]) = {
    var i = 0
    val until = a.length
    val ib = new SimpleBuffer[Int]
    while (i < until) {
      val elem = a(i)
      if (math.cos(elem) > 0.0) ib.pushback(elem)
      i += 1
    }
    ib.narr
  }

  def filterCosParallel(a: Array[Int])(implicit s: WorkstealingTreeScheduler) = a.toPar.filter{x:Int => math.cos(x) > 0.0}

  val other = List(2, 3)

  def flatMapSequential(a: Array[Int]) = {
    var i = 0
    val until = a.length
    val ib = new SimpleBuffer[Int]
    while (i < until) {
      val elem = a(i)
      other.foreach(y => ib.pushback(elem * y))
      i += 1
    }
    ib.narr
  }

  def flatMapParallel(a: Array[Int])(implicit s: WorkstealingTreeScheduler) = {
    val pa = a.toPar
    for {
      x <- pa
      y <- other
    } yield {
      x * y
    }: @unchecked
  }

}









