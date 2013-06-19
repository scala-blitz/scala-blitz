package scala.collection.parallel



import scala.reflect.ClassTag
import Par._
import workstealing.WorkstealingTreeScheduler
import workstealing.WorkstealingTreeScheduler.Config
import workstealing.Ops._



trait ReducableSnippets {

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

  def reduceParallel(r: Reducable[Int])(implicit s: WorkstealingTreeScheduler) = r.reduce(_ + _)

  def mapReduceParallel(r: Reducable[Int])(implicit s: WorkstealingTreeScheduler) = r.mapReduce(_ + 1)(_ + _)

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

  def aggregateParallel(r: Reducable[Int])(implicit s: WorkstealingTreeScheduler) = r.aggregate(0)(_ + _)(_ + _)

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

  def foldParallel(r: Reducable[Int])(implicit s: WorkstealingTreeScheduler) = r.fold(0)(_ + _)

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

  def minParallel(r: Reducable[Int])(implicit s: WorkstealingTreeScheduler) = r.min

  def minParallel(r: Reducable[Int], ord: Ordering[Int])(implicit s: WorkstealingTreeScheduler) = r.min(ord, s)

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

  def maxParallel(r: Reducable[Int])(implicit s: WorkstealingTreeScheduler) = r.max

  def maxParallel(r: Reducable[Int], ord: Ordering[Int])(implicit s: WorkstealingTreeScheduler) = r.max(ord, s)

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

  def sumParallel(r: Reducable[Int])(implicit s: WorkstealingTreeScheduler) = r.sum

  def sumParallel(r: Reducable[Int], customNum: Numeric[Int])(implicit s: WorkstealingTreeScheduler) = r.sum(customNum, s)

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

  def productParallel(r: Reducable[Int])(implicit s: WorkstealingTreeScheduler) = r.product

  def productParallel(r: Reducable[Int], customNum: Numeric[Int])(implicit s: WorkstealingTreeScheduler) = r.product(customNum, s)

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

  def countParallel(r: Reducable[Int])(implicit s: WorkstealingTreeScheduler) = r.count(x => (x & 0x1) == 0)

  def findFirstSequential(r: Range) = {
    var i = r.head
    val to = r.last
    var found = false
    var result = -1
    while (i <= to && !found) {
      if (i == 0) {
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
      if (i == Int.MinValue) {
        found = true
        result = i
      }
      i += 1
    }
    result
  }

  def findFirstParallel(r: Reducable[Int])(implicit s: WorkstealingTreeScheduler) = {
    r.find(_ == 0)
  }

  def findNotExistingParallel(r: Reducable[Int])(implicit s: WorkstealingTreeScheduler) = {
    r.find(_ == Int.MinValue)
  }

  def findSinSequential(r: Range) = {
    var i = r.head
    val to = r.last
    var found = false
    var result = -1
    while (i != to  && !found) {
      if (2.0 == math.sin(i)) {
        found = true
        result = i
      }
      i += 1
    }
    result
  }

  def findSinParallel(a: Reducable[Int])(implicit s: WorkstealingTreeScheduler) = a.find(x => math.sin(x) == 2.0)


  def existsSequential(r: Range) = {
    var i = r.head
    val to = r.last
    var result = false
    while (i <= to && (!result)) {
      if (i == 0) {
        result = true
      }
      i += 1
    }
    if (!result) ???
    else result
  }

  def existsParallel(r: Reducable[Int])(implicit s: WorkstealingTreeScheduler) = {
    r.exists(_ == 0)
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

  def forallParallel(r: Reducable[Int])(implicit s: WorkstealingTreeScheduler) = {
    r.forall(_ < Int.MaxValue)
  }

  /*def copyAllToArraySequential(ra: (Range,Array[Int]))  = {
    val r = ra._1
    val a = ra._2
    r.copyToArray(a)
    a
  }

  def copyAllToArrayParallel(ra: (Range,Array[Int]))(implicit s: WorkstealingTreeScheduler) = {
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

  def copyPartToArrayParallel(r: Range)(implicit s: WorkstealingTreeScheduler) = {
    val start = r.size / 7
    val len = 5 * (r.size / 7)
    val dest = new Array[Int](len)
    r.toPar.copyToArray(dest, start, len)
    dest
  }*/

  def mapParallel(r: Reducable[Int])(implicit s: WorkstealingTreeScheduler):Par[Array[Int]] = r.map(_ + 1)
  def mapParallel[Repr](r: Reducable[Int], customCmf: collection.parallel.generic.CanMergeFrom[Reducable[Int], Int, Par[Repr]])(implicit s: WorkstealingTreeScheduler) = r.map(_ + 1)(customCmf, s)

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

  def foreachParallel(r: Reducable[Int])(implicit s: WorkstealingTreeScheduler) = {
    val ai = new java.util.concurrent.atomic.AtomicLong(0)
    r.foreach(x=> if (x % 500 == 0) ai.incrementAndGet())
    ai.get
  }

  /*def filterMod3Parallel(r: Range)(implicit s: WorkstealingTreeScheduler) = r.toPar.filter(_ % 3 == 0)

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

  def filterCosParallel(r: Range)(implicit s: WorkstealingTreeScheduler) = r.toPar.filter(x => math.cos(x) > 0.0)

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

  def flatMapParallel(r: Range)(implicit s: WorkstealingTreeScheduler) = {
    val pr = r.toPar
    for {
      x <- pr
      y <- other
    } yield {
      x * y
    }: @unchecked
  }*/

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

  def mapSqrtParallel(r: Reducable[Int])(implicit s: WorkstealingTreeScheduler) = r.map(x => math.sqrt(x).toInt)

 
}
