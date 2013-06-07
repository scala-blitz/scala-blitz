package scala.collection.parallel



import scala.reflect.ClassTag
import Par._
import workstealing.WorkstealingTreeScheduler
import workstealing.WorkstealingTreeScheduler.Config
import workstealing.Ops._



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

  def reduceParallel(r: Range)(implicit s: WorkstealingTreeScheduler) = r.toPar.reduce(_ + _)

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

  def aggregateParallel(r: Range)(implicit s: WorkstealingTreeScheduler) = r.toPar.aggregate(0)(_ + _)(_ + _)

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

  def foldParallel(r: Range)(implicit s: WorkstealingTreeScheduler) = r.toPar.fold(0)(_ + _)

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

  def minParallel(r: Range)(implicit s: WorkstealingTreeScheduler) = r.toPar.min

  def minParallel(r: Range, ord: Ordering[Int])(implicit s: WorkstealingTreeScheduler) = r.toPar.min(ord, s)

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

  def maxParallel(r: Range)(implicit s: WorkstealingTreeScheduler) = r.toPar.max

  def maxParallel(r: Range, ord: Ordering[Int])(implicit s: WorkstealingTreeScheduler) = r.toPar.max(ord, s)

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

  def sumParallel(r: Range)(implicit s: WorkstealingTreeScheduler) = r.toPar.sum

  def sumParallel(r: Range, customNum: Numeric[Int])(implicit s: WorkstealingTreeScheduler) = r.toPar.sum(customNum, s)

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

  def productParallel(r: Range)(implicit s: WorkstealingTreeScheduler) = r.toPar.product

  def productParallel(r: Range, customNum: Numeric[Int])(implicit s: WorkstealingTreeScheduler) = r.toPar.product(customNum, s)

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

  def countParallel(r: Range)(implicit s: WorkstealingTreeScheduler) = r.toPar.count(x => (x & 0x1) == 0)

  def findSequential(r: Range) = {
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

  def findParallel(r: Range)(implicit s: WorkstealingTreeScheduler) = {
    val mx = r.last
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

  def existsParallel(r: Range)(implicit s: WorkstealingTreeScheduler) = {
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

  def forallParallel(r: Range)(implicit s: WorkstealingTreeScheduler) = {
    val mx = r.last + 1
    r.toPar.forall(_ == mx)
  }

  def copyToArraySequential(rra: (Range, Array[Int])) {
    val r = rra._1
    val a = rra._2
    var i = r.head
    val to = r.last
    while (i <= to) {
      a(i) = i
      i += 1
    }
  }

  def copyToArrayParallel(rra: (Range, Array[Int]))(implicit s: WorkstealingTreeScheduler) = {
    val r = rra._1
    val a = rra._2
    r.toPar.copyToArray(a)
  }

}









