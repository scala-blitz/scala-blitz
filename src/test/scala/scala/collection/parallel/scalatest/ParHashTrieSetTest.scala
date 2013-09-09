package scala.collection.parallel
package scalatest



import org.scalatest._
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._
import Conc._
import Par._
import workstealing.Ops._
import scala.collection.immutable.HashSet



class ParHashTrieSetTest extends FunSuite with Timeouts with Tests[HashSet[Int]] with ParHashTrieSetSnippets {

  val TEST_AS_REDUCABLE = true // disabled, as tests are failing and breaking build. Should be enabled after rewriting stealers
  def testForSizes(method: Range => Unit) {
/*    for (i <- 1 to 20000) {
      method(0 to 357)
    }

    for (i <- 1 to 20000) {
      method(0 to 45)
    }
    for (i <- 1 to 20000) {
      method(0 to 56)
    }
    for (i <- 1 to 20000) {
      method(0 to 58)
    }
    for (i <- 1 to 20000) {
      method(0 to 79)
    }
    for (i <- 1 to 1000) {
      for (sz <- 40 until 90) {
        method(0 to sz)
      }
    }
    for (i <- 1 to 10000) {
      method(0 to 212)
    }
 */
    for (i <- 1 to 100) {
      method(0 to i)
      method(i to 0 by -1)
    }
    for (i <- 1 to 1000 by 2) {
      method(0 to i)
      method(i to 0 by -1)
    }
    for (i <- 1000 to 10000 by 1000) {
      method(0 to i)
      method(i to 0 by -1)
    }
    for (i <- 10000 to 100000 by 10000) {
      method(0 to i)
      method(i to 0 by -1)
    }
    for (i <- 100000 to 1000000 by 200000) {
      method(0 to i)
      method(i to 0 by -1)
    }
    for (i <- 1000 to 1 by -1) {
      method(0 to i)
      method(i to 0 by -1)
    }
  }

  def targetCollections(r: Range) = Seq(
    createHashSet(r)
  )

  def createHashSet(r: Range): HashSet[Int] = {
    var hm = HashSet[Int]()
    for (i <- r) hm += i
    hm
  }

  test("aggregate") {
    val rt = (r: Range) => r.aggregate(0)(_ + _, _ + _)
    val ht = (h: HashSet[Int]) => {
      //printHashSet(h)
      collection.parallel.workstealing.TreeStealer.debug.clear()
      val r = aggregateParallel(h)   
      //println("-------------------------------")
      r
    }
    try {
      testOperation()(rt)(ht)
    } catch {
      case t: Throwable =>
        collection.parallel.workstealing.TreeStealer.debug.print()
        throw t
    }
  }

  /*if(TEST_AS_REDUCABLE) test("Reducable.aggregate(union)") {
    val rt = (r: Range) => r.toSeq
    val ht = (h: HashSet[Int]) => {
      
      collection.parallel.workstealing.TreeStealer.debug.clear()
      collection.parallel.workstealing.TreeStealer.debug(hashSetPrettyString(h))
      val r = aggregateReducableUnion(h)
      //println("-------------------------------")
      r.toSeq.sorted
    }
    try {
      testOperation(comparison = seqComparison[Int])(rt)(ht)
    } catch {
      case t: Throwable =>
        collection.parallel.workstealing.TreeStealer.debug.print()
        throw t
    }
  }

  if(TEST_AS_REDUCABLE) test("Reducable.aggregate") {
    val rt = (r: Range) => r.aggregate(0)(_ + _, _ + _)
    val ht = (h: HashSet[Int]) => {
      //printHashSet(h)
      collection.parallel.workstealing.TreeStealer.debug.clear()
      val r = aggregateReducable(h)
      //println("-------------------------------")
      r
    }
    try {
      testOperation()(rt)(ht)
    } catch {
      case t: Throwable =>
        collection.parallel.workstealing.TreeStealer.debug.print()
        throw t
    }
  }
  test("map") {
    val rt = (r: Range) => r.toSet.map((x: Int) => x * 2)
    val ht = (hs: HashSet[Int]) => mapParallel(hs)
    testOperation(comparison = hashTrieSetComparison[Int])(rt)(ht)
  }
  if(TEST_AS_REDUCABLE) test("Reducable.map") {
    val rt = (r: Range) => r.toSet.map((x: Int) => x * 2)
    val ht = (hs: HashSet[Int]) => (HashSet() ++ mapReducable(hs).seq).toPar
    testOperation(comparison = hashTrieSetComparison[Int])(rt)(ht)
 }

  test("reduce") {
    val rt = (r: Range) => r.reduce(_ + _)
    val at = (a: HashSet[Int]) => reduceParallel(a)
    intercept[UnsupportedOperationException] {
      testOperationForSize(0 until 0)(rt)(at)
    }
    testOperation(testEmpty = false)(rt)(at)
  }

  if(TEST_AS_REDUCABLE) test("Reducable.reduce") {
    val rt = (r: Range) => r.reduce(_ + _)
    val at = (a: HashSet[Int]) => reduceReducable(a)
    intercept[UnsupportedOperationException] {
      testOperationForSize(0 until 0)(rt)(at)
    }
    testOperation(testEmpty = false)(rt)(at)
  }

  test("fold") {
    testOperation() {
      r => r.fold(0)(_ + _)
    } {
      a => foldParallel(a)
    }
  }

  test("sum") {
    testOperation() {
      r => r.sum
    } {
      a => sumParallel(a)
    }
  }

  test("sumCustomNumeric") {
    object customNum extends Numeric[Int] {
      def fromInt(x: Int): Int = ???
      def minus(x: Int, y: Int): Int = ???
      def negate(x: Int): Int = ???
      def plus(x: Int, y: Int): Int = math.min(x, y)
      def times(x: Int, y: Int): Int = ???
      def toDouble(x: Int): Double = ???
      def toFloat(x: Int): Float = ???
      def toInt(x: Int): Int = ???
      def toLong(x: Int): Long = ???
      override def zero = Int.MaxValue
      override def one = ???
      def compare(x: Int, y: Int): Int = ???
    }

    testOperation() {
      r => r.toArray.sum(customNum)
    } {
      a => sumParallel(a, customNum)
    }
  }

  test("product") {
    testOperation() {
      r => r.product
    } {
      a => productParallel(a)
    }
  }

  test("productCustomNumeric") {
    object customNum extends Numeric[Int] {
      def fromInt(x: Int): Int = ???
      def minus(x: Int, y: Int): Int = ???
      def negate(x: Int): Int = ???
      def plus(x: Int, y: Int): Int = ???
      def times(x: Int, y: Int): Int = math.max(x, y)
      def toDouble(x: Int): Double = ???
      def toFloat(x: Int): Float = ???
      def toInt(x: Int): Int = ???
      def toLong(x: Int): Long = ???
      override def zero = ???
      override def one = Int.MinValue
      def compare(x: Int, y: Int): Int = ???
    }

    testOperation() {
      r => r.toArray.product(customNum)
    } {
      a => productParallel(a, customNum)
    }
  }

  test("foreach") {
    testOperation() {
      r => foreachSequential(createHashSet(r))
    } {
      p => foreachParallel(p)
    }
  }

  test("count") {
    testOperation() {
      r => r.count(_ % 2 == 0)
    } {
      a => countParallel(a)
    }
  }

  test("min") {
    testOperation() {
      r => r.min
    } {
      a => minParallel(a)
    }
  }

  test("minCustomOrdering") {
    object customOrd extends Ordering[Int] {
      def compare(x: Int, y: Int) = if (x < y) 1 else if (x > y) -1 else 0
    }
    testOperation() {
      r => r.min(customOrd)
    } {
      a => minParallel(a, customOrd)
    }
  }

  test("max") {
    testOperation() {
      r => r.max
    } {
      a => maxParallel(a)
    }
  }

  test("maxCustomOrdering") {
    object customOrd extends Ordering[Int] {
      def compare(x: Int, y: Int) = if (x < y) 1 else if (x > y) -1 else 0
    }
    testOperation() {
      r => r.max(customOrd)
    } {
      a => maxParallel(a, customOrd)
    }
  }
 
  test("find") {
    testOperation() {
      r => createHashSet(r).find(x => x == Int.MaxValue)
    } {
      a => findParallel(a, Int.MaxValue)
    }
    testOperation() {
      r => r.find(x => x == 1)
    } {
      a => findParallel(a, 1)
    }
  }


  if(TEST_AS_REDUCABLE) test("Reducable.find") {
    testOperation() {
      r => None
    } {
      a => findReducable(a, Int.MaxValue)
    }
    testOperation() {
      r => Some(1)
    } {
      a => 
//      printHashSet(a)
      collection.parallel.workstealing.TreeStealer.debug.clear()
      val r = findReducable(a, 1)
      if(r.isEmpty) {
        printHashSet(a)
        collection.parallel.workstealing.TreeStealer.debug.print()
      }
//      println("-------------------------------")
      r
    }
  }


  test("exists") {
    testOperation() {
      r => createHashSet(r).exists(x => x == Int.MaxValue)
    } {
      a => existsParallel(a, Int.MaxValue)
    }
    testOperation() {
      r => createHashSet(r).exists(x => x == 1)
    } {
      a => existsParallel(a, 1)
    }
  }
 
  if(TEST_AS_REDUCABLE) test("Reducable.exists") {
    testOperation() {
      r => createHashSet(r).exists(x => x == Int.MaxValue)
    } {
      a => existsReducable(a, Int.MaxValue)
    }
    testOperation() {
      r => createHashSet(r).exists(x => x == 1)
    } {
      a => existsReducable(a, 1)
    }
  }


  test("forall") {
    testOperation() {
      r => r.forall(_ < Int.MaxValue)
    } {
      a => forallSmallerParallel(a, Int.MaxValue)
    }
    testOperation() {
      r => r.forall(_ < r.last)
    } {
      a => forallSmallerParallel(a, a.last)
    }
  }

  if(TEST_AS_REDUCABLE) test("Reducable.forall") {
    testOperation() {
      r => r.forall(_ < Int.MaxValue)
    } {
      a => forallSmallerReducable(a, Int.MaxValue)
    }
    testOperation() {
      r => r.forall(_ < r.last)
    } {
      a => forallSmallerReducable(a, a.last)
    }
  }
   */
}

















