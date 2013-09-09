package scala.collection.parallel
package scalatest



import org.scalatest._
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._
import Par._
import workstealing.Ops._



class ParRangeTest extends FunSuite with Timeouts with Tests[Range] with ParRangeSnippets {

  def testForSizes(method: Range => Unit) {
    for (i <- 1 to 1000) {
      method(0 to i)
      method(i to 0 by -1)
    }
    for (i <- 1000 to 10000 by 1000) {
      method(0 to i)
      method(i to 0 by -1)
      method(i to 0 by -13)
      method(0 to i by 13)
    }
    for (i <- 10000 to 100000 by 10000) {
      method(0 to i)
      method(i to 0 by -1)
      method(i to 0 by -15)
      method(0 to i by 15)
    }
    for (i <- 100000 to 1000000 by 100000) {
      method(0 to i)
      method(i to 0 by -1)
      method(i to 0 by -29)
      method(0 to i by 29)
    }
 
    method(1 to 5 by 1000)
    method(1 to 1 by 1000)
    method(1000 to 1 by -100000)
  }

  def targetCollections(r: Range) = Seq(r)

  test("reduce") {
    val rt = (r: Range) => r.reduce(_ + _)
    val pt = (p: Range) => reduceParallel(p)
    intercept[UnsupportedOperationException] {
      testOperationForSize(0 until 0)(rt)(pt)
    }
    testOperation(testEmpty = false)(rt)(pt)
  }

  test("fold") {
    testOperation() {
      r => r.fold(0)(_ + _)
    } {
      p => foldParallel(p)
    }
  }

  test("count") {
    testOperation() {
      r => r.count(_ % 2 == 0)
    } {
      p => countParallel(p)
    }
  }

  test("groupMapAggregate") {
    testOperation(comparison = hashMapComparison[Int,Int]) { 
      r => scala.collection.mutable.HashMap()++r.groupBy(x=>x%15).map(x=>(x._1,x._2.sum))
    } {
      p => p.toPar.groupMapAggregate(x=>x%15)(x=>x)(_+_)
    }
  }

  test("groupBy") {
    testOperation(comparison = hashMapArrayComparison[Int, Int]) { 
      r => scala.collection.mutable.HashMap() ++ r.groupBy(x=>x%15)
    } {
      p => p.toPar.groupBy(x=>x%15)
    }
  }

  test("aggregate") {
    testOperation() {
      r => r.aggregate(0)(_ + _, _ + _)
    } {
      p => aggregateParallel(p)
    }
  }

  test("sum") {
    testOperation() {
      r => r.sum
    } {
      p => sumParallel(p)
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
      p => sumParallel(p, customNum)
    }
  }

  test("product") {
    testOperation() {
      r => r.product
    } {
      p => productParallel(p)
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
      p => productParallel(p, customNum)
    }
  }

  test("min") {
    testOperation() {
      r => r.min
    } {
      p => minParallel(p)
    }
  }

  test("minCustomOrdering") {
    object customOrd extends Ordering[Int] {
      def compare(x: Int, y: Int) = if (x < y) 1 else if (x > y) -1 else 0
    }
    testOperation() {
      r => r.min(customOrd)
    } {
      p => minParallel(p, customOrd)
    }
  }

  test("max") {
    testOperation() {
      r => r.max
    } {
      p => maxParallel(p)
    }
  }

  test("maxCustomOrdering") {
    object customOrd extends Ordering[Int] {
      def compare(x: Int, y: Int) = if (x < y) 1 else if (x > y) -1 else 0
    }
    testOperation() {
      r => r.max(customOrd)
    } {
      p => maxParallel(p, customOrd)
    }
  }

  test("foreach") {
    testOperation() {
      r => foreachSequential(r)
    } {
      p => foreachParallel(p)
    }
  }

  test("find") {
    //should be found
    testOperation() {
      r => r.find(_ == r.last)
    } {
      p => findLastParallel(p)
    }
    //should not be found
    testOperation() {
      r => r.find(_ == r.max + 1)
    } {
      p => findNotExistingParallel(p)
    }

  }

  test("exists") {
    testOperation() {
      r => r.exists(_ == (r.last + 1))
    } {
      p => existsParallel(p)
    }
  }

  test("forall") {
    testOperation() {
      r => r.forall(_ < Int.MaxValue)
    } {
      p => forallParallel(p)
    }
  }

  test("copyAllToArray") {
    testOperation(comparison = seqComparison[Int]) {
      r => copyAllToArraySequential((r,new Array[Int](r.length)))
    } {
      p => copyAllToArrayParallel((p,new Array[Int](p.length)))
    }
  }

  test("copyPartToArray") {
    testOperation(comparison = seqComparison[Int]) {
      r => copyPartToArraySequential(r)
    } {
      p => copyPartToArrayParallel(p)
    }
  }

  test("map") {
    testOperation(comparison = arrayComparison[Int]) {
      r => r.map(_ + 1)
    } {
      p => mapParallel(p)
    }
  }

  test("mapCustomCanMergeFrom") {
    object customCmf extends scala.collection.parallel.generic.CanMergeFrom[Par[Range], Int, Par[Conc[Int]]] {
        def apply(from: Par[Range]) = new Conc.ConcMerger[Int]
        def apply() = new Conc.ConcMerger[Int]
      }
    testOperation(comparison = concComparison[Int]) {
      r => r.map(_ + 1)
    } {
      p => mapParallel[Conc[Int]](p, customCmf)
    }
  }

  test("filter") {
    testOperation(comparison = arrayComparison[Int]) {
      r => r.filter(_ % 3 == 0)
    } {
      p => filterMod3Parallel(p)
    }
  }

  test("flatMap") {
    testOperation(comparison = arrayComparison[Int]) {
      r => for (x <- r; y <- other) yield x * y
    } {
      p => flatMapParallel(p)
    }
  }
  test("mapReduce") {
    testOperation() {
      r => r.map(_ + 1).reduce(_ + _)
    } {
      p => mapReduceParallel(p)
    }
  }
}

