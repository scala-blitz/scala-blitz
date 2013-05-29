package scala.collection.parallel
package scalatest



import org.scalatest._
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._
import Par._
import workstealing.Ops._



class ParArrayTest extends FunSuite with Timeouts {

  implicit val scheduler = new workstealing.WorkstealingTreeScheduler.ForkJoin()

  def runForSizes(method: Range => Unit) {
    for (i <- 1 to 1000) {
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

  def testReduce(r: Range): Unit = try {
    failAfter(6 seconds) {
      val x = r.reduce(_ + _)

      val a = r.toArray
      val pa = a.toPar
      val px = pa.reduce(_ + _)

      assert(x == px, r + ".reduce: " + x + ", " + px)
    }
  } catch {
    case e: exceptions.TestFailedDueToTimeoutException =>
      assert(false, "timeout for array: " + r)
  }

  test("reduce") {
    intercept[UnsupportedOperationException] {
      testReduce(0 until 0)
    }
    runForSizes(testReduce)
  }

  def testAggregate(r: Range): Unit = try {
    failAfter(1 seconds) {
      val x = r.aggregate(0)(_ + _, _ + _)

      val a = r.toArray
      val pa = a.toPar
      val px = pa.aggregate(0)(_ + _)(_ + _)

      assert(x == px, x + ", " + px)
    }
  } catch {
    case e: exceptions.TestFailedDueToTimeoutException =>
      assert(false, "timeout for range: " + r)
  }

  test("aggregate") {
    testAggregate(0 until 0)
    runForSizes(testAggregate)
  }

 def testFold(r: Range): Unit = try {
    failAfter(1 seconds) {
      val x = r.fold(0)(_ + _)

      val a = r.toArray
      val pa = a.toPar
      val px = pa.fold(0)(_ + _)

      assert(x == px, x + ", " + px)
    }
  } catch {
    case e: exceptions.TestFailedDueToTimeoutException =>
      assert(false, "timeout for range: " + r)
  }

  test("fold") {
    testFold(0 until 0)
    runForSizes(testFold)
  }

 def testSum(r: Range): Unit = try {
    failAfter(1 seconds) {
      val x = r.sum

      val a = r.toArray
      val pa = a.toPar
      val px = pa.sum

      assert(x == px, x + ", " + px)
    }
  } catch {
    case e: exceptions.TestFailedDueToTimeoutException =>
      assert(false, "timeout for range: " + r)
  }

  test("sum") {
    testSum(0 until 0)
    runForSizes(testSum)
  }

  def testSumWithCustomNumeric(r: Range): Unit = try {
    failAfter(1 seconds) {
      object mynum extends Numeric[Int] {
        // Members declared in scala.math.Numeric
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

        // Members declared in scala.math.Ordering
        def compare(x: Int, y: Int): Int = ???
      }

      val a = r.toArray
      val pa = a.toPar
      val px = pa.sum(mynum, scheduler)
      val x = if (r.isEmpty) Int.MaxValue else r.min
      assert(x == px, x + ", " + px)
    }
  } catch {
    case e: exceptions.TestFailedDueToTimeoutException =>
      assert(false, "timeout for range: " + r)
  }

  test("sumCustomNumeric") {
    testSumWithCustomNumeric(0 until 0)
    runForSizes(testSumWithCustomNumeric)
  }

  def testProduct(r: Range): Unit = try {
    failAfter(1 seconds) {
      val x = r.product

      val a = r.toArray
      val pa = a.toPar
      val px = pa.product

      assert(x == px, x + ", " + px)
    }
  } catch {
    case e: exceptions.TestFailedDueToTimeoutException =>
      assert(false, "timeout for range: " + r)
  }

  test("product") {
    testSum(0 until 0)
    runForSizes(testProduct)
  }

  def testProductWithCustomNumeric(r: Range): Unit = try {
    failAfter(1 seconds) {
      object mynum extends Numeric[Int] {
        // Members declared in scala.math.Numeric
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

        // Members declared in scala.math.Ordering
        def compare(x: Int, y: Int): Int = ???
      }

      val a = r.toArray
      val pa = a.toPar
      val px = pa.product(mynum, scheduler)
      val x = if (r.isEmpty) Int.MinValue else r.max
      assert(x == px, x + ", " + px)
    }
  } catch {
    case e: exceptions.TestFailedDueToTimeoutException =>
      assert(false, "timeout for range: " + r)
  }

  test("productCustomNumeric") {
    testProductWithCustomNumeric(0 until 0)
    runForSizes(testProductWithCustomNumeric)
  }

  def testCount(r: Range): Unit = try {
    failAfter(1 seconds) {
      val x = r.count(_ % 3 == 1)

      val a = r.toArray
      val pa = a.toPar
      val px = pa.count(_ % 3 == 1)

      assert(x == px, x + ", " + px)
    }
  } catch {
    case e: exceptions.TestFailedDueToTimeoutException =>
      assert(false, "timeout for range: " + r)
  }

  test("count") {
    testFold(0 until 0)
    runForSizes(testFold)
  }

  def testMap(r: Range): Unit = try {
    failAfter(6 seconds) {
      val rm = r.map(_ + 1)

      val a = r.toArray
      val pa = a.toPar
      val pam = pa.map(_ + 1)

      assert(rm == pam.seq.toSeq, r + ".map: " + rm + ", " + pam.seq.toBuffer)
    }
  } catch {
    case e: exceptions.TestFailedDueToTimeoutException =>
      assert(false, "timeout for array: " + r)
  }

  test("map") {
    testMap(0 until 0)
    runForSizes(testMap)
  }

}

















