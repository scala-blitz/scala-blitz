package scala.collection.parallel
package scalatest

import org.scalatest._
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._
import Par._
import workstealing.Ops._

class RangeTest extends FunSuite with Timeouts {

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
    for (i <- 100000 to 1000000 by 100000) {
      method(0 to i)
      method(i to 0 by -1)
    }
    for (i <- 1000000 to 10000000 by 1000000) {
      method(0 to i)
      method(i to 0 by -1)
    }
    for (i<- 1000 to 1 by -1 ) {
      method(0 to i)
      method(i to 0 by -1)
    }

      method(1 to 5 by 1000)
      method(1 to 1 by 1000)
      method(1000 to 1 by -100000)

  }

  def testReduce(r: Range): Unit = try {
    failAfter(1 seconds) {
      val x = r.reduce(_ + _)

      val pr = r.toPar
      val px = pr.reduce(_ + _)

      assert(x == px, x + ", " + px)
    }
  } catch {
    case e: exceptions.TestFailedDueToTimeoutException =>
      assert(false, "timeout for range: " + r)
  }

  test("reduce") {
    intercept[UnsupportedOperationException] {
      testReduce(0 until 0)
    }
    runForSizes(testReduce)
  }


  def testFold(r: Range): Unit = try {
    failAfter(1 seconds) {

      val x = r.fold(0)(_ + _)

      val pr = r.toPar
      val px = pr.fold(0)(_ + _)

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

  def testAggregate(r: Range): Unit = try {
    failAfter(1 seconds) {
      val x = r.aggregate(0)(_+_,_+_)

      val pr = r.toPar
      val px = pr.aggregate(0)(_+_)(_+_)

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


  def testSum(r: Range): Unit = try {
    failAfter(1 seconds) {
      val x = r.sum

      val pr = r.toPar
      val px = pr.sum

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

  def testProduct(r: Range): Unit = try {
    failAfter(1 seconds) {

      val x = r.product

      val pr = r.toPar
      val px = pr.product

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

  def testMin(r: Range): Unit = try {
    failAfter(1 seconds) {
      val x = r.min

      val pr = r.toPar
      val px = pr.min

      assert(x == px, x + ", " + px)
    }
  } catch {
    case e: exceptions.TestFailedDueToTimeoutException =>
      assert(false, "timeout for range: " + r)
  }

  test("min") {
  /*  intercept[UnsupportedOperationException] {
      testMin(0)
    } */


    runForSizes(testMin)
  }

  def testMax(r: Range): Unit = try {
    failAfter(1 seconds) {

      val x = r.max

      val pr = r.toPar
      val px = pr.max

      assert(x == px, x + ", " + px)
    }
  } catch {
    case e: exceptions.TestFailedDueToTimeoutException =>
      assert(false, "timeout for range: " + r)
  }

  test("max") {
  /*  intercept[UnsupportedOperationException] {
      testMax(0)
    } */
    runForSizes(testMax)
  }

}

