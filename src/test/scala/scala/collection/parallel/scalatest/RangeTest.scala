package scala.collection.parallel
package scalatest



import org.scalatest._
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._
import Par._
import workstealing.Ops._



class RangeTest extends FunSuite with Timeouts {

  implicit val scheduler = new workstealing.WorkstealingTreeScheduler.ForkJoin()

  def testReduce(sz: Int): Unit = try {
    failAfter(1000 seconds) {
      val r = 0 until sz
      val x = r.reduce(_ + _)

      val pr = r.toPar
      val px = pr.reduce(_ + _)

      assert(x == px, x + ", " + px)
    }
  } catch {
    case e: exceptions.TestFailedDueToTimeoutException =>
      assert(false, "timeout for size: " + sz)
  }

  test("reduce") {
    intercept[UnsupportedOperationException] {
      testReduce(0)
    }
    for (i <- 1 to 1000) {
      testReduce(i)
    }
    for (i <- 1000 to 10000 by 1000) {
      testReduce(i)
    }
    for (i <- 10000 to 100000 by 10000) {
      testReduce(i)
    }
    for (i <- 100000 to 1000000 by 100000) {
      testReduce(i)
    }
    for (i <- 1000000 to 10000000 by 1000000) {
      testReduce(i)
    }
  }

  def testFold(sz: Int): Unit = try {
    failAfter(1000 seconds) {
      val r = 0 until sz
      val x = r.fold(0)(_ + _)

      val pr = r.toPar
      val px = pr.fold(0)(_ + _)

      assert(x == px, x + ", " + px)
    }
  } catch {
    case e: exceptions.TestFailedDueToTimeoutException =>
      assert(false, "timeout for size: " + sz)
  }

  test("fold") {
    for (i <- 0 to 1000) {
      testFold(i)
    }
    for (i <- 1000 to 10000 by 1000) {
      testFold(i)
    }
    for (i <- 10000 to 100000 by 10000) {
      testFold(i)
    }
    for (i <- 100000 to 1000000 by 100000) {
      testFold(i)
    }
    for (i <- 1000000 to 10000000 by 1000000) {
      testFold(i)
    }
  }

}





