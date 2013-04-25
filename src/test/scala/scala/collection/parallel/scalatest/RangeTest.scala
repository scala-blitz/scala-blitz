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
    for (i <- 1 to 100) {
      testReduce(i)
    }
  }

}