package scala.collection.parallel
package scalatest



import org.scalatest._
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._
import Conc._
import Par._
import workstealing.Ops._



class ParConcTest extends FunSuite with Timeouts {

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
    for (i <- 1000 to 1 by -1) {
      method(0 to i)
      method(i to 0 by -1)
    }
  }

  def createConc(r: Range): Conc[Int] = {
    var c: Conc[Int] = Zero
    for (i <- r) c = c <> (Single(i): Conc[Int])
    c
  }

  def createFlatConc(r: Range, chunksz: Int) = {
    val elems = r.iterator.grouped(chunksz)
    var conc: Conc[Int] = Zero
    for (sq <- elems) conc = conc <> new Chunk(sq.toArray, sq.size)
    conc
  }

  def testReduce(r: Range): Unit = try {
    failAfter(1 seconds) {
      val x = r.reduce(_ + _)

      val c = createConc(r)
      val pc = c.toPar
      val px = pc.reduce(_ + _)

      val cf = createFlatConc(r, 128)
      val pcf = cf.toPar
      val pxf = pcf.reduce(_ + _)

      assert(x == px, r + ".reduce: " + x + ", " + px)
      assert(x == pxf, r + ".reduce: " + x + ", " + pxf)
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

}