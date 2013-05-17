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
    for (i <- 100000 to 1000000 by 200000) {
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

  def testReduce(r: Range, cf: Range => Conc[Int]): Unit = try {
    failAfter(6 seconds) {
      val x = r.reduce(_ + _)

      val c = cf(r)
      val pc = c.toPar
      val px = pc.reduce(_ + _)

      assert(x == px, r + ".reduce: " + x + ", " + px)
    }
  } catch {
    case e: exceptions.TestFailedDueToTimeoutException =>
      assert(false, "timeout for conc: " + r)
  }

  test("reduce") {
    intercept[UnsupportedOperationException] {
      testReduce(0 until 0, createConc)
    }
    runForSizes(testReduce(_, createConc))
    runForSizes(testReduce(_, createFlatConc(_, 128)))
  }

  def testCopyToArray(r: Range, cf: Range => Conc[Int]): Unit = try {
    failAfter(6 seconds) {
      val array = new Array[Int](r.length)

      val c = cf(r)
      val pc = c.toPar
      pc.copyToArray(array, 0, c.size)

      assert(array.sameElements(r), r + " != " + array.mkString(", "))
    }
  } catch {
    case e: exceptions.TestFailedDueToTimeoutException =>
      assert(false, "timeout for conc: " + r)
  }

  test("copyToArray") {
    runForSizes(testCopyToArray(_, createConc))
    runForSizes(testCopyToArray(_, createFlatConc(_, 32)))
    runForSizes(testCopyToArray(_, createFlatConc(_, 1024)))
  }

}

















