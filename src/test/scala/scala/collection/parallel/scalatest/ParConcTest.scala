package scala.collection.parallel
package scalatest



import org.scalatest._
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._
import Conc._
import Par._
import workstealing.Ops._



class ParConcTest extends FunSuite with Timeouts with Tests[Conc[Int]] with ParConcSnippets {

  def testForSizes(method: Range => Unit) {
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

  def targetCollections(r: Range) = Seq(
    createConc(r),
    createFlatConc(r, 32),
    createFlatConc(r, 128),
    createFlatConc(r, 1024)
  )

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

  test("reduce") {
    val rt = (r: Range) => r.reduce(_ + _)
    val ct = (c: Conc[Int]) => reduceParallel(c)
    intercept[UnsupportedOperationException] {
      testOperationForSize(0 until 0)(rt)(ct)
    }
    testOperation(testEmpty = false)(rt)(ct)
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

  val array1 = new Array[Int](1000000)
  val array2 = new Array[Int](1000000)

  def copyComparison(r: Range, u: Unit, v: Unit) = {
    array1.take(r.size) sameElements array2.take(r.size)
  }

  test("copyToArray") {
    testOperation(comparison = copyComparison) {
      r => r.copyToArray(array1, 0, r.size)
    } {
      c => copyToArrayParallel((c, array2))
    }
  }

}

















