package org.scala.optimized.test.par
package scalatest



import org.scalatest._
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._
import scala.collection.par._
import scala.collection._
import Conc._



class ConcTest extends FunSuite {

  import scala.collection._

  def testTraverse(c: Conc[Int], comparison: Seq[Int]) {
    val elems = mutable.ArrayBuffer[Int]()
    def traverse(c: Conc[Int]): Unit = c match {
      case Zero => // done
      case Single(x) => elems += x
      case Chunk(xs, sz) => elems ++= xs.take(sz)
      case left <> right => traverse(left); traverse(right)
    }

    traverse(c)
    assert(elems == comparison)
  }

  def testBalance[T](c: Conc[T]) {
    val level = c.level
    val size = c.size
    val depthBound = 4 * math.log(size) / math.log(2)

    assert(level < depthBound)

    def assertBalanceProperty(c: Conc[T]): Unit = c match {
      case Zero => // done
      case Single(x) => // done
      case Chunk(xs, sz) => // done
      case left <> right =>
        if (c.isInstanceOf[<>[_]]) assert(c.level == math.max(left.level, right.level) + 1)
        if (c.isInstanceOf[<>[_]]) assert(math.abs(right.level - left.level) <= 1)
        assertBalanceProperty(left)
        assertBalanceProperty(right)
    }

    assertBalanceProperty(c)
  }

  test("<>(Single[T])") {
    val size = 20000
    val elems = 0 until size
    var conc: Conc[Int] = Zero
    for (i <- elems) conc = conc <> Single(i)

    testTraverse(conc, elems)
    testBalance(conc)
  }

  test("<>(Conc[T])") {
    val size = 20000
    val elems1 = 0 until size
    val elems2 = size until (10 * size)
    var conc1: Conc[Int] = Zero
    for (i <- elems1) conc1 = conc1 <> Single(i)
    var conc2: Conc[Int] = Zero
    for (i <- elems2) conc2 = conc2 <> Single(i)
    val conc = conc1 <> conc2

    testTraverse(conc, elems1 ++ elems2)
    testBalance(conc)
  }

  test("<>(T)") {
    val size = 20000
    val elems = 0 until size
    var conc: Conc[Int] = Zero
    for (i <- elems) conc = conc <> i

    testTraverse(conc, elems)
    testBalance(conc)
  }

  test("Buffer.+=") {
    val size = 200000
    val elems = 0 until size
    val cb = new Conc.Buffer[Int]
    for (i <- elems) cb += i
    val conc = cb.result

    testTraverse(conc, elems)
  }

  test("append; normalize") {
    val size = 20000
    val elems = 0 until size
    var conc: Conc[Int] = Zero
    for (i <- elems) conc = conc <> i
    conc = conc.normalized

    testTraverse(conc, elems)
    testBalance(conc)
  }

  test("Buffer.merge") {
    val size = 200000
    val elems = 0 until size
    val cb1 = new Conc.Buffer[Int]
    for (i <- elems) cb1 += i
    val cb2 = new Conc.Buffer[Int]
    for (i <- elems) cb2 += i
    val cb = cb1 merge cb2
    val conc = cb.result

    testTraverse(conc, elems ++ elems)
    testBalance(conc)
  }

  test("(T)<>") {
    val size = 20000
    val elems = 0 until size
    var conc: Conc[Int] = Zero
    for (i <- elems) conc = i <> conc

    testTraverse(conc, elems.reverse)
    testBalance(conc)
  }

  test("prepend; normalize") {
    val size = 40000
    val elems = 0 until size
    var conc: Conc[Int] = Zero
    for (i <- elems) conc = i <> conc
    conc = conc.normalized

    testTraverse(conc, elems.reverse)
    testBalance(conc)
  }

  test("append; prepend; normalize") {
    val size = 15000
    val elems = 0 until size
    var conc: Conc[Int] = Zero
    for (i <- elems) conc = conc <> i
    for (i <- elems) conc = i <> conc

    testTraverse(conc, elems.reverse ++ elems)
    testBalance(conc)

    conc = conc.normalized

    testTraverse(conc, elems.reverse ++ elems)
    testBalance(conc)
  }

  test("prepend/append; normalize") {
    val size = 24000
    val elems = 0 until size
    var conc: Conc[Int] = Zero
    for (i <- elems) {
      conc = i <> conc
      conc = conc <> i
    }

    testTraverse(conc, elems.reverse ++ elems)
    testBalance(conc)

    conc = conc.normalized

    testTraverse(conc, elems.reverse ++ elems)
    testBalance(conc)
  }

}








