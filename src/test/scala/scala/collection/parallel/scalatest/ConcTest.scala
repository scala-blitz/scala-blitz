package scala.collection.parallel
package scalatest



import org.scalatest._
import Conc._



class ConcTest extends FunSuite {

  import scala.collection._

  def testTraverse(c: Conc[Int], comparison: Seq[Int]) {
    val elems = mutable.ArrayBuffer[Int]()
    def traverse(c: Conc[Int]): Unit = c match {
      case Zero => // done
      case Single(x) => elems += x
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
  }

  test("append trees") {
    val size = 20000
    val elems = 0 until size
    var conc: Conc[Int] = Zero
    for (i <- elems) conc = conc <> Single(i)
    testTraverse(conc, elems)
    testBalance(conc)
  }

  test("append elems") {
    val size = 20000
    val elems = 0 until size
    var conc: Conc[Int] = Zero
    for (i <- elems) conc = conc <> i
    testTraverse(conc, elems)
    testBalance(conc)
  }

}








