package scala.collection.parallel
package scalatest



import org.scalatest._
import Par._
import workstealing.Ops._



class RangeTest extends FunSuite {

  implicit val scheduler = new workstealing.WorkstealingTreeScheduler.ForkJoin()

  def testReduce(sz: Int) {
    val r = 0 until sz
    val x = r.reduce(_ + _)

    val pr = r.toPar
    //val px = pr.reduce(_ + _)

    //assert(x == px, x + ", " + px)
  }

  test("reduce") {
    testReduce(1)
  }

}