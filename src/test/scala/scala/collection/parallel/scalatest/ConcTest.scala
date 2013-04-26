package scala.collection.parallel
package scalatest



import org.scalatest._
import Conc._



class ConcTest extends FunSuite {

  test("construct") {
    var conc: Conc[Int] = Zero
    for (i <- 0 until 10) conc = conc <> i
  }

}








