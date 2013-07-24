package scala.collection.parallel
package scalatest



import org.scalatest._
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._
import Conc._
import Par._
import workstealing.Ops._
import scala.collection.mutable.HashSet



class ParHashSetTest extends FunSuite with Timeouts with Tests[HashSet[Int]] with ParHashSetSnippets {

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
    createHashSet(r)
  )

  def createHashSet(r: Range): HashSet[Int] = {
    val hs = HashSet[Int]()
    for (i <- r) hs += i
    hs
  }

  test("aggregate") {
    val rt = (r: Range) => r.aggregate(0)(_ + _, _ + _)
    val ht = (h: HashSet[Int]) => aggregateParallel(h)
    testOperation()(rt)(ht)
  }

  test("map") {
    val rt = (r: Range) => HashSet[Int](r: _*).map((x: Int) => x * 2)
    val ht = (hs: HashSet[Int]) => mapParallel(hs)
    testOperation(comparison = hashSetComparison[Int])(rt)(ht)
  }

}

















