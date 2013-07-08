package scala.collection.parallel
package scalatest



import org.scalatest._
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._
import Conc._
import Par._
import workstealing.Ops._
import scala.collection.immutable.HashMap



class ParHashTrieMapTest extends FunSuite with Timeouts with Tests[HashMap[Int, Int]] with ParHashTrieMapSnippets {

  def testForSizes(method: Range => Unit) {
    for (i <- 1 to 20000) {
      method(0 to 45)
    }
    for (i <- 1 to 20000) {
      method(0 to 56)  
    }
    for (i <- 1 to 1000) {
      for (sz <- 40 until 90) {
        method(0 to sz)
      }
    }
    for (i <- 1 to 10000) {
      method(0 to 212)
    }
    for (i <- 1 to 100) {
      method(0 to i)
      method(i to 0 by -1)
    }
    for (i <- 1 to 1000 by 2) {
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
    createHashMap(r)
  )

  def createHashMap(r: Range): HashMap[Int, Int] = {
    var hm = HashMap[Int, Int]()
    for (i <- r) hm += ((i, i))
    hm
  }

  test("aggregate") {
    val rt = (r: Range) => r.aggregate(0)(_ + _, _ + _)
    val ht = (h: HashMap[Int, Int]) => aggregateParallel(h)
    try {
      testOperation()(rt)(ht)
    } catch {
      case t: Throwable =>
        throw t
    }
  }

  test("map") {
    val rt = (r: Range) => (r zip r).toMap.map(kv => (kv._1 * 2, kv._2))
    val ht = (h: HashMap[Int, Int]) => mapParallel(h)
    testOperation(comparison = hashTrieMapComparison[Int, Int])(rt)(ht)
  }

}

















