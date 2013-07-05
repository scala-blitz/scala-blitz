package scala.collection.parallel
package scalatest



import org.scalatest._
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._
import Conc._
import Par._
import workstealing.Ops._
import scala.collection.immutable.HashSet



class ParHashTrieSetTest extends FunSuite with Timeouts with Tests[HashSet[Int]] with ParHashSetSnippets {

  def testForSizes(method: Range => Unit) {
    for (i <- 1 to 100) {
      method(0 to 45)
    }
    for (i <- 1 to 100) {
      method(0 to i)
      method(i to 0 by -1)
    }
    for (i <- 1 to 100 by 10) {
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
    var hm = HashSet[Int]()
    for (i <- r) hm += i
    hm
  }

  // test("aggregate-union") {
  //   val rt = (r: Range) => r.aggregate(new HashSet[Int])(_ + _, _ ++ _)
  //   val ht = (h: HashSet[Int]) => aggregateParallelUnion(h)
  //   testOperation()(rt)(ht)
  // }

  test("aggregate") {
    val rt = (r: Range) => r.aggregate(0)(_ + _, _ + _)
    val ht = (h: HashSet[Int]) => {
      //printHashSet(h)
      collection.parallel.workstealing.TreeStealer.debug.clear()
      val r = aggregateParallel(h)
      //println("-------------------------------")
      r
    }
    try {
      testOperation()(rt)(ht)
    } catch {
      case t: Throwable =>
        collection.parallel.workstealing.TreeStealer.debug.print()
        throw t
    }
  }

}

















