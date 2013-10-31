package org.scala.optimized.test.par
package scalatest



import org.scalatest._
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._
import scala.collection.par._
import scala.collection.immutable.HashMap



class ParHashTrieMapTest extends FunSuite with Timeouts with Tests[HashMap[Int, Int]] with ParHashTrieMapSnippets {

  def testForSizes(method: Range => Unit) {
    /*for (i <- 1 to 20000) {
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
    }*/
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
/*    for (i <- 100000 to 1000000 by 200000) {
      method(0 to i)
      method(i to 0 by -1)
    }
 */
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

  test("reduce") {
    val rt = (r: Range) => reduceSequential(createHashMap(r))
    val at = (a: HashMap[Int, Int]) => reduceParallel(a)
    intercept[UnsupportedOperationException] {
      testOperationForSize(0 until 0)(rt)(at)
    }
    testOperation(testEmpty = false)(rt)(at)
  }


  test("fold") {
    testOperation() {
      r => foldSequentical(createHashMap(r))
    } {
      a => foldParallel(a)
    }
  }

  test("sum") {
    testOperation() {
      r => sumSequential(createHashMap(r))
    } {
      a => sumParallel(a)
    }
  }

  test("product") {
    testOperation() {
      r => productSequential(createHashMap(r))
    } {
      a => productParallel(a)
    }
  }

  test("foreach") {
    testOperation() {
      r => foreachSequential(createHashMap(r))
    } {
      p => foreachParallel(p)
    }
  }

  test("count") {
    testOperation() {
      r => countSquareMod3Sequential(createHashMap(r))
    } {
      a => countSquareMod3Parallel(a)
    }
  }

  test("min") {
    testOperation() {
      r => createHashMap(r).min
    } {
      a => minParallel(a)
    }
  }

  test("max") {
    testOperation() {
      r => createHashMap(r).max
    } {
      a => maxParallel(a)
    }
  }
  
  test("find") {
    testOperation() {
      r => createHashMap(r).find(x => x._1 == Int.MaxValue && x._2 == Int.MaxValue)
    } {
      a => findParallel(a, Int.MaxValue)
    }
    testOperation() {
      r => val opt = r.find(x => x == 1)
      if(opt.isDefined) Some((1,1))
      else None
    } {
      a => findParallel(a, 1)
    }
  }

  test("exists") {
    testOperation() {
      r => createHashMap(r).exists(x => x._1 == Int.MaxValue && x._2 == Int.MaxValue)
    } {
      a => existsParallel(a, Int.MaxValue)
    }
    testOperation() {
      r => createHashMap(r).exists(x => x._1 == 1 && x._2 == 1)
    } {
      a => existsParallel(a, 1)
    }
  }

  test("forall") {
    testOperation() {
      r => r.forall(_ < Int.MaxValue)
    } {
      a => forallSmallerParallel(a, Int.MaxValue)
    }
    testOperation() {
      r => r.forall(_ < r.last)
    } {
      a => forallSmallerParallel(a, a.last._1)
    }
  }


  test("filter") {
    testOperation(comparison = immutableHashMapComparison[Int, Int]) {
      r => filterCosSequential(createHashMap(r))
    } {
      a => filterCosParallel(a)
    }
  }

  test("flatMap") {
    testOperation(comparison = immutableHashMapComparison[Int, Int]) {
      r =>  flatMapSequential(createHashMap(r))
    } {
      a => flatMapParallel(a)
    }
  }

  test("mapReduce") {
    testOperation() {
      r => mapReduceSequential(createHashMap(r))
    } {
      a => mapReduceParallel(a)
    }
  }
}

















