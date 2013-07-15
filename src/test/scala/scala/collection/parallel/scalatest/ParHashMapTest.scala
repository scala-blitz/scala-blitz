package scala.collection.parallel
package scalatest



import org.scalatest._
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._
import Conc._
import Par._
import workstealing.Ops._
import scala.collection.mutable.HashMap



class ParHashMapTest extends FunSuite with Timeouts with Tests[HashMap[Int, Int]] with ParHashMapSnippets {

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
    createHashMap(r)
  )

  def createHashMap(r: Range): HashMap[Int, Int] = {
    val hm = HashMap[Int, Int]()
    for (i <- r) hm += ((i, i))
    hm
  }


  test("aggregate") {
    val rt = (r: Range) => r.aggregate(0)(_ + _, _ + _)
    val ht = (h: HashMap[Int, Int]) => aggregateParallel(h)
    testOperation()(rt)(ht)
  }

  test("count") {
    val rt = (r: Range) => r.count(_ % 2 == 0)
    val ht = (h: HashMap[Int, Int]) => countParallel(h)
    testOperation()(rt)(ht)
  }

  test("simple filter") {
    val hm = HashMap(0 -> 0, 1 -> 2, 2 -> 4, 3 -> 6, 4 -> 8, 5 -> 10, 6 -> 12)
    val res = hm.toPar.filter{x:(Int,Int)=>x._1 % 2 == 0}
    val expected = HashMap(0 -> 0, 2 -> 4, 4 -> 8, 6 -> 12)
    assert(res.seq == HashMap(0 -> 0, 2 -> 4, 4 -> 8, 6 -> 12), (hm, res))
  }

  test("filter") {
    val rt = (r: Range) => HashMap(r zip r: _*).filter(_._1 % 5 != 0)
    val ht = (h: collection.mutable.HashMap[Int, Int]) => filterParallel(h)
    testOperation(comparison = hashMapComparison[Int, Int])(rt)(ht)
  }
  
  test("mapReduce") {
    val rt = (r: Range) => mapReduceSequential(HashMap(r zip r: _*));
    val ht = (h: collection.mutable.HashMap[Int, Int]) =>  mapReduceParallel(h) 
    intercept[UnsupportedOperationException] {
      mapReduceParallel(collection.mutable.HashMap.empty[Int, Int])
    }
    testOperation(testEmpty = false)(rt)(ht)
  }
 

  test("foreach") {
    val rt = (r: Range) => foreachSequential(r)
    val ht = (h: HashMap[Int, Int]) => foreachParallel(h)
    testOperation()(rt)(ht)
  }

  test("min") {
    val rt = (r: Range) => minSequential(HashMap(r zip r: _*))
    val ht = (h: HashMap[Int, Int]) => minParallel(h)

    intercept[UnsupportedOperationException] {
      collection.mutable.HashMap.empty[Int, Int].toPar.min
    }
    testOperation(testEmpty = false)(rt)(ht)
  }

  test("max") {
    
    val rt = (r: Range) => maxSequential(HashMap(r zip r: _*))
    val ht = (h: HashMap[Int, Int]) => maxParallel(h)

    intercept[UnsupportedOperationException] {
      collection.mutable.HashMap.empty[Int, Int].toPar.max
    }
    testOperation(testEmpty = false)(rt)(ht)
  }

  test("sum") {
    
    val rt = (r: Range) => sumSequential(HashMap(r zip r: _*))
    val ht = (h: HashMap[Int, Int]) => sumParallel(h)

    testOperation()(rt)(ht)
  }

  test("product") {
    val rt = (r: Range) => productSequential(HashMap(r zip r: _*))
    val ht = (h: HashMap[Int, Int]) => productParallel(h)

    testOperation()(rt)(ht)
  }


}

















