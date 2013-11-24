package org.scala.optimized.test.par
package scalatest



import org.scalatest._
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._
import scala.collection.optimizer._



class OptimizedBlockTest extends FunSuite {

  import scala.collection._

  test("foreach") {
    val size = 200000
    val elems = 0 until size
    val expected = {
      var count = 0
      elems.foreach(x => count = count + x)
      count
    }
    val obtained = {
      var count = 0
      optimize { elems.foreach(x => count = count + x) }
      count
    }
    assert(obtained == expected)
  }

  test("map") {
    val size = 200000
    val elems = 0 until size
    val expected = elems.map(_ + 1)
    val obtained = optimize { elems.map(_ + 1) }
    assert(obtained.toSeq == expected.toSeq)
  }

  test("filter") {
    val size = 200000
    val elems = 0 until size
    val expected = elems.filter(_ % 2 == 0)
    val obtained = optimize { elems.filter(_ % 2 == 0) }
    assert(obtained.toSeq == expected.toSeq)
  }

  test("reduce") {
    val size = 200000
    val elems = 0 until size
    val expected = elems.reduce(_ + _)
    val obtained = optimize { elems.reduce(_ + _) }
    assert(obtained == expected)
  }

  test("flatMap") {
    val size = 200000
    val elems = 0 until size
    val expected = elems.flatMap(x => x to (x + 10))
    val obtained = optimize { elems.flatMap(x => x to (x + 10)) }
    assert(obtained.toSeq == expected.toSeq)
  }

  test("count") {
    val size = 200000
    val elems = 0 until size
    val expected = elems.count(x => (x & 1) == 0)
    val obtained = optimize { elems.count(x => (x & 1) == 0) }
    assert(obtained == expected)
  }

  test("fold") {
    val size = 200000
    val elems = 0 until size
    val expected = elems.fold(0)(_ + _)
    val obtained = optimize { elems.fold(0)(_ + _) }
    assert(obtained == expected)
  }

  test("sum") {
    val size = 200000
    val elems = 0 until size
    val expected = elems.sum
    val obtained = optimize { elems.sum }
    assert(obtained == expected)
  }

  test("min") {
    val size = 200000
    val elems = 0 until size
    val expected = elems.min
    val obtained = optimize { elems.min }
    assert(obtained == expected)
  }

  test("max") {
    val size = 200000
    val elems = 0 until size
    val expected = elems.max
    val obtained = optimize { elems.max }
    assert(obtained == expected)
  }
}
