package org.scala.optimized.test.par
package scalatest



import org.scalatest._
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._
import scala.collection.optimizer._



class OptimizedBlockTest extends FunSuite {

  import scala.collection._

  test("macro.nameClashes.compiles") {
    def bla(x: Range) = optimize {
      var sum = 0f; x.foreach { x => sum += x}
    }
  }

  test("Range.foreach") {
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

  test("Range.map") {
    val size = 200000
    val elems = 0 until size
    val expected = elems.map(_ + 1)
    val obtained = optimize { elems.map(_ + 1) }
    assert(obtained.toSeq == expected.toSeq)
  }

  test("Range.filter") {
    val size = 200000
    val elems = 0 until size
    val expected = elems.filter(_ % 2 == 0)
    val obtained = optimize { elems.filter(_ % 2 == 0) }
    assert(obtained.toSeq == expected.toSeq)
  }

  test("Range.reduce") {
    val size = 200000
    val elems = 0 until size
    val expected = elems.reduce(_ + _)
    val obtained = optimize { elems.reduce(_ + _) }
    assert(obtained == expected)
  }

  test("Range.flatMap") {
    val size = 200000
    val elems = 0 until size
    val expected = elems.flatMap(x => x to (x + 10))
    val obtained = optimize { elems.flatMap(x => x to (x + 10)) }
    assert(obtained.toSeq == expected.toSeq)
  }

  test("Range.count") {
    val size = 200000
    val elems = 0 until size
    val expected = elems.count(x => (x & 1) == 0)
    val obtained = optimize { elems.count(x => (x & 1) == 0) }
    assert(obtained == expected)
  }

  test("Range.fold") {
    val size = 200000
    val elems = 0 until size
    val expected = elems.fold(0)(_ + _)
    val obtained = optimize { elems.fold(0)(_ + _) }
    assert(obtained == expected)
  }

  test("Range.sum") {
    val size = 200000
    val elems = 0 until size
    val expected = elems.sum
    val obtained = optimize { elems.sum }
    assert(obtained == expected)
  }

  test("Range.min") {
    val size = 200000
    val elems = 0 until size
    val expected = elems.min
    val obtained = optimize { elems.min }
    assert(obtained == expected)
  }

  test("Range.max") {
    val size = 200000
    val elems = 0 until size
    val expected = elems.max
    val obtained = optimize { elems.max }
    assert(obtained == expected)
  }

  test("Array.foreach") {
    val size = 200000
    val elems = (0 until size).toArray
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

  test("Array.map") {
    val size = 200000
    val elems = (0 until size).toArray
    val expected = elems.map(_ + 1)
    val obtained = optimize { elems.map(_ + 1) }
    assert(obtained.toSeq == expected.toSeq)
  }

  test("Array.aggregate") {
    val size = 200000
    val elems = (0 until size).toArray
    val expected = elems.aggregate(0.0)((acc:Double, x:Int) => acc + x, _ + _)
    val obtained = optimize { elems.aggregate(0.0)((acc:Double, x:Int) => acc + x, _ + _) }
    assert(obtained == expected)
  }

  test("Array.filter") {
    val size = 200000
    val elems = (0 until size).toArray
    val expected = elems.filter(_ % 2 == 0)
    val obtained = optimize { elems.filter(_ % 2 == 0) }
    assert(obtained.toSeq == expected.toSeq)
  }

  test("Array.reduce") {
    val size = 200000
    val elems = (0 until size).toArray
    val expected = elems.reduce(_ + _)
    val obtained = optimize { elems.reduce(_ + _) }
    assert(obtained == expected)
  }

  test("Array.flatMap") {
    val size = 200000
    val elems = (0 until size).toArray
    val expected = elems.flatMap(x => x to (x + 10))
    val obtained = optimize { elems.flatMap(x => x to (x + 10)) }
    assert(obtained.toSeq == expected.toSeq)
  }

  test("Array.count") {
    val size = 200000
    val elems = (0 until size).toArray
    val expected = elems.count(x => (x & 1) == 0)
    val obtained = optimize { elems.count(x => (x & 1) == 0) }
    assert(obtained == expected)
  }

  test("Array.fold") {
    val size = 200000
    val elems = (0 until size).toArray
    val expected = elems.fold(0)(_ + _)
    val obtained = optimize { elems.fold(0)(_ + _) }
    assert(obtained == expected)
  }

  test("Array.sum") {
    val size = 200000
    val elems = (0 until size).toArray
    val expected = elems.sum
    val obtained = optimize { elems.sum }
    assert(obtained == expected)
  }

  test("Array.min") {
    val size = 200000
    val elems = (0 until size).toArray
    val expected = elems.min
    val obtained = optimize { elems.min }
    assert(obtained == expected)
  }

  test("Array.max") {
    val size = 200000
    val elems = (0 until size).toArray
    val expected = elems.max
    val obtained = optimize { elems.max }
    assert(obtained == expected)
  }

  test("Range->Array->Array->int") {
    val size = 200000
    val elems = 0 until size
    val expected = elems.map(_ + 1).filter(_ % 2 == 0).sum
    val obtained = optimize { elems.map(_ + 1).filter(_ % 2 == 0).sum}
    assert(obtained == expected)
  }

  test("Range->(Range)->int") {
    val size = 200
    val elems = 0 until size
    val expected = elems.flatMap(x => elems.map(_ + 1)).sum
    val obtained = optimize { elems.flatMap(x => elems.map(_ + 1)).sum }
    assert(obtained == expected)
  }

  test("Range->(Range->Array->Array)->Array->int") {
    val size = 200
    val elems = 0 until size
    val expected = elems.flatMap(x => elems.map(_ + 1).filter(_ % 2 == x % 2)).filter(_ % 2 == 0).sum
    val obtained = optimize { elems.flatMap(x => elems.map(_ + 1).filter(_ % 2 == x % 2)).filter(_ % 2 == 0).sum }
    assert(obtained == expected)
  }

  test("Range->(Range->Array->Array)->Array->int, defs inside") {
    val size = 200
   
    val expected = {
      val elems = 0 until size
      elems.flatMap(x => elems.map(_ + 1).filter(_ % 2 == x % 2)).filter(_ % 2 == 0).sum
    }
    val obtained = optimize {
      val elems = 0 until size
      elems.flatMap(x => elems.map(_ + 1).filter(_ % 2 == x % 2)).filter(_ % 2 == 0).sum
    }
    assert(obtained == expected)
  }
}
