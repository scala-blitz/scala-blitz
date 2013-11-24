package org.scala.optimized.test.par
package scalatest



import org.scalatest._
import scala.collection._



class TreeStealerTest extends FunSuite with scala.collection.par.scalatest.Helpers {
  import par._

  def createHashSet(sz: Int) = {
    var hs = new immutable.HashSet[Int]
    for (i <- 0 until sz) hs = hs + i
    hs
  }

  test("HashTrieStealer(1).nextBatch(1)") {
    val hs = createHashSet(1)
    val stealer = new workstealing.Trees.HashSetStealer(hs)
    stealer.rootInit()
    assert(stealer.nextBatch(1) == 1)
    assert(stealer.hasNext)
    assert(stealer.next() == 0)
    assert(!stealer.hasNext)
    assert(stealer.nextBatch(1) == -1)
  }

  test("HashTrieStealer(2).nextBatch(1)") {
    val hs = createHashSet(2)
    val stealer = new workstealing.Trees.HashSetStealer(hs)
    stealer.rootInit()
    assert(stealer.nextBatch(1) == 1)
    assert(stealer.hasNext)
    assert(hs contains stealer.next())
    assert(!stealer.hasNext)
    assert(stealer.nextBatch(1) == 1)
    assert(stealer.hasNext)
    assert(hs contains stealer.next())
    assert(!stealer.hasNext)
    assert(stealer.nextBatch(1) == -1)
  }

  test("HashTrieStealer(5).nextBatch(1)") {
    import immutable.HashSet._
    val hs = {
      val h2a = new HashTrieSet(3, Array[immutable.HashSet[Int]](new HashSet1(0, 0), new HashSet1(1, 0)), 2)
      val h2b = new HashSet1(2, 0)
      val h2c = new HashTrieSet(3, Array[immutable.HashSet[Int]](new HashSet1(3, 0), new HashSet1(4, 0)), 2)
      val h1 = new HashTrieSet(7, Array[immutable.HashSet[Int]](h2a, h2b, h2c), 5)
      h1
    }
    val stealer = new workstealing.Trees.HashSetStealer(hs)
    stealer.rootInit()
    assert(stealer.nextBatch(1) == 1)
    assert(stealer.hasNext)
    assert(stealer.next() == 0)
    assert(!stealer.hasNext)
    assert(stealer.nextBatch(1) == 1)
    assert(stealer.hasNext)
    assert(stealer.next() == 1)
    assert(!stealer.hasNext)
    assert(stealer.nextBatch(1) == 1)
    assert(stealer.hasNext)
    assert(stealer.next() == 2)
    assert(!stealer.hasNext)
    assert(stealer.nextBatch(1) == 1)
    assert(stealer.hasNext)
    assert(stealer.next() == 3)
    assert(!stealer.hasNext)
    assert(stealer.nextBatch(1) == 1)
    assert(stealer.hasNext)
    assert(stealer.next() == 4)
    assert(!stealer.hasNext)
    assert(stealer.nextBatch(1) == -1)
  }

  def testNextBatch(sz: Int, step: Int => Int) {
    val hs = createHashSet(sz)
    testNextBatchGeneric(hs, step)
  }

  def testNextBatchGeneric[T](hs: immutable.HashSet[T], step: Int => Int) {
    val seen = mutable.Set[T]()
    val iterator = hs.iterator
    val stealer = new workstealing.Trees.HashSetStealer(hs)
    stealer.rootInit()
    var iter = 0
    var i = 0
    while (stealer.nextBatch(step(iter)) != -1) {
      while (stealer.hasNext) {
        val expected = iterator.next()
        val observed = stealer.next()
        seen += observed
        assert(expected == observed, "at " + i + ": " + expected + ", vs. observed: " + observed)
        i += 1
      }
      iter += 1
    }
    assert(seen == hs, seen.size + ", " + hs.size)
  }

  test("HashTrieStealer(64).nextBatch(1)") {
    testNextBatch(64, x => 1)
  }

  test("HashTrieStealer(64).nextBatch(2)") {
    testNextBatch(64, x => 2)
  }

  test("HashTrieStealer(64).nextBatch(4)") {
    testNextBatch(64, x => 4)
  }

  test("HashTrieStealer(64).nextBatch(8)") {
    testNextBatch(64, x => 8)
  }

  test("HashTrieStealer(128).nextBatch(29)") {
    testNextBatch(128, x => 29)
  }

  test("HashTrieStealer(256).nextBatch(11)") {
    testNextBatch(256, x => 37)
  }

  test("HashTrieStealer(256).nextBatch(16)") {
    testNextBatch(256, x => 16)
  }

  test("HashTrieStealer(256).nextBatch(63)") {
    testNextBatch(256, x => 16)
  }

  test("HashTrieStealer(1024).nextBatch(64)") {
    testNextBatch(1024, x => 64)
  }

  test("HashTrieStealer(4096).nextBatch(512)") {
    testNextBatch(4096, x => 512)
  }

  test("HashTrieStealer(1024).nextBatch(8 << _)") {
    testNextBatch(2048, 8 << _)
  }

  test("HashTrieStealer(256).nextBatch(1 << _)") {
    testNextBatch(256, 1 << _)
  }

  test("HashTrieStealer(1024).nextBatch(1 << _)") {
    testNextBatch(1024, 1 << _)
  }

  def testRecursiveStealing(sz: Int, step: Int) {
    for (i <- 1 until sz) {
      val hs = createHashSet(sz)
      val stealer = new workstealing.Trees.HashSetStealer(hs)
      val seen = mutable.HashSet[Int]()
      stealer.rootInit()

      def advanceSteal(s: Stealer[Int], level: Int): Unit = {
        var left = i
        var added = false
        while (left > 0 && s.nextBatch(step) != -1) {
          while (s.hasNext) {
            seen += s.next()
            added = true
          }
          left -= 1
        }

        if (s.markStolen()) {
          val (l, r) = s.split
  
          if (added) advanceSteal(l, level + 1)
          if (added) advanceSteal(r, level + 1)
        }
      }

      advanceSteal(stealer, 0)

      assert(seen == hs, (seen, hs))
    }
  }

  test("recursive stealing") {
    for (i <- 1 until 128; step <- Seq(1, 4, 8, 16, 32)) testRecursiveStealing(i, step)
  }

  def testConcurrentStealing(sz: Int, step: Int => Int) {
    val hs = createHashSet(sz)
    val stealer = new workstealing.Trees.HashSetStealer(hs)
    stealer.rootInit()
    class Base extends Thread {
      val seen = mutable.Set[Int]()
      def consume(s: Stealer[Int]) {
        var it = 0
        while (s.nextBatch(step(it)) != -1) {
          while (s.hasNext) seen += s.next()
          it += 1
        }
      }
    }
    class First extends Base {
      override def run() {
        consume(stealer)
      }
    }
    val t1 = new First
    var left: Stealer[Int] = null
    var right: Stealer[Int] = null
    var ls: String = null
    var rs: String = null
    class Second extends Base {
      override def run() {
        if (stealer.markStolen()) {
          // println(stealer)
          val (l, r) = stealer.split
          left = l
          right = r
          ls = left.toString
          rs = right.toString
          // println(left)
          // println(right)
          consume(left)
          consume(right)
        }
      }
    }
    val t2 = new Second
    // printHashSet(hs)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    try {
      assert(hs == (t1.seen ++ t2.seen), ("for size: " + sz, t1.seen, t2.seen, "diff: " + (hs diff (t1.seen ++ t2.seen))))
      assert(hs.size == t1.seen.size + t2.seen.size, ("sizes: " + t1.seen.size + ", " + t2.seen.size, "for size: " + sz, t1.seen, t2.seen))
    } catch {
      case e: Exception =>
        printHashSet(hs)
        println(stealer)
        println(ls)
        println(rs)
        println(left)
        println(right)
        throw e
    }
  }

  test("concurrent stealing") {
    val problematic = Seq(
      125,
      126,
      128,
      133,
      134,
      143,
      155,
      157,
      162,
      164,
      168,
      170,
      212
    ).flatMap(x => Seq.fill(20)(x))
    val specific = Seq(
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 50, 100, 200, 500, 1000,
      2000, 5000, 10000, 20000, 50000, 100000, 200000
    )
    val small = (0 until 200) ++ (200 until 100 by -1) ++ (200 until 300)
    val big = (200 until 500 by 2) ++ (500 until 5000 by 200) ++ (5000 until 50000 by 2500)
    val sizes = problematic ++ specific ++ small ++ big
    for {
      sz <- sizes
      step <- Seq(1, 2, 4, 8, 16, 32, 64)
    } {
      testConcurrentStealing(sz, it => 1 << (it * step))
    }
  }

  test("HashTrieStealer(...).nextBatch(1 << ...)") {
    val sizes = Seq(
      1, 2, 5, 10, 20, 50, 100, 155, 200, 500, 1000,
      2000, 5000, 10000, 20000, 50000, 100000,
      200000, 500000
    )
    for {
      sz <- sizes
      step <- Seq(2, 4, 8, 16)
    } {
      testNextBatch(sz, it => 1 << (step * it))
    }
  }

  test("HashTrieStealer(collisions).nextBatch(5)") {
    class Dummy(val x: Int) {
      override def hashCode = x % 10
    }

    val hs = for (i <- 0 until 100) yield new Dummy(i)
    testNextBatchGeneric(hs.to[immutable.HashSet], x => 5)
  }

  test("HashTrieStealer(1).markStolen()") {
    val hs = createHashSet(1)
    val stealer = new workstealing.Trees.HashSetStealer(hs)
    stealer.rootInit()
    stealer.markStolen()
    assert(stealer.nextBatch(1) == -1)
  }

  def testNextBatchStealSplit(sz: Int, initialStep: Int, step: Int => Int) {
    def addOnce(s: Stealer[Int], set: mutable.Set[Int]) {
      if (s.nextBatch(initialStep) != -1) {
        while (s.hasNext) set += s.next()
      }
    }

    def addAll(s: Stealer[Int], set: mutable.Set[Int]) {
      var it = 0
      while (s.nextBatch(step(it)) != -1) {
        while (s.hasNext) set += s.next()
        it += 1
      }
    }

    val hs = createHashSet(sz)
    val seen = mutable.Set[Int]()
    val lseen = mutable.Set[Int]()
    val rseen = mutable.Set[Int]()

    val stealer = new workstealing.Trees.HashSetStealer(hs)
    stealer.rootInit()
    addOnce(stealer, seen)
    assert(hs.size == stealer.elementsRemainingEstimate + seen.size)
    if (stealer.markStolen()) {
      val (l, r) = stealer.split
      addAll(l, lseen)
      addAll(r, rseen)
      assert(lseen.size + rseen.size + seen.size == sz, ("for size: " + sz, seen, lseen, rseen))
      assert(hs == (seen ++ lseen ++ rseen), ("for size: " + sz, seen, lseen, rseen))
    }
  }

  test("HashTrieStealer(...).nextBatch(...).markStolen().split") {
    val sizes = Seq(
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 50, 100, 200, 500, 1000,
      2000, 5000, 10000, 20000, 50000, 100000
    )
    for {
      sz <- sizes
      initialStep <- Seq(1, 2, 4, 8, 16, 32, 64, 128, 1024)
      step <- Seq(2, 4, 8, 16)
    } {
      testNextBatchStealSplit(sz, initialStep, it => 1 << (step * it))
    }
  }

}





