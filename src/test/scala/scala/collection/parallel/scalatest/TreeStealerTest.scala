package scala.collection.parallel
package scalatest



import org.scalatest._
import scala.collection._



class TreeStealerTest extends FunSuite with Helpers {
  import Par._
  import parallel.workstealing.Ops._

  def createHashSet(sz: Int) = {
    var hs = new immutable.HashSet[Int]
    for (i <- 0 until sz) hs = hs + i
    hs
  }

  test("HashTrieStealer(1).advance(1)") {
    val hs = createHashSet(1)
    val stealer = new workstealing.Trees.HashSetStealer(hs)
    stealer.rootInit()
    assert(stealer.advance(1) == 1)
    assert(stealer.hasNext)
    assert(stealer.next() == 0)
    assert(!stealer.hasNext)
    assert(stealer.advance(1) == -1)
  }

  test("HashTrieStealer(2).advance(1)") {
    val hs = createHashSet(2)
    val stealer = new workstealing.Trees.HashSetStealer(hs)
    stealer.rootInit()
    assert(stealer.advance(1) == 1)
    assert(stealer.hasNext)
    assert(hs contains stealer.next())
    assert(!stealer.hasNext)
    assert(stealer.advance(1) == 1)
    assert(stealer.hasNext)
    assert(hs contains stealer.next())
    assert(!stealer.hasNext)
    assert(stealer.advance(1) == -1)
  }

  test("HashTrieStealer(5).advance(1)") {
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
    assert(stealer.advance(1) == 1)
    assert(stealer.hasNext)
    assert(stealer.next() == 0)
    assert(!stealer.hasNext)
    assert(stealer.advance(1) == 1)
    assert(stealer.hasNext)
    assert(stealer.next() == 1)
    assert(!stealer.hasNext)
    assert(stealer.advance(1) == 1)
    assert(stealer.hasNext)
    assert(stealer.next() == 2)
    assert(!stealer.hasNext)
    assert(stealer.advance(1) == 1)
    assert(stealer.hasNext)
    assert(stealer.next() == 3)
    assert(!stealer.hasNext)
    assert(stealer.advance(1) == 1)
    assert(stealer.hasNext)
    assert(stealer.next() == 4)
    assert(!stealer.hasNext)
    assert(stealer.advance(1) == -1)
  }

  def testAdvance(sz: Int, step: Int => Int) {
    val hs = createHashSet(sz)
    testAdvanceGeneric(hs, step)
  }

  def testAdvanceGeneric[T](hs: immutable.HashSet[T], step: Int => Int) {
    val seen = mutable.Set[T]()
    val iterator = hs.iterator
    val stealer = new workstealing.Trees.HashSetStealer(hs)
    stealer.rootInit()
    var iter = 0
    var i = 0
    while (stealer.advance(step(iter)) != -1) {
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

  test("HashTrieStealer(64).advance(1)") {
    testAdvance(64, x => 1)
  }

  test("HashTrieStealer(64).advance(2)") {
    testAdvance(64, x => 2)
  }

  test("HashTrieStealer(64).advance(4)") {
    testAdvance(64, x => 4)
  }

  test("HashTrieStealer(64).advance(8)") {
    testAdvance(64, x => 8)
  }

  test("HashTrieStealer(128).advance(29)") {
    testAdvance(128, x => 29)
  }

  test("HashTrieStealer(256).advance(11)") {
    testAdvance(256, x => 37)
  }

  test("HashTrieStealer(256).advance(16)") {
    testAdvance(256, x => 16)
  }

  test("HashTrieStealer(256).advance(63)") {
    testAdvance(256, x => 16)
  }

  test("HashTrieStealer(1024).advance(64)") {
    testAdvance(1024, x => 64)
  }

  test("HashTrieStealer(4096).advance(512)") {
    testAdvance(4096, x => 512)
  }

  test("HashTrieStealer(1024).advance(8 << _)") {
    testAdvance(2048, 8 << _)
  }

  test("HashTrieStealer(256).advance(1 << _)") {
    testAdvance(256, 1 << _)
  }

  test("HashTrieStealer(1024).advance(1 << _)") {
    testAdvance(1024, 1 << _)
  }

  def testConcurrentStealing(sz: Int, step: Int => Int) {
    val hs = createHashSet(sz)
    val stealer = new workstealing.Trees.HashSetStealer(hs)
    stealer.rootInit()
    class Base extends Thread {
      val seen = mutable.Set[Int]()
      def consume(s: Stealer[Int]) {
        var it = 0
        while (s.advance(step(it)) != -1) {
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
        stealer.markStolen()
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

  test("HashTrieStealer(...).advance(1 << ...)") {
    val sizes = Seq(
      1, 2, 5, 10, 20, 50, 100, 155, 200, 500, 1000,
      2000, 5000, 10000, 20000, 50000, 100000,
      200000, 500000
    )
    for {
      sz <- sizes
      step <- Seq(2, 4, 8, 16)
    } {
      testAdvance(sz, it => 1 << (step * it))
    }
  }

  test("HashTrieStealer(collisions).advance(5)") {
    class Dummy(val x: Int) {
      override def hashCode = x % 10
    }

    val hs = for (i <- 0 until 100) yield new Dummy(i)
    testAdvanceGeneric(hs.to[immutable.HashSet], x => 5)
  }

  test("HashTrieStealer(1).markStolen()") {
    val hs = createHashSet(1)
    val stealer = new workstealing.Trees.HashSetStealer(hs)
    stealer.rootInit()
    stealer.markStolen()
    assert(stealer.advance(1) == -1)
  }

  def testAdvanceStealSplit(sz: Int, initialStep: Int, step: Int => Int) {
    def addOnce(s: Stealer[Int], set: mutable.Set[Int]) {
      if (s.advance(initialStep) != -1) {
        while (s.hasNext) set += s.next()
      }
    }

    def addAll(s: Stealer[Int], set: mutable.Set[Int]) {
      var it = 0
      while (s.advance(step(it)) != -1) {
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
    stealer.markStolen()
    val (l, r) = stealer.split
    addAll(l, lseen)
    addAll(r, rseen)
    assert(lseen.size + rseen.size + seen.size == sz, ("for size: " + sz, seen, lseen, rseen))
    assert(hs == (seen ++ lseen ++ rseen), ("for size: " + sz, seen, lseen, rseen))
  }

  test("HashTrieStealer(...).advance(...).markStolen().split") {
    val sizes = Seq(
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 50, 100, 200, 500, 1000,
      2000, 5000, 10000, 20000, 50000, 100000
    )
    for {
      sz <- sizes
      initialStep <- Seq(1, 2, 4, 8, 16, 32, 64, 128, 1024)
      step <- Seq(2, 4, 8, 16)
    } {
      testAdvanceStealSplit(sz, initialStep, it => 1 << (step * it))
    }
  }

}





