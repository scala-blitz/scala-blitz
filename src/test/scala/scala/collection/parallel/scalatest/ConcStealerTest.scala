package scala.collection.parallel
package scalatest



import org.scalatest._
import scala.collection._
import Conc._



class ConcStealerTest extends FunSuite {

  def createConc(sz: Int) = {
    val elems = 0 until sz
    var conc: Conc[Int] = Zero
    for (i <- elems) conc = conc <> (Single(i): Conc[Int])
    conc
  }

  def createFlatConc(sz: Int, chunksz: Int) = {
    val elems = (0 until sz).iterator.grouped(chunksz)
    var conc: Conc[Int] = Zero
    for (sq <- elems) conc = conc <> new Chunk(sq.toArray, sq.size)
    conc
  }

  def testTraversal(sz: Int) {
    def traverseConc(c: Conc[Int]) {
      val stealer = new parallel.workstealing.Concs.ConcStealer(c, 0, c.size)
      stealer.advance(c.size)
  
      val b = mutable.ArrayBuffer[Int]()
      while (stealer.hasNext) b += stealer.next()
  
      assert(b == (0 until c.size))
    }

    val c = createConc(sz)
    traverseConc(c)

    val cf4 = createFlatConc(sz, 4)
    traverseConc(cf4)

    val cf32 = createFlatConc(sz, 32)
    traverseConc(cf32)
  }

  def testSizes(f: Int => Unit) {
    for (i <- 0 until 10) f(i)
    for (i <- 10 until 100 by 10) f(i)
    for (i <- 100 until 1000 by 100) f(i)
  }

  test("stealer.traverse") {
    testSizes(testTraversal)
  }

  def testMoveN(sz: Int) {
    def moveStealer(c: Conc[Int], startIndex: Int, num: Int) {
      val stealer = new parallel.workstealing.Concs.ConcStealer(c, startIndex, c.size)
      stealer.advance(c.size)
      if (c.size > 0) stealer.moveN(num)
  
      val b = mutable.ArrayBuffer[Int]()
      while (stealer.hasNext) {
        val elem = stealer.next()
        b += elem
      }
    }

    val cs = createConc(sz)
    moveStealer(cs, 0, cs.size)
    moveStealer(cs, 0, cs.size / 2)
    moveStealer(cs, 0, cs.size / 4)
    moveStealer(cs, cs.size / 4, cs.size / 4)

    val cf = createFlatConc(sz, 4)
    moveStealer(cf, 0, cf.size)
    moveStealer(cf, 0, cf.size / 2)
    moveStealer(cf, 0, cf.size / 4)
    moveStealer(cf, cf.size / 4, cf.size / 4)

    val cf128 = createFlatConc(sz, 128)
    moveStealer(cf128, 0, cf128.size)
    moveStealer(cf128, 0, cf128.size / 2)
    moveStealer(cf128, 0, cf128.size / 4)
    moveStealer(cf128, cf128.size / 4, cf128.size / 4)
  }

  test("stealer.moveN") {
    testSizes(testMoveN)
  }

  def testAdvance(sz: Int) {
    def testTraversal(c: Conc[Int]) {
      val b = mutable.ArrayBuffer[Int]()
      val stealer = new parallel.workstealing.Concs.ConcStealer(c, 0, c.size)
      var step = 1
      while (stealer.isAvailable) {
        stealer.advance(step)
        while (stealer.hasNext) b += stealer.next()
        step *= 2
      }

      assert(b == (0 until c.size))
    }

    def testMove(c: Conc[Int]) {
      val b = mutable.ArrayBuffer[Int]()
      val stealer = new parallel.workstealing.Concs.ConcStealer(c, 0, c.size)
      var step = 1
      while (stealer.isAvailable) {
        stealer.advance(step)
        stealer.moveN(step)
        step *= 2
      }
    }

    val c = createConc(sz)
    testTraversal(c)
    testMove(c)

    val cf = createFlatConc(sz, 32)
    testTraversal(cf)
    testMove(cf)

    val cf128 = createFlatConc(sz, 128)
    testTraversal(cf128)
    testMove(cf128)
  }

  test("stealer.advance") {
    testSizes(testAdvance)
  }

}








