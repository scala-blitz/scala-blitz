package org.scala.optimized.test.par
package scalatest



import org.scalatest._
import scala.collection._
import scala.collection.par._



class HashStealerTest extends FunSuite with scala.collection.par.scalatest.Helpers {

  def createHash(sz: Int) = {
    val hm = mutable.HashMap[Int, Int]()
    for (i <- 0 until sz) hm += ((i, i))
    hm
  }

  def testTraversal(sz: Int) {
    def traverseHash(hm: mutable.HashMap[Int, Int]) {
      val stealer = hm.toPar.stealer
      stealer.advance(getHashTableContents(hm).table.length)
  
      val s = mutable.HashSet[Int]()
      while (stealer.hasNext) s += stealer.next()._1
  
      assert(s == (0 until hm.size).toSet, s)
    }

    val hm = createHash(sz)
    traverseHash(hm)
  }

  def testSizes(f: Int => Unit) {
    for (i <- 0 until 10) f(i)
    for (i <- 10 until 100 by 10) f(i)
    for (i <- 100 until 1000 by 100) f(i)
    for (i <- 1000 until 10000 by 1000) f(i)
    for (i <- 10000 until 100000 by 10000) f(i)
  }

  test("stealer.traverse") {
    testSizes(testTraversal)
  }

  def testAdvance(sz: Int) {
    def traverseHash(hm: mutable.HashMap[Int, Int]) {
      val b = mutable.HashSet[Int]()
      val stealer = hm.toPar.stealer
      var step = 1
      while (stealer.isAvailable) {
        stealer.advance(step)
        while (stealer.hasNext) b += stealer.next()._1
        step *= 2
      }

      assert(b == (0 until hm.size).toSet)
    }

    val hm = createHash(sz)
    traverseHash(hm)
  }

  test("stealer.advance") {
    testSizes(testAdvance)
  }

}








