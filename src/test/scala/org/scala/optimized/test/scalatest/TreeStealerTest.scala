package org.scala.optimized.test.par
package scalatest



import org.scalatest._
import scala.collection._



class TreeStealerTest extends FunSuite with scala.collection.par.scalatest.Helpers {
  import par._

  test("advance") {
    val isBinary = collection.immutable.RedBlackTreeStealer.redBlackTreeSetIsBinary[Int]
    val tree = immutable.TreeSet(1, 2, 4, 8, 12)
    val root = immutable.RedBlackTreeStealer.redBlackRoot(tree)
    val stealer = new workstealing.BinaryTreeStealer(root, 0, tree.size, isBinary)
    assert(stealer.advance(1) > 0)
    assert(isBinary.value(stealer.topLocal) == 1)
    assert(stealer.advance(1) > 0)
    assert(isBinary.value(stealer.topLocal) == 2)
    assert(stealer.advance(1) > 0)
    assert(isBinary.value(stealer.topLocal) == 4)
    assert(stealer.advance(1) > 0)
    assert(isBinary.value(stealer.topLocal) == 8)
    assert(stealer.advance(1) > 0)
    assert(isBinary.value(stealer.topLocal) == 12)
    assert(stealer.advance(1) == -1)
  }

  test("advance empty") {
    val isBinary = collection.immutable.RedBlackTreeStealer.redBlackTreeSetIsBinary[Int]
    val tree = immutable.TreeSet[Int]()
    val root = immutable.RedBlackTreeStealer.redBlackRoot(tree)
    val stealer = new workstealing.BinaryTreeStealer(root, 0, tree.size, isBinary)
    assert(stealer.state == Stealer.AvailableOrOwned)
    assert(stealer.advance(1) == -1)
  }

}





