package org.scala.optimized.test.par
package scalatest



import org.scalatest._
import scala.collection._



class TreeStealerTest extends FunSuite with scala.collection.par.scalatest.Helpers {
  import par._

  test("simple advance") {
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

  test("simple iterator traversal") {
    val isBinary = collection.immutable.RedBlackTreeStealer.redBlackTreeSetIsBinary[Int]
    val tree = immutable.TreeSet(1, 2, 4, 8, 12)
    val root = immutable.RedBlackTreeStealer.redBlackRoot(tree)
    val stealer = new workstealing.BinaryTreeStealer(root, 0, tree.size, isBinary)
    
    val iterator = stealer.subtreeIterator
    iterator.set(root)
    val data = iterator.toList

    assert(tree.toSeq == data)
  }

  test("advance empty") {
    val isBinary = collection.immutable.RedBlackTreeStealer.redBlackTreeSetIsBinary[Int]
    val tree = immutable.TreeSet[Int]()
    val root = immutable.RedBlackTreeStealer.redBlackRoot(tree)
    val stealer = new workstealing.BinaryTreeStealer(root, 0, tree.size, isBinary)
    assert(stealer.state == Stealer.AvailableOrOwned)
    assert(stealer.advance(1) == -1)
  }

  test("longer advance") {
    val isBinary = collection.immutable.RedBlackTreeStealer.redBlackTreeSetIsBinary[Int]
    val tree = immutable.TreeSet(0 until 100: _*)
    val root = immutable.RedBlackTreeStealer.redBlackRoot(tree)
    val stealer = new workstealing.BinaryTreeStealer(root, 0, tree.size, isBinary)

    var i = 0
    while (stealer.state == Stealer.AvailableOrOwned) {
      val chunk = stealer.advance(1)
      if (chunk > 0) {
        assert(isBinary.value(stealer.topLocal) == i)
      }
      i += 1
    }
    assert(i == 100)
  }

  test("longer traversal") {
    val isBinary = collection.immutable.RedBlackTreeStealer.redBlackTreeSetIsBinary[Int]
    val tree = immutable.TreeSet(0 until 100: _*)
    val root = immutable.RedBlackTreeStealer.redBlackRoot(tree)
    val stealer = new workstealing.BinaryTreeStealer(root, 0, tree.size, isBinary)

    val iterator = stealer.subtreeIterator
    iterator.set(root)
    val data = iterator.toList

    assert(tree.toSeq == data)
  }

  test("huge traversal") {
    val isBinary = collection.immutable.RedBlackTreeStealer.redBlackTreeSetIsBinary[Int]
    val tree = immutable.TreeSet(0 until Int.MaxValue/256: _*)
    println("build done")
    val root = immutable.RedBlackTreeStealer.redBlackRoot(tree)
    val stealer = new workstealing.BinaryTreeStealer(root, 0, tree.size, isBinary)

    val iterator = stealer.subtreeIterator
    iterator.set(root)
    val data = iterator.toList

    assert(tree.toSeq == data)
  }

  def extract(stealer: Stealer[Int], top: Stealer[Int] => Int): Seq[Int] = {
    val b = mutable.Buffer[Int]()
    while (stealer.state == Stealer.AvailableOrOwned) {
      if (stealer.advance(1) > 0) {
        b += top(stealer)
      }
    }
    b
  }

  def printRoot(t: Stealer[Int]) = t match {
    case bts: workstealing.BinaryTreeStealer[_, _] => println(bts.root)
    case _ =>
  }

  test("split R*LL") {
    val isBinary = collection.immutable.RedBlackTreeStealer.redBlackTreeSetIsBinary[Int]
    val tree = immutable.TreeSet(32, 4, 8, 12, 24, 76, 2)
    val root = immutable.RedBlackTreeStealer.redBlackRoot(tree)
    val stealer = new workstealing.BinaryTreeStealer(root, 0, tree.size, isBinary)

    def nextSingleElem(s: Stealer[Int]): Int = s match {
      case bts: workstealing.BinaryTreeStealer[_, _] => isBinary.value(bts.asInstanceOf[stealer.type].topLocal)
      case s: Stealer.Single[Int] => s.elem
    }

    println(root)
    println(stealer.advance(1))
    println(stealer)
    println(stealer.markStolen())
    println(stealer)
    val (l, r) = stealer.split
    printRoot(l)
    println(l)
    val lelems = extract(l, nextSingleElem)
    println(lelems)
    printRoot(r)
    println(r)
    val relems = extract(r, nextSingleElem)
    println(relems)
  }

}





