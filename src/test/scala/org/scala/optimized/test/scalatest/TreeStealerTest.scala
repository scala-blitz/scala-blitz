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
    println(root)
    val stealer = new workstealing.BinaryTreeStealer(root, 0, tree.size, isBinary)
    println(stealer)
    stealer.advance(1)
    println(stealer)
  }

}





