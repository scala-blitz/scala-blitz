package org.scala.optimized.test.par
package scalatest



import org.scalatest._
import scala.collection._
import scala.annotation.tailrec


class TreeStealerTest extends FunSuite with scala.collection.par.scalatest.Helpers {
  import par._

  type RBNode >: Null <: AnyRef

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
    val tree = immutable.TreeSet(0 until Int.MaxValue / 2048: _*)
    val root = immutable.RedBlackTreeStealer.redBlackRoot(tree)
    val stealer = new workstealing.BinaryTreeStealer(root, 0, tree.size, isBinary)

    val iterator = stealer.subtreeIterator
    iterator.set(root)
    val data = iterator.toList

    assert(tree.toSeq == data)
  }

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
        i += 1
      }
    }
    assert(i == 100, i)
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

    assert(stealer.advance(1) > 0)
    assert(stealer.markStolen() == true)

    val nextelem = nextSingleElem(stealer)
    assert(nextelem == 2)
    val (l, r) = stealer.split
    val lelems = extract(l, nextSingleElem)
    val relems = extract(r, nextSingleElem)
    val observed = Seq(nextelem) ++ lelems ++ relems
    assert(observed == tree.toList, observed)
  }

  test("split R*LT") {
    val isBinary = collection.immutable.RedBlackTreeStealer.redBlackTreeSetIsBinary[Int]
    val tree = immutable.TreeSet(32, 4, 8, 12, 24, 76, 2)
    val root = immutable.RedBlackTreeStealer.redBlackRoot(tree)
    val stealer = new workstealing.BinaryTreeStealer(root, 0, tree.size, isBinary)

    def nextSingleElem(s: Stealer[Int]): Int = s match {
      case bts: workstealing.BinaryTreeStealer[_, _] => isBinary.value(bts.asInstanceOf[stealer.type].topLocal)
      case s: Stealer.Single[Int] => s.elem
    }

    val orig = mutable.Buffer[Int]()

    assert(stealer.advance(1) > 0)
    orig += nextSingleElem(stealer)
    assert(stealer.advance(1) > 0)
    orig += nextSingleElem(stealer)
    assert(stealer.markStolen() == true)

    val (l, r) = stealer.split
    val lelems = extract(l, nextSingleElem)
    val relems = extract(r, nextSingleElem)
    val observed = orig ++ lelems ++ relems
    assert(observed == tree.toList, observed)
  }

  test("split R*LR") {
    val isBinary = collection.immutable.RedBlackTreeStealer.redBlackTreeSetIsBinary[Int]
    val tree = immutable.TreeSet(4, 8, 1, 10, 12, 24, 76, 2)
    val root = immutable.RedBlackTreeStealer.redBlackRoot(tree)
    val stealer = new workstealing.BinaryTreeStealer(root, 0, tree.size, isBinary)

    def nextSingleElem(s: Stealer[Int]): Int = s match {
      case bts: workstealing.BinaryTreeStealer[_, _] => isBinary.value(bts.asInstanceOf[stealer.type].topLocal)
      case s: Stealer.Single[Int] => s.elem
    }

    val orig = mutable.Buffer[Int]()

    assert(stealer.advance(1) > 0)
    orig += nextSingleElem(stealer)
    assert(stealer.advance(1) > 0)
    orig += nextSingleElem(stealer)
    assert(stealer.advance(1) > 0)
    orig += nextSingleElem(stealer)
    assert(stealer.markStolen() == true)

    val (l, r) = stealer.split
    val lelems = extract(l, nextSingleElem)
    val relems = extract(r, nextSingleElem)
    val observed = orig ++ lelems ++ relems
    assert(observed == tree.toList, observed)
  }

  test("split R*LS") {
    val isBinary = collection.immutable.RedBlackTreeStealer.redBlackTreeSetIsBinary[Int]
    val tree = immutable.TreeSet(4, 16, 1, 24, 2, 6, 10)
    val root = immutable.RedBlackTreeStealer.redBlackRoot(tree)
    val stealer = new workstealing.BinaryTreeStealer(root, 0, tree.size, isBinary)

    def nextSingleElem(s: Stealer[Int]): Int = s match {
      case bts: workstealing.BinaryTreeStealer[_, _] => isBinary.value(bts.asInstanceOf[stealer.type].topLocal)
      case s: Stealer.Single[Int] => s.elem
    }

    assert(stealer.advance(512) > 0)
    val orig = mutable.Buffer[Int]()
    while (stealer.hasNext) {
      orig += stealer.next()
    }

    assert(stealer.markStolen() == true)
    val (l, r) = stealer.split
    val lelems = extract(l, nextSingleElem)
    val relems = extract(r, nextSingleElem)
    val observed = orig ++ lelems ++ relems
    assert(observed == tree.toList, observed)
  }

  test("split R*S") {
    val isBinary = collection.immutable.RedBlackTreeStealer.redBlackTreeSetIsBinary[Int]
    val tree = immutable.TreeSet(4, 16, 1, 24, 2, 6, 10)
    val root = immutable.RedBlackTreeStealer.redBlackRoot(tree)
    val stealer = new workstealing.BinaryTreeStealer(root, 0, tree.size, isBinary)

    def nextSingleElem(s: Stealer[Int]): Int = s match {
      case bts: workstealing.BinaryTreeStealer[_, _] => isBinary.value(bts.asInstanceOf[stealer.type].topLocal)
      case s: Stealer.Single[Int] => s.elem
    }

    val orig = mutable.Buffer[Int]()
    assert(stealer.advance(1) > 0)
    orig += nextSingleElem(stealer)
    assert(stealer.advance(1) > 0)
    orig += nextSingleElem(stealer)
    assert(stealer.advance(1) > 0)
    orig += nextSingleElem(stealer)
    assert(stealer.advance(1) > 0)
    orig += nextSingleElem(stealer)
    assert(stealer.advance(512) > 0)
    while (stealer.hasNext) {
      orig += stealer.next()
    }

    assert(stealer.markStolen() == true)
    val (l, r) = stealer.split
    val lelems = extract(l, nextSingleElem)
    val relems = extract(r, nextSingleElem)
    val observed = orig ++ lelems ++ relems
    assert(observed == tree.toList, observed)
    assert(lelems.isEmpty)
    assert(relems.isEmpty)
  }

  test("split R*T") {
    val isBinary = collection.immutable.RedBlackTreeStealer.redBlackTreeSetIsBinary[Int]
    val tree = immutable.TreeSet(4, 16, 1, 24, 2, 6, 10)
    val root = immutable.RedBlackTreeStealer.redBlackRoot(tree)
    val stealer = new workstealing.BinaryTreeStealer(root, 0, tree.size, isBinary)

    def nextSingleElem(s: Stealer[Int]): Int = s match {
      case bts: workstealing.BinaryTreeStealer[_, _] => isBinary.value(bts.asInstanceOf[stealer.type].topLocal)
      case s: Stealer.Single[Int] => s.elem
    }

    val orig = mutable.Buffer[Int]()
    assert(stealer.advance(1) > 0)
    orig += nextSingleElem(stealer)
    assert(stealer.advance(1) > 0)
    orig += nextSingleElem(stealer)
    assert(stealer.advance(1) > 0)
    orig += nextSingleElem(stealer)
    assert(stealer.advance(1) > 0)
    orig += nextSingleElem(stealer)
    assert(stealer.advance(1) > 0)
    orig += nextSingleElem(stealer)
    assert(stealer.advance(1) > 0)
    while (stealer.hasNext) {
      orig += stealer.next()
    }

    assert(stealer.markStolen() == true)
    val (l, r) = stealer.split
    val lelems = extract(l, nextSingleElem)
    val relems = extract(r, nextSingleElem)
    val observed = orig ++ lelems ++ relems
    assert(observed == tree.toList, observed)
  }

  test("split uninitialized stolen") {
    val isBinary = collection.immutable.RedBlackTreeStealer.redBlackTreeSetIsBinary[Int]
    val tree = immutable.TreeSet(4, 8, 1, 2, 6, 10)
    val root = immutable.RedBlackTreeStealer.redBlackRoot(tree)
    val stealer = new workstealing.BinaryTreeStealer(root, 0, tree.size, isBinary)

    def nextSingleElem(s: Stealer[Int]): Int = s match {
      case bts: workstealing.BinaryTreeStealer[_, _] => isBinary.value(bts.asInstanceOf[stealer.type].topLocal)
      case s: Stealer.Single[Int] => s.elem
    }

    assert(stealer.markStolen() == true)
    val (l, r) = stealer.split
    val lelems = extract(l, nextSingleElem)
    val relems = extract(r, nextSingleElem)
    val observed = lelems ++ relems
    assert(observed == tree.toList, observed)
  }

  test("huge random splitting") {
    import scala.collection.mutable.ListBuffer

    val isBinary = collection.immutable.RedBlackTreeStealer.redBlackTreeSetIsBinary[Int]
    val tree = immutable.TreeSet(0 until 32000: _*)
    val root = immutable.RedBlackTreeStealer.redBlackRoot(tree)
    println("-----> depth: " + isBinary.depth(root))
    println("-----> depth: " + isBinary.depthBound(tree.size, 0))
    val random = new scala.util.Random(44)
    //println("build done")

    @tailrec
    def hugeRandomFun(testId: Int, stack: List[(Stealer[Int], Int, List[String])], elementsCollected: ListBuffer[Int] = new ListBuffer[Int], last : Int = -1): List[Int] = {
      if (!stack.isEmpty)  {
        var lst = last
        val (head, level, messages) = stack.head
        val tail = stack.tail
        val iWantToTake = random.nextInt(1024)
        val iTookEstimate = head.advance(iWantToTake)
        var collectedCount = 0
        if (iTookEstimate >= 0) {
          while (head.hasNext) {
            val nxt = head.next()
            if (nxt != last + 1) {
              //println("failed on level " + level + " got " + nxt+ " after " + last + messages.mkString("\nsplit messages:", "\n\n", "\n"))
              assert(false, (nxt, last))
            }
            elementsCollected += nxt
            lst = nxt
            collectedCount = collectedCount + 1
          }
        }

        // if (testId == 53) println("wanted " + iWantToTake + " got estimate " + iTookEstimate + " got " + collectedCount)
        if (head.state != Stealer.Completed) {
          val iWantToTakeMore = random.nextBoolean()
          if (iWantToTakeMore) hugeRandomFun(testId, stack, elementsCollected, lst)
          else {
            val beforeSplit = head.toString
            // i want to split!
            // val archive = head.duplicated
            val (splitA, splitB) = head.split
            // val splitAD = splitA.duplicated
            // val afterSplit = head.toString
            // val aText = splitA.toString
            // val bText = splitB.toString
            // val elLeftA = splitAD.advance(1)
            // val elRightA = archive.advance(1)
            // if ((elLeftA > 0) && (elRightA > 0)) {
            //   val elGot = splitAD.next()
            //   val elExp = archive.next()
            //   if (elGot != elExp) {
            //     def toBs[T](s: Stealer[T]) = s.asInstanceOf[workstealing.BinaryTreeStealer[_, RBNode]]
            //     head.advance(1)
            //     println("lst:     " + lst)
            //     println("left:    " + elGot)
            //     println("archive: " + elExp)
            //     println("orig:    " + head.hasNext + " -> " + head.next() + " -> " + head.hasNext)
            //     println(toBs(head).iterator.getClass)
            //     println(List(head, splitA, splitB).mkString("\n"))
            //     println("and head before split was: " + beforeSplit)
            //     println("and head after split was: " + afterSplit)
            //     assert(false)
            //   }
            // }

            val message = ""// "was " + beforeSplit + "\nleft: " + aText + "\nright:" + bText
            hugeRandomFun(
              testId,
              (splitA, level + 1, message :: messages) :: (splitB, level + 1, message :: messages) :: tail,
              elementsCollected,
              lst)
          }
        } else hugeRandomFun(testId, tail, elementsCollected, lst)
      } else elementsCollected.toList
    }

    for (testId <- 1 to 100) {
      val stealer = new workstealing.BinaryTreeStealer(root, 0, tree.size, isBinary)
      val data = hugeRandomFun(testId, List((stealer,0, Nil)))
      if (tree.size != data.size) {
        assert(tree.size == data.size, s"test $testId, tree size: ${tree.size}, data size: ${data.size}\n  values: $data")
      }
    }
  }

}

