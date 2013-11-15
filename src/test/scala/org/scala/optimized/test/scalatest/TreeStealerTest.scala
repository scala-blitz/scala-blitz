package org.scala.optimized.test.par
package scalatest



import org.scalatest._
import scala.collection._



class TreeStealerTest extends FunSuite with scala.collection.par.scalatest.Helpers {
  import par._

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
    val tree = immutable.TreeSet(4, 8, 1, 2, 6, 10)
    val root = immutable.RedBlackTreeStealer.redBlackRoot(tree)
    val stealer = new workstealing.BinaryTreeStealer(root, 0, tree.size, isBinary)

  }

  test("split R*S") {

  }

  test("split R*T") {

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
    val isBinary = collection.immutable.RedBlackTreeStealer.redBlackTreeSetIsBinary[Int]
    val tree = immutable.TreeSet(0 until 4096: _*)
    println("build done")
    val root = immutable.RedBlackTreeStealer.redBlackRoot(tree)

    val random = new scala.util.Random(44)
    for(testId <- 1 to 1000) {
      val stealer = new workstealing.BinaryTreeStealer(root, 0, tree.size, isBinary)
      import scala.collection.mutable.ListBuffer
      @tailrec
      def hugeRandomFun(stack: List[Stealer[Int]], elementsCollected: ListBuffer[Int] = new ListBuffer[Int]()) : List[Int] = {
        if(!stack.isEmpty)  {
          val head = stack.head
          val tail = stack.tail
            val iWantToTake = random.nextInt(1024)

            val iTookEstimate = head.advance(iWantToTake)
            var collectedCount = 0
            if(iTookEstimate >= 0) {
              while(head.hasNext) {
                elementsCollected += head.next()
                collectedCount = collectedCount + 1
              }
            }
        //    println("wanted " + iWantToTake + " got estimate " + iTookEstimate + " got " + collectedCount)
            if(head.state != Stealer.Completed) {
              val iWantToTakeMore = random.nextBoolean()
              if(iWantToTakeMore) hugeRandomFun(stack, elementsCollected)
              else { // i want to split!
                val (splitA, splitB) = head.split
                hugeRandomFun(splitA::splitB::tail, elementsCollected)
              }
            } else hugeRandomFun(tail, elementsCollected)
        }
        else elementsCollected.toList
      }
    
    val data = hugeRandomFun(List(stealer))
    if(tree.size != data.size) assert(tree.size == data.size, "test "+ testId + "tree size: "+ tree.size + " data size: " + data.size + "\n values " + data)
    }
  }
}











