package scala.collection.parallel
package scalatest



import org.scalatest._
import scala.collection._



class TreeStealerTest extends FunSuite {
  import Par._
  import parallel.workstealing.Ops._

  def createHashSet(sz: Int) = {
    var hs = new immutable.HashSet[Int]
    for (i <- 0 until sz) hs = hs + i
    hs
  }

  def printHashSet[T](hs: immutable.HashSet[T], indent: Int = 0) {
    import immutable.HashSet._
    hs match {
      case t: HashTrieSet[_] =>
        println("%sTrie\n".format(" " * indent))
        for (h <- t.elems) printHashSet(h, indent + 2)
      case t: HashSet1[_] =>
        println("%s%s".format(" " * indent, t.iterator.next()))
    }
  }

  test("HashTrieStealer.advance(1)") {
    val hs = createHashSet(1)
    val stealer = new workstealing.Trees.HashTrieSetStealer(hs)
    stealer.rootInit()
    assert(stealer.advance(1) == 1)
    assert(stealer.hasNext)
    assert(stealer.next() == 0)
    assert(!stealer.hasNext)
    assert(stealer.advance(1) == -1)
  }

  test("HashTrieStealer.advance(2)") {
    val hs = createHashSet(2)
    val stealer = new workstealing.Trees.HashTrieSetStealer(hs)
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

}





