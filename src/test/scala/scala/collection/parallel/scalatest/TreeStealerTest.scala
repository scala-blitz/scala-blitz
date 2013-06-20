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

  test("HashTrieStealer.advance") {
    val hs = createHashSet(1)
    val stealer = new workstealing.Trees.HashTrieSetStealer(hs)
    stealer.rootInit()
    assert(stealer.advance(1) == 1)
    assert(stealer.next() == 0)
    assert(stealer.advance(1) == -1)
  }

}





