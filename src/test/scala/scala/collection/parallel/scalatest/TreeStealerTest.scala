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

  test("TreeStealer.External.advance") {
    val hs = createHashSet(1)
    val stealer = new workstealing.Trees.HashTrieSetStealer(hs)
    stealer.rootInit()
    println(stealer)
    stealer.advance(1)
    println(stealer)
  }

}





