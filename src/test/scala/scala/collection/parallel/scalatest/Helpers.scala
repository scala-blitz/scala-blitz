package scala.collection.parallel
package scalatest



import org.scalatest._
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._
import scala.collection._



trait Helpers {

  def printHashSet[T](hs: immutable.HashSet[T], indent: Int = 0) {
    import immutable.HashSet._
    hs match {
      case t: HashTrieSet[_] =>
        println("%s%d)Trie\n".format(" " * indent, indent / 2))
        for (h <- t.elems) printHashSet(h, indent + 2)
      case t: HashSet1[_] =>
        println("%s%d)%s".format(" " * indent, indent / 2, t.iterator.next()))
      case _ =>
        println("default: " + hs)
    }
  }

}