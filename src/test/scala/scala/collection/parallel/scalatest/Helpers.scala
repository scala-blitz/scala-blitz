package scala.collection.parallel
package scalatest

import org.scalatest._
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._
import scala.collection._

trait Helpers {

  def printHashSet[T](hs: immutable.HashSet[T]) = println(hashSetPrettyString(hs))

  def hashSetPrettyString[T](hs: immutable.HashSet[T], indent: Int = 0): String = {
    import immutable.HashSet._
    val sb: StringBuilder = new StringBuilder()
    def apply(hs: immutable.HashSet[T], indent: Int)  {
      hs match {
        case t: HashTrieSet[_] =>
          sb ++= "%s%d)Trie\n".format(" " * indent, indent / 2)
          for (h <- t.elems) apply(h, indent + 2)
        case t: HashSet1[_] =>
          sb ++= "%s%d)%s\n".format(" " * indent, indent / 2, t.iterator.next())
        case _ =>
          sb ++= "default: " + hs + "\n"
      }
    }
    apply(hs, indent)
    sb.toString
  }

}
