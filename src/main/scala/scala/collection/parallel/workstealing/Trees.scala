package scala.collection.parallel.workstealing



import scala.reflect.ClassTag
import scala.collection.immutable.HashSet



object Trees {

  class HashTrieSetStealer[T](val root: HashSet[T]) extends {
    val classTag = implicitly[ClassTag[HashSet[T]]]
    val totalSize = root.size
  } with TreeStealer.External[T, HashSet[T]] {
    val chunkIterator = null

    var padding9: Int = 0
    var padding10: Int = 0
    var padding11: Int = 0
    var padding12: Int = 0
    var padding13: Int = 0
    var padding14: Int = 0
    var padding15: Int = 0

    final def resetIterator(n: HashSet[T]) = ???
    final def child(n: HashSet[T], idx: Int) = n.asInstanceOf[HashSet.HashTrieSet[T]].elems(idx - 1)
    final def elementAt(n: HashSet[T], idx: Int): T = ???
    final def depthBound(totalSize: Int): Int = 6
    final def isLeaf(n: HashSet[T]) = n match {
      case _: HashSet.HashTrieSet[_] => false
      case _ => true
    }
    final def estimateSubtree(n: HashSet[T], depth: Int, totalSize: Int) = n.size
    final def totalChildren(n: HashSet[T]) = n match {
      case n: HashSet.HashTrieSet[_] => n.elems.length
      case _ => 0
    }
  }

}
