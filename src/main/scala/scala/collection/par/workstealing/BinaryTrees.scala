package scala.collection.par
package workstealing



import scala.collection.parallel.Splitter
import scala.collection.immutable.TreeSet
import generic._



object BinaryTrees {

  trait Scope {

    implicit def treeSetOps[T](ts: Par[TreeSet[T]]) = new TreeSetOps(ts)

    implicit def immutableTreeSetIsReducible[T] = new IsReducible[TreeSet[T], T] {
      def apply(pts: Par[TreeSet[T]]) = new Reducible[T] {
        def iterator: Iterator[T] = pts.seq.iterator
        def splitter: Splitter[T] = ???
        def stealer: Stealer[T] = pts.stealer
      }
    }

  }

  class TreeSetOps[T](val treeset: Par[TreeSet[T]]) extends AnyVal with Reducibles.OpsLike[T, Par[TreeSet[T]]] {
    def stealer: Stealer[T] = {
      val root = scala.collection.immutable.RedBlackTreeStealer.redBlackRoot(treeset.seq)
      val binary = scala.collection.immutable.RedBlackTreeStealer.redBlackTreeSetIsBinary[T]
      new workstealing.BinaryTreeStealer(root, 0, treeset.seq.size, binary)
    }
    def seq = treeset
  }

}
