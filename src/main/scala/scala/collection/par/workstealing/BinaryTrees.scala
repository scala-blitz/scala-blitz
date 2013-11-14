package scala.collection



import scala.collection.immutable.RedBlackTree



package immutable {

  import par._
  import workstealing._

  object RedBlackTreeStealer {
    val TREESET_OFFSET_TREE = unsafe.objectFieldOffset(classOf[immutable.TreeSet[_]].getDeclaredField("tree"))

    final def redBlackRoot[T](ts: immutable.TreeSet[T]) = unsafe.getObject(ts, TREESET_OFFSET_TREE).asInstanceOf[immutable.RedBlackTree.Tree[T, Unit]]
    
    abstract class RedBlackTreeIsBinary[T, K, V] extends BinaryTreeStealer.Binary[T, RedBlackTree.Tree[K, V]] {
      def sizeBound(total: Int, depth: Int): Int = {
        1 << depthBound(total, depth)
      }
      def depthBound(total: Int, depth: Int): Int = {
        math.max(2, (2.5 * math.log(total) / math.log(2) + 2).toInt - depth)
      }
      def isEmptyLeaf(n: RedBlackTree.Tree[K, V]): Boolean = n eq null
      def left(n: RedBlackTree.Tree[K, V]): RedBlackTree.Tree[K, V] = n.left
      def right(n: RedBlackTree.Tree[K, V]): RedBlackTree.Tree[K, V] = n.right
      def value(n: RedBlackTree.Tree[K, V]): T
    }

    def redBlackTreeSetIsBinary[K] = new RedBlackTreeIsBinary[K, K, Unit] {
      def value(n: RedBlackTree.Tree[K, Unit]) = n.key
    }
  }

}


package par {
  package workstealing {

    object BinaryTrees {
    
      trait Scope {
      }

    }

  }
}