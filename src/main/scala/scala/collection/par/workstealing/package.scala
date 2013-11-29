package scala.collection



import sun.misc.Unsafe
import scala.reflect.macros._


package immutable {

  import par._
  import workstealing._

  object RedBlackTreeStealer {
    val TREESET_OFFSET_TREE = unsafe.objectFieldOffset(classOf[immutable.TreeSet[_]].getDeclaredField("tree"))

    final def redBlackRoot[T](ts: immutable.TreeSet[T]) = unsafe.getObject(ts, TREESET_OFFSET_TREE).asInstanceOf[immutable.RedBlackTree.Tree[T, Unit]]
    
    abstract class RedBlackTreeIsBinary[T, K, V] extends BinaryTreeStealer.Binary[T, RedBlackTree.Tree[K, V]] {
      def sizeBound(total: Int, depth: Int): Int = {
        val avgdepth = (math.log(total) / math.log(2)).toInt + 1
        if (depth > avgdepth) 1
        else 1 << (avgdepth - depth)
      }
      def depthBound(total: Int, depth: Int): Int = {
        val depthBeneath = (2.5 * math.log(total) / math.log(2) + 2).toInt - depth
        math.min(30, math.max(2, depthBeneath))
      }
      def isEmptyLeaf(n: RedBlackTree.Tree[K, V]): Boolean = n eq null
      def left(n: RedBlackTree.Tree[K, V]): RedBlackTree.Tree[K, V] = n.left
      def right(n: RedBlackTree.Tree[K, V]): RedBlackTree.Tree[K, V] = n.right
      def value(n: RedBlackTree.Tree[K, V]): T
      def depth(n: RedBlackTree.Tree[K, V]): Int = if (n == null) 0 else 1 + math.max(depth(n.left), depth(n.right))
    }

    def redBlackTreeSetIsBinary[K] = new RedBlackTreeIsBinary[K, K, Unit] {
      def value(n: RedBlackTree.Tree[K, Unit]) = n.key
    }
  }

}


package par {

package workstealing {
  
  class ResultCell[@specialized T] {
    private var r: T = _
    private var empty = true
    def result: T = r
    def result_=(v: T) = {
      r = v
      empty = false
    }
    def isEmpty = empty
    override def toString = if (empty) "ResultCell(empty)" else "ResultCell(" + r + ")"
  }

  object ResultFound extends Scheduler.TerminationCause {
    def validateResult[R](r: R) = if (r.isInstanceOf[Option[_]]) r else ???
  }

  final case class ProgressStatus(val start: Int, var progress: Int)
  }

}

