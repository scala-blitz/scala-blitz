package scala.collection.parallel
package workstealing



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.reflect.ClassTag
import scala.collection.parallel.generic._
import scala.collection.immutable.HashSet
import scala.collection.immutable.TrieIterator



object Trees {

  import WorkstealingTreeScheduler.{ Kernel, Node }

  val TRIE_ITERATOR_NAME = classOf[TrieIterator[_]].getName.replace(".", "$")
  val HASHSET1_NAME = classOf[TrieIterator[_]].getName.replace(".", "$")

  def mangledT(x: String) = TRIE_ITERATOR_NAME + "$$" + x
  def mangledHS(x: String) = TRIE_ITERATOR_NAME + "$$" + x

  val OFFSET_DEPTH = unsafe.objectFieldOffset(classOf[TrieChunkIterator[_, _]].getField(mangledT("depth")))
  val OFFSET_ARRAY_STACK = unsafe.objectFieldOffset(classOf[TrieChunkIterator[_, _]].getField(mangledT("arrayStack")))
  val OFFSET_POS_STACK = unsafe.objectFieldOffset(classOf[TrieChunkIterator[_, _]].getField(mangledT("posStack")))
  val OFFSET_ARRAY_D = unsafe.objectFieldOffset(classOf[TrieChunkIterator[_, _]].getField(mangledT("arrayD")))
  val OFFSET_POS_D = unsafe.objectFieldOffset(classOf[TrieChunkIterator[_, _]].getField(mangledT("posD")))
  val OFFSET_SUBITER = unsafe.objectFieldOffset(classOf[TrieChunkIterator[_, _]].getField(mangledT("subIter")))
  val OFFSET_HS_KEY = unsafe.objectFieldOffset(classOf[HashSet.HashSet1[_]].getDeclaredField("key"))

  final def key[T](hs1: HashSet.HashSet1[T]) = unsafe.getObject(hs1, OFFSET_HS_KEY).asInstanceOf[T]

  abstract class TrieChunkIterator[T, Repr] extends TrieIterator[T](null) with TreeStealer.ChunkIterator[T] {
    final def setDepth(d: Int) = unsafe.putInt(this, OFFSET_DEPTH, d)
    final def setPosD(p: Int) = unsafe.putInt(this, OFFSET_POS_D, p)
    final def setArrayD(a: Array[Iterable[T]]) = unsafe.putObject(this, OFFSET_ARRAY_D, a)
    final def getArrayD = unsafe.getObject(this, OFFSET_ARRAY_D).asInstanceOf[Array[Iterable[T]]]
    final def getSubIter = unsafe.getObject(this, OFFSET_SUBITER).asInstanceOf[Iterator[T]]
    final def clearArrayStack() {
      val arrayStack = unsafe.getObject(this, OFFSET_ARRAY_STACK).asInstanceOf[Array[Array[Iterable[T]]]]
      var i = 0
      while (i < 6 && arrayStack(i) != null) {
        arrayStack(i) = null
        i += 1
      }
    }
    final def clearPosStack() {
     val posStack = unsafe.getObject(this, OFFSET_POS_STACK).asInstanceOf[Array[Int]]
     var i = 0
     while (i < 6) {
       posStack(i) = 0
       i += 1
     }
    }
    final def clearSubIter() = unsafe.putObject(this, OFFSET_SUBITER, null)
    final def root = getArrayD(0).asInstanceOf[Repr]
  }

  trait Scope {
    implicit def hashTrieSetOps[T](a: Par[HashSet[T]]) = new Trees.HashSetOps(a)
    implicit def canMergeHashTrieSet[T](implicit ctx: WorkstealingTreeScheduler) = new CanMergeFrom[Par[HashSet[_]], T, Par[HashSet[T]]] {
      def apply(from: Par[HashSet[_]]) = ???
      def apply() = ???
    }
    implicit def hashTrieSetIsReducable[T] = new IsReducable[HashSet[T], T] {
      def apply(pa: Par[HashSet[T]]) = ???
    }
  }

  class HashSetOps[T](val hashset: Par[HashSet[T]]) extends AnyVal with Reducables.OpsLike[T, Par[HashSet[T]]] {
    def stealer: Stealer[T] = {
      val s = new HashSetStealer(hashset.seq)
      s.rootInit()
      s
    }
    def aggregate[S](z: S)(combop: (S, S) => S)(seqop: (S, T) => S)(implicit ctx: WorkstealingTreeScheduler) = macro methods.HashTrieSetMacros.aggregate[T, S]
  }

  class HashSetStealer[T](val root: HashSet[T]) extends {
    val classTag = implicitly[ClassTag[HashSet[T]]]
    val totalSize = root.size
  } with TreeStealer.External[T, HashSet[T]] {
    val chunkIterator = new TrieChunkIterator[T, HashSet[T]] {
      final def getElem(x: AnyRef): T = {
        val hs1 = x.asInstanceOf[HashSet.HashSet1[T]]
        Trees.key(hs1)
      }
    }
    val leafArray = new Array[Iterable[T]](1)

    var padding10: Int = 0
    var padding11: Int = 0
    var padding12: Int = 0
    var padding13: Int = 0
    var padding14: Int = 0
    var padding15: Int = 0

    final def newStealer = new HashSetStealer(root)
    final def resetIterator(n: HashSet[T]): Unit = if (n.nonEmpty) {
      chunkIterator.setDepth(0)
      chunkIterator.setPosD(0)
      chunkIterator.clearArrayStack()
      chunkIterator.clearPosStack()
      chunkIterator.clearSubIter()
      leafArray(0) = n
      chunkIterator.setArrayD(leafArray)
    } else {
      chunkIterator.clearSubIter()
      chunkIterator.setDepth(-1)
    }
    final def child(n: HashSet[T], idx: Int) = {
      val trie = n.asInstanceOf[HashSet.HashTrieSet[T]]
      if (trie.elems.length > 1 || idx == 1) trie.elems(idx - 1)
      else if (idx == 2) HashSet.empty
      else sys.error("error state")
    }
    final def elementAt(n: HashSet[T], idx: Int): T = ???
    final def depthBound(totalSize: Int): Int = 6
    final def isLeaf(n: HashSet[T]) = n match {
      case _: HashSet.HashTrieSet[_] => false
      case _ => true
    }
    final def estimateSubtree(n: HashSet[T], depth: Int, totalSize: Int) = n.size
    final def totalChildren(n: HashSet[T]) = n match {
      case n: HashSet.HashTrieSet[_] =>
        val len = n.elems.length
        if (len == 1) 2 else len
      case _ =>
        0
    }
  }

  abstract class HashSetKernel[T, R] extends Kernel[T, R] {
    def apply(node: Node[T, R], chunkSize: Int): R = {
      val stealer = node.stealer.asInstanceOf[HashSetStealer[T]]
      apply(node, stealer.chunkIterator)
    }
    def apply(node: Node[T, R], ci: TrieChunkIterator[T, HashSet[T]]): R
  }

}
