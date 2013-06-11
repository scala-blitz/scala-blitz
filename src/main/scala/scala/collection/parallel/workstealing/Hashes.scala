package scala.collection.parallel
package workstealing



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.reflect.ClassTag
import scala.collection.parallel.generic._
import scala.collection.mutable.HashTable
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashEntry
import scala.collection.mutable.DefaultEntry



object Hashes {

  import WorkstealingTreeScheduler.{ Kernel, Node }

  trait Scope {
    implicit def hashMapOps[K, V](a: Par[HashMap[K, V]]) = new Hashes.HashMapOps(a)
    implicit def canMergeHashMap[K, V](implicit ctx: WorkstealingTreeScheduler) = new CanMergeFrom[Par[HashMap[_, _]], (K, V), Par[HashMap[K, V]]] {
      def apply(from: Par[HashMap[_, _]]) = ???
      def apply() = ???
    }
    implicit def hashMapIsReducable[K, V] = new IsReducable[HashMap[K, V], (K, V)] {
      def apply(pa: Par[HashMap[K, V]]) = ???
    }
  }

  class HashMapOps[K, V](val hashmap: Par[HashMap[K, V]]) extends AnyVal with Reducables.OpsLike[(K, V), Par[HashMap[K, V]]] {
    def stealer: Stealer[(K, V)] = {
      val contents = hashmap.seq.hashTableContents
      new HashMapStealer(contents, 0, contents.table.length)
    }
    def aggregate[S](z: S)(combop: (S, S) => S)(seqop: (S, (K, V)) => S)(implicit ctx: WorkstealingTreeScheduler) = macro methods.HashMapMacros.aggregate[K, V, S]
  }

  abstract class HashStealer[T](si: Int, ei: Int) extends IndexedStealer[T](si, ei) {
    type StealerType <: HashStealer[T]

    def elementsRemainingEstimate = (indicesRemaining.toLong * loadFactor / 1000).toInt

    protected def loadFactor: Int

    protected def positionAt(idx: Int): Boolean

    protected def hasNextAt: Boolean

    protected def nextAt: T

    def moveForward() {
      var i = nextProgress
      while (i < nextUntil) {
        if (positionAt(i)) {
          nextProgress = i + 1
          i = nextUntil
        } else i += 1
      }
    }

    def hasNext: Boolean = if (hasNextAt) true else {
      moveForward()
      hasNextAt
    }

    def next(): T = if (hasNext) nextAt else throw new NoSuchElementException

    def split: (HashStealer[T], HashStealer[T]) = splitAtIndex(indicesRemaining / 2)
  }

  final class HashMapStealer[K, V](val contents: HashTable.Contents[K, DefaultEntry[K, V]], si: Int, ei: Int) extends HashStealer[(K, V)](si, ei) {
    type StealerType = HashMapStealer[K, V]

    val table: Array[HashEntry[K, DefaultEntry[K, V]]] = contents.table
    var entry: HashEntry[K, DefaultEntry[K, V]] = null

    var padding10: Int = _
    var padding11: Int = _
    var padding12: Int = _
    var padding13: Int = _
    var padding14: Int = _
    var padding15: Int = _

    def newStealer(si: Int, ei: Int) = new HashMapStealer(contents, si, ei)

    protected def loadFactor: Int = contents.loadFactor

    protected def positionAt(idx: Int): Boolean = if (table(idx) == null) false else {
      entry = table(idx)
      true
    }

    protected def hasNextAt = entry != null

    @annotation.tailrec final def nextAt = if (entry != null) {
      val curr = entry.asInstanceOf[DefaultEntry[K, V]]
      entry = entry.next
      (curr.key, curr.value)
    } else {
      moveForward()
      nextAt
    }

  }

  abstract class HashMapKernel[K, V, R] extends IndexedStealer.IndexedKernel[(K, V), R] {
    def apply(node: Node[(K, V), R], chunkSize: Int): R = {
      val stealer = node.stealer.asInstanceOf[HashMapStealer[K, V]]
      apply(node, stealer.nextProgress, stealer.nextUntil)
    }
    def apply(node: Node[(K, V), R], from: Int, until: Int): R
  }

}





