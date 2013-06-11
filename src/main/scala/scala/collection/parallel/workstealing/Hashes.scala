package scala.collection.parallel
package workstealing



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.reflect.ClassTag
import scala.collection.parallel.generic._
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

  class HashMapOps[K, V](val array: Par[HashMap[K, V]]) extends AnyVal with Reducables.OpsLike[(K, V), Par[HashMap[K, V]]] {
    def stealer: Stealer[(K, V)] = ???
    def aggregate[S](z: S)(combop: (S, S) => S)(seqop: (S, (K, V)) => S)(implicit ctx: WorkstealingTreeScheduler) = ???
  }

  abstract class HashStealer[T](si: Int, ei: Int) extends IndexedStealer[T](si, ei) {
    type StealerType <: HashStealer[T]

    def elementsRemainingEstimate = indicesRemaining

    protected def positionAt(idx: Int): Boolean

    protected def hasNextAt: Boolean

    protected def nextAt: T

    def advance() {
      var i = nextProgress + 1
      while (i < nextUntil) {
        if (positionAt(i)) {
          nextProgress = i
          i = nextUntil
        } else i += 1
      }
    }

    def hasNext: Boolean = if (hasNextAt) true else {
      advance()
      hasNextAt
    }

    def next(): T = nextAt

    def split: (HashStealer[T], HashStealer[T]) = splitAtIndex(elementsRemainingEstimate / 2) 

    advance()
  }

  final class HashMapStealer[K, V](val table: Array[HashEntry[K, DefaultEntry[K, V]]], si: Int, ei: Int) extends HashStealer[(K, V)](si, ei) {
    type StealerType = HashMapStealer[K, V]

    var entry: HashEntry[K, DefaultEntry[K, V]] = _

    var padding9: Int = _
    var padding10: Int = _
    var padding11: Int = _
    var padding12: Int = _
    var padding13: Int = _
    var padding14: Int = _
    var padding15: Int = _

    def newStealer(si: Int, ei: Int) = new HashMapStealer(table, si, ei)

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
      advance()
      nextAt
    }

  }

}





