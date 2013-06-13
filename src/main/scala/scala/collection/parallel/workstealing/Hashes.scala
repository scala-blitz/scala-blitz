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
    override def fold[U >: (K, V)](z: =>U)(op: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler) = macro methods.HashMapMacros.fold[K, V, U]
    def count[U >: (K, V)](p: U => Boolean)(implicit ctx: WorkstealingTreeScheduler): Int = macro methods.HashMapMacros.count[K, V, U]
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

  class HashMapMerger[@specialized(Int, Long) K: ClassTag, @specialized(Int, Long, Float, Double) V: ClassTag](
    val width: Int,
    val loadFactor: Int,
    val ctx: WorkstealingTreeScheduler
  ) extends HashBuckets[K, (K, V), HashMapMerger[K, V], Par[HashMap[K, V]]] {
    val keys = new Array[Conc.Buffer[K]](1 << width)
    val vals = new Array[Conc.Buffer[V]](1 << width)

    def newHashBucket = new HashMapMerger(width, loadFactor, ctx)

    def clearBucket(i: Int) {
      keys(i) = null
      vals(i) = null
    }

    def mergeBucket(idx: Int, that: HashMapMerger[K, V], res: HashMapMerger[K, V]) {
      val thisk = this.keys(idx)
      val thisv = this.vals(idx)
      val thatk = that.keys(idx)
      val thatv = that.vals(idx)
      if (thisk == null) {
        res.keys(idx) = thatk
        res.vals(idx) = thatv
      } else {
        thisk.prepareForMerge()
        thatk.prepareForMerge()
        res.keys(idx) = thisk merge thatk
        thisv.prepareForMerge()
        thatv.prepareForMerge()
        res.vals(idx) = thisv merge thatv
      }
    }

    def +=(kv: (K, V)): HashMapMerger[K, V] = this.put(kv._1, kv._2)

    def put(k: K, v: V): HashMapMerger[K, V] = {
      val kz = keys
      val vz = vals
      val hc = k.##
      val idx = hc >>> HashBuckets.IRRELEVANT_BITS
      var bkey = kz(idx)
      if (bkey eq null) {
        kz(idx) = new Conc.Buffer
        vz(idx) = new Conc.Buffer
        bkey = kz(idx)
      }
      val bval = vz(idx)
      bkey += k
      bval += v
      this
    }

    def result = {
      import Par._
      import Ops._
      val expectedSize = keys.foldLeft(0)(_ + _.size)
      val tableLength = HashTable.powerOfTwo((expectedSize.toLong * 1000 / loadFactor + 1).toInt)
      val table = new Array[HashEntry[K, DefaultEntry[K, V]]](tableLength)
      val stealer = (0 until keys.length).toPar.stealer
      val kernel = new HashMapMergerResultKernel(width, keys, vals, table)
      val size = ctx.invokeParallelOperation(stealer, kernel)
      val contents = new HashTable.Contents(
        loadFactor,
        table,
        size,
        HashTable.newThreshold(loadFactor, tableLength),
        HashBuckets.IRRELEVANT_BITS,
        null
      )
      (new HashMap(contents)).toPar
    }
  }

  class HashMapMergerResultKernel[@specialized(Int, Long) K, @specialized(Int, Long, Float, Double) V](
    val width: Int,
    val keys: Array[Conc.Buffer[K]],
    val vals: Array[Conc.Buffer[V]],
    val table: Array[HashEntry[K, DefaultEntry[K, V]]]
  ) extends IndexedStealer.IndexedKernel[Int, Int] {
    override def incrementStepFactor(config: WorkstealingTreeScheduler.Config) = 1
    def zero = 0
    def combine(a: Int, b: Int) = a + b
    def apply(node: Node[Int, Int], chunkSize: Int) = {
      val stealer = node.stealer.asInstanceOf[Ranges.RangeStealer]
      var i = stealer.nextProgress
      val until = stealer.nextUntil
      var total = 0
      while (i < until) {
        total += storeBucket(i)
        i += 1
      }
      total
    }
    private def storeBucket(i: Int): Int = {
      0
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





