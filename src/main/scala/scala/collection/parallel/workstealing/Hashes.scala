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
    implicit def canMergeHashMap[@specialized(Int, Long) K: ClassTag, @specialized(Int, Long, Float, Double) V: ClassTag](implicit ctx: WorkstealingTreeScheduler) = new CanMergeFrom[Par[HashMap[_, _]], (K, V), Par[HashMap[K, V]]] {
      def apply(from: Par[HashMap[_, _]]) = new HashMapMerger[K, V](HashBuckets.DISCRIMINANT_BITS, HashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, ctx)
      def apply() = new HashMapMerger[K, V](HashBuckets.DISCRIMINANT_BITS, HashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, ctx)
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
    override def aggregate[S](z: S)(combop: (S, S) => S)(seqop: (S, (K, V)) => S)(implicit ctx: WorkstealingTreeScheduler) = macro methods.HashMapMacros.aggregate[K, V, S]
    override def fold[U >: (K, V)](z: => U)(op: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler) = macro methods.HashMapMacros.fold[K, V, U]
    override def count[U >: (K, V)](p: U => Boolean)(implicit ctx: WorkstealingTreeScheduler): Int = macro methods.HashMapMacros.count[K, V, U]
    override def filter[That](pred: ((K, V)) => Boolean)(implicit cmf: CanMergeFrom[Par[HashMap[K, V]], ((K, V)), That], ctx: WorkstealingTreeScheduler) = macro methods.HashMapMacros.filter[K, V, That]
    override def mapReduce[R](mapper: ((K, V)) => R)(reducer: (R, R) => R)(implicit ctx: WorkstealingTreeScheduler): R = macro methods.HashMapMacros.mapReduce[K, V, R]
    override def reduce[U >: (K, V)](op: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler): U = macro methods.HashMapMacros.reduce[K, V, U]
    override def min[U >: (K, V)](implicit ord: Ordering[U], ctx: WorkstealingTreeScheduler): U = macro methods.HashMapMacros.min[K, V, U]
    override def max[U >: (K, V)](implicit ord: Ordering[U], ctx: WorkstealingTreeScheduler): U = macro methods.HashMapMacros.max[K, V, U]
    override def foreach[U >: (K, V)](action: U => Unit)(implicit ctx: WorkstealingTreeScheduler): Unit = macro methods.HashMapMacros.foreach[K, V, U]
    override def sum[U >: (K, V)](implicit num: Numeric[U], ctx: WorkstealingTreeScheduler): U = macro methods.HashMapMacros.sum[K, V, U]
    override def product[U >: (K, V)](implicit num: Numeric[U], ctx: WorkstealingTreeScheduler): U = macro methods.HashMapMacros.product[K, V, U]
    override def find[U >: (K, V)](p: U => Boolean)(implicit ctx: WorkstealingTreeScheduler): Option[(K, V)] = macro methods.HashMapMacros.find[K, V, U]
    override def exists[U >: (K, V)](p: U => Boolean)(implicit ctx: WorkstealingTreeScheduler): Boolean = macro methods.HashMapMacros.exists[K, V, U]
    override def forall[U >: (K, V)](p: U => Boolean)(implicit ctx: WorkstealingTreeScheduler): Boolean = macro methods.HashMapMacros.forall[K, V, U]

    def seq = hashmap
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

  def newHashMapMerger[@specialized(Int, Long) K <: AnyVal: ClassTag, @specialized(Int, Long, Float, Double) V <: AnyVal: ClassTag](callee: Par[HashMap[K, V]])(implicit ctx: WorkstealingTreeScheduler) = {
    new HashMapMerger[K, V](HashBuckets.DISCRIMINANT_BITS, HashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, ctx)
  }

  def newHashMapMerger[@specialized(Int, Long) K: ClassTag, @specialized(Int, Long, Float, Double) V: ClassTag](implicit ctx: WorkstealingTreeScheduler) = {
    new HashMapMerger[K, V](HashBuckets.DISCRIMINANT_BITS, HashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, ctx)
  }

  def newHashMapMerger[K, V](callee: Par[HashMap[K, V]])(implicit ctx: WorkstealingTreeScheduler) = {
    new HashMapMerger[Object, Object](HashBuckets.DISCRIMINANT_BITS, HashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, ctx).asInstanceOf[HashMapMerger[K, V]]
  }

  class HashMapMerger[@specialized(Int, Long) K: ClassTag, @specialized(Int, Long, Float, Double) V: ClassTag](
    val width: Int,
    val loadFactor: Int,
    val seed: Int,
    val ctx: WorkstealingTreeScheduler) extends HashBuckets[K, (K, V), HashMapMerger[K, V], Par[HashMap[K, V]]] with HashTable.HashUtils[K] {
    val keys = new Array[Conc.Buffer[K]](1 << width)
    val vals = new Array[Conc.Buffer[V]](1 << width)

    def newHashBucket = new HashMapMerger(width, loadFactor, seed, ctx)

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
      } else if (thatk == null) {
        res.keys(idx) = thisk
        res.vals(idx) = thisv
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
      val hc = improve(k.##, seed)
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

    def get(k: K): Option[(Conc.Chunk[V], Int)] = {
      val kz = keys
      val vz = vals
      val hc = improve(k.##, seed)
      val idx = hc >>> HashBuckets.IRRELEVANT_BITS
      var bkey = kz(idx)
      val bval = vz(idx)
      ???
    }

    def result = {
      import Par._
      import Ops._
      val expectedSize = keys.foldLeft(0) { (acc, x) =>
        if (x ne null) acc + x.size else acc
      }
      val table = new HashBuckets.DefaultEntries[K, V](width, expectedSize, loadFactor, seed)
      val stealer = (0 until keys.length).toPar.stealer
      val kernel = new HashMapMergerResultKernel(width, keys, vals, table)
      val size = ctx.invokeParallelOperation(stealer, kernel)
      table.setSize(size)
      (new HashMap(table.hashTableContents)).toPar
    }

    override def toString = "HashMapMerger(%s)".format(keys.mkString(", "))
  }

  class HashMapMergerResultKernel[@specialized(Int, Long) K, @specialized(Int, Long, Float, Double) V](
    val width: Int,
    val keys: Array[Conc.Buffer[K]],
    val vals: Array[Conc.Buffer[V]],
    val table: HashBuckets.DefaultEntries[K, V]) extends IndexedStealer.IndexedKernel[Int, Int] {
    override def incrementStepFactor(config: WorkstealingTreeScheduler.Config) = 1
    def zero = 0
    def combine(a: Int, b: Int) = a + b
    def apply(node: Node[Int, Int], chunkSize: Int): Int = {
      val stealer = node.stealer.asInstanceOf[Ranges.RangeStealer]
      var i = stealer.nextProgress
      val until = stealer.nextUntil
      var sum = 0
      while (i < until) {
        sum += storeBucket(i)
        i += 1
      }
      sum
    }
    private def storeBucket(i: Int): Int = {
      def traverse(ks: Conc[K], vs: Conc[V]): Int = ks match {
        case knode: Conc.<>[K] =>
          val vnode = vs.asInstanceOf[Conc.<>[V]]
          traverse(knode.left, vnode.left) + traverse(knode.right, vnode.right)
        case kchunk: Conc.Chunk[K] =>
          val kchunkarr = kchunk.elems
          val vchunkarr = vs.asInstanceOf[Conc.Chunk[V]].elems
          var i = 0
          var total = 0
          val until = kchunk.size
          while (i < until) {
            val k = kchunkarr(i)
            val v = vchunkarr(i)
            if (table.tryInsertEntry(k, v)) total += 1
            i += 1
          }
          total
        case _ =>
          sys.error("unreachable: " + ks)
      }

      if (keys(i) ne null) traverse(keys(i).result.normalized, vals(i).result.normalized) else 0
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

