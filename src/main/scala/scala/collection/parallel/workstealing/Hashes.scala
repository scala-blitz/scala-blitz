package scala.collection.parallel
package workstealing



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.reflect.ClassTag
import scala.collection.parallel.generic._
import scala.collection.mutable.HashTable
import scala.collection.mutable.HashMap
import scala.collection.mutable.FlatHashTable
import scala.collection.mutable.HashSet
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
    implicit def hashSetOps[T](a: Par[HashSet[T]]) = new Hashes.HashSetOps(a)
    implicit def canMergeHashSet[T: ClassTag](implicit ctx: WorkstealingTreeScheduler) = new CanMergeFrom[Par[HashSet[_]], T, Par[HashSet[T]]] {
      def apply(from: Par[HashSet[_]]) = new HashSetMerger[T](HashBuckets.DISCRIMINANT_BITS, FlatHashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, ctx)
      def apply() = new HashSetMerger[T](HashBuckets.DISCRIMINANT_BITS, FlatHashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, ctx)
    }
    implicit def canMergeIntHashSet(implicit ctx: WorkstealingTreeScheduler) = new CanMergeFrom[Par[HashSet[_]], Int, Par[HashSet[Int]]] {
      def apply(from: Par[HashSet[_]]) = new HashSetMerger[Int](HashBuckets.DISCRIMINANT_BITS, FlatHashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, ctx)
      def apply() = new HashSetMerger[Int](HashBuckets.DISCRIMINANT_BITS, FlatHashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, ctx)
    }
    implicit def hashSetIsReducable[T] = new IsReducable[HashSet[T], T] {
      def apply(pa: Par[HashSet[T]]) = ???
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
    def filter(pred: ((K, V)) => Boolean)(implicit ctx: WorkstealingTreeScheduler) = macro methods.HashMapMacros.filter[K, V]
  }

  class HashSetOps[T](val hashset: Par[HashSet[T]]) extends AnyVal with Reducables.OpsLike[T, Par[HashSet[T]]] {
    def stealer: Stealer[T] = {
      val contents = hashset.seq.hashTableContents
      new HashSetStealer(contents, 0, contents.table.length)
    }
    def aggregate[S](z: S)(combop: (S, S) => S)(seqop: (S, T) => S)(implicit ctx: WorkstealingTreeScheduler) = macro methods.HashSetMacros.aggregate[T, S]
    override def map[S, That](func: T => S)(implicit cmf: CanMergeFrom[Par[HashSet[T]], S, That], ctx: WorkstealingTreeScheduler): That = macro methods.HashSetMacros.map[T, S, That]
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

  final class HashSetStealer[T](val contents: FlatHashTable.Contents[T], si: Int, ei: Int) extends HashStealer[T](si, ei) {
    type StealerType = HashSetStealer[T]

    val table: Array[AnyRef] = contents.table
    var current: T = _

    var padding9: Int = _
    var padding10: Int = _
    var padding11: Int = _
    var padding12: Int = _
    var padding13: Int = _
    var padding14: Int = _
    var padding15: Int = _

    def newStealer(si: Int, ei: Int) = new HashSetStealer(contents, si, ei)

    protected def loadFactor: Int = contents.loadFactor

    protected def positionAt(idx: Int): Boolean = if (table(idx) == null) false else {
      current = table(idx).asInstanceOf[T]
      true
    }

    protected def hasNextAt = current != null

    @annotation.tailrec final def nextAt = if (current != null) {
      val curr = current
      current = null.asInstanceOf[T]
      curr
    } else {
      moveForward()
      nextAt
    }
  }

  def newHashMapMerger[@specialized(Int, Long) K <: AnyVal: ClassTag, @specialized(Int, Long, Float, Double) V <: AnyVal: ClassTag](callee: Par[HashMap[K, V]])(implicit ctx: WorkstealingTreeScheduler) = {
    new HashMapMerger[K, V](HashBuckets.DISCRIMINANT_BITS, HashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, ctx)
  }

  def newHashMapMerger[K, V](callee: Par[HashMap[K, V]])(implicit ctx: WorkstealingTreeScheduler) = {
    new HashMapMerger[Object, Object](HashBuckets.DISCRIMINANT_BITS, HashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, ctx).asInstanceOf[HashMapMerger[K, V]]
  }

  class HashMapMerger[@specialized(Int, Long) K: ClassTag, @specialized(Int, Long, Float, Double) V: ClassTag](
    val width: Int,
    val loadFactor: Int,
    val seed: Int,
    val ctx: WorkstealingTreeScheduler
  ) extends HashBuckets[K, (K, V), HashMapMerger[K, V], Par[HashMap[K, V]]] with HashTable.HashUtils[K] {
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
    val table: HashBuckets.DefaultEntries[K, V]
  ) extends IndexedStealer.IndexedKernel[Int, Int] {
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

  def newHashSetMerger[@specialized(Int, Long) T <: AnyVal: ClassTag](callee: Par[HashSet[T]])(implicit ctx: WorkstealingTreeScheduler) = {
    new HashSetMerger[T](HashBuckets.DISCRIMINANT_BITS, FlatHashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, ctx)
  }

  def newHashSetMerger[T](callee: Par[HashSet[T]])(implicit ctx: WorkstealingTreeScheduler) = {
    new HashSetMerger[Object](HashBuckets.DISCRIMINANT_BITS, FlatHashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, ctx).asInstanceOf[HashSetMerger[T]]
  }

  class HashSetMerger[@specialized(Int, Long) T: ClassTag](
    val width: Int,
    val loadFactor: Int,
    val seed: Int,
    val ctx: WorkstealingTreeScheduler
  ) extends HashBuckets[T, T, HashSetMerger[T], Par[HashSet[T]]] with FlatHashTable.HashUtils[T] {
    val keys = new Array[Conc.Buffer[T]](1 << width)

    def newHashBucket = new HashSetMerger(width, loadFactor, seed, ctx)

    def clearBucket(idx: Int) {
      keys(idx) = null
    }

    def mergeBucket(idx: Int, that: HashSetMerger[T], res: HashSetMerger[T]) {
      val thisk = this.keys(idx)
      val thatk = that.keys(idx)
      if (thisk == null) {
        res.keys(idx) = thatk
      } else if (thatk == null) {
        res.keys(idx) = thisk
      } else {
        thisk.prepareForMerge()
        thatk.prepareForMerge()
        res.keys(idx) = thisk merge thatk
      }
    }

    def +=(k: T): HashSetMerger[T] = {
      val kz = keys
      val hc = improve(k.##, seed)
      val idx = hc >>> HashBuckets.IRRELEVANT_BITS
      var bkey = kz(idx)
      if (bkey eq null) {
        kz(idx) = new Conc.Buffer
        bkey = kz(idx)
      }
      bkey += k
      this
    }

    def result = {
      import Par._
      import Ops._
      val expectedSize = keys.foldLeft(0) { (acc, x) =>
        if (x ne null) acc + x.size else acc
      }
      val table = new HashBuckets.FlatEntries[T](width, expectedSize, loadFactor, seed)
      val stealer = (0 until keys.length).toPar.stealer
      val kernel = new HashSetMergerResultKernel(width, keys, table)
      val size = ctx.invokeParallelOperation(stealer, kernel)
      table.setSize(size)
      val hs = new HashSet(table.hashTableContents)
      for (elem <- table.spills) hs += elem
      hs.toPar
    }

    override def toString = "HashSetMerger(%s)".format(keys.mkString(", "))
  }

  class HashSetMergerResultKernel[@specialized(Int, Long) T](
    val width: Int,
    val keys: Array[Conc.Buffer[T]],
    val table: HashBuckets.FlatEntries[T]
  ) extends IndexedStealer.IndexedKernel[Int, Int] {
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
    private val blocksize = table.tableLength >> HashBuckets.DISCRIMINANT_BITS
    private def blockStart(block: Int) = block * blocksize
    private def storeBucket(i: Int): Int = {
      val nextBlockStart = blockStart(i + 1)

      def traverse(ks: Conc[T]): Int = ks match {
        case knode: Conc.<>[T] =>
          traverse(knode.left) + traverse(knode.right)
        case kchunk: Conc.Chunk[T] =>
          val kchunkarr = kchunk.elems
          var i = 0
          var total = 0
          val until = kchunk.size
          while (i < until) {
            val k = kchunkarr(i)
            total += table.tryInsertEntry(-1, nextBlockStart, k)
            i += 1
          }
          total
        case _ =>
          sys.error("unreachable: " + ks)
      }

      if (keys(i) ne null) traverse(keys(i).result.normalized) else 0
    }
  }

  abstract class HashSetKernel[T, R] extends IndexedStealer.IndexedKernel[T, R] {
    def apply(node: Node[T, R], chunkSize: Int): R = {
      val stealer = node.stealer.asInstanceOf[HashSetStealer[T]]
      apply(node, stealer.nextProgress, stealer.nextUntil)
    }
    def apply(node: Node[T, R], from: Int, until: Int): R
  }

}





