package scala.collection.par
package workstealing




import scala.language.experimental.macros
import scala.reflect.macros._
import scala.reflect.ClassTag
import scala.collection.par.generic._
import scala.collection.mutable.HashTable
import scala.collection.mutable.HashMap
import scala.collection.mutable.FlatHashTable
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashEntry
import scala.collection.mutable.DefaultEntry




object HashTables {

  import Scheduler.{ Kernel, Node }

  trait Scope {
    implicit def hashMapOps[K, V](a: Par[HashMap[K, V]]) = new HashTables.HashMapOps(a)
    implicit def canMergeHashMap[@specialized(Int, Long) K: ClassTag, @specialized(Int, Long, Float, Double) V: ClassTag](implicit ctx: Scheduler) = new CanMergeFrom[Par[HashMap[_, _]], (K, V), Par[HashMap[K, V]]] {
      def apply(from: Par[HashMap[_, _]]) = new HashMapDiscardingMerger[K, V](HashBuckets.DISCRIMINANT_BITS, HashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, ctx)
      def apply() = new HashMapDiscardingMerger[K, V](HashBuckets.DISCRIMINANT_BITS, HashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, ctx)
    }
    implicit def hashMapIsReducable[K, V] = new IsReducable[HashMap[K, V], (K, V)] {
      def apply(pa: Par[HashMap[K, V]]) = new Reducable[(K, V)] {
        def iterator: Iterator[(K, V)] = pa.seq.iterator


        def stealer: Stealer[(K, V)] = pa.stealer
      }
    }
    implicit def hashSetOps[T](a: Par[HashSet[T]]) = new HashTables.HashSetOps(a)
    implicit def canMergeHashSet[T: ClassTag](implicit ctx: Scheduler) = new CanMergeFrom[Par[HashSet[_]], T, Par[HashSet[T]]] {
      def apply(from: Par[HashSet[_]]) = new HashSetMerger[T](HashBuckets.DISCRIMINANT_BITS, FlatHashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, ctx)
      def apply() = new HashSetMerger[T](HashBuckets.DISCRIMINANT_BITS, FlatHashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, ctx)
    }
    implicit def canMergeIntHashSet(implicit ctx: Scheduler) = new CanMergeFrom[Par[HashSet[_]], Int, Par[HashSet[Int]]] {
      def apply(from: Par[HashSet[_]]) = new HashSetMerger[Int](HashBuckets.DISCRIMINANT_BITS, FlatHashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, ctx)
      def apply() = new HashSetMerger[Int](HashBuckets.DISCRIMINANT_BITS, FlatHashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, ctx)
    }
    implicit def hashSetIsReducable[T] = new IsReducable[HashSet[T], T] {
      def apply(pa: Par[HashSet[T]]) = new Reducable[T] {
        def iterator: Iterator[T] = pa.seq.iterator


        def stealer: Stealer[T] = pa.stealer
      }
    }
  }

  class HashMapOps[K, V](val hashmap: Par[HashMap[K, V]]) extends AnyVal with Reducables.OpsLike[(K, V), Par[HashMap[K, V]]] {
    def stealer: Stealer[(K, V)] = {
      val contents = hashmap.seq.hashTableContents
      new HashMapStealer(contents, 0, contents.table.length)
    }
    override def aggregate[S](z: S)(combop: (S, S) => S)(seqop: (S, (K, V)) => S)(implicit ctx: Scheduler) = macro internal.HashMapMacros.aggregate[K, V, S]
    override def fold[U >: (K, V)](z: => U)(op: (U, U) => U)(implicit ctx: Scheduler) = macro internal.HashMapMacros.fold[K, V, U]
    override def count[U >: (K, V)](p: U => Boolean)(implicit ctx: Scheduler): Int = macro internal.HashMapMacros.count[K, V, U]
    override def filter[That](pred: ((K, V)) => Boolean)(implicit cmf: CanMergeFrom[Par[HashMap[K, V]], ((K, V)), That], ctx: Scheduler) = macro internal.HashMapMacros.filter[K, V, That]
    override def mapReduce[R](mapper: ((K, V)) => R)(reducer: (R, R) => R)(implicit ctx: Scheduler): R = macro internal.HashMapMacros.mapReduce[K, V, R]
    override def reduce[U >: (K, V)](op: (U, U) => U)(implicit ctx: Scheduler): U = macro internal.HashMapMacros.reduce[K, V, U]
    override def min[U >: (K, V)](implicit ord: Ordering[U], ctx: Scheduler): U = macro internal.HashMapMacros.min[K, V, U]
    override def max[U >: (K, V)](implicit ord: Ordering[U], ctx: Scheduler): U = macro internal.HashMapMacros.max[K, V, U]
    override def foreach[U >: (K, V)](action: U => Unit)(implicit ctx: Scheduler): Unit = macro internal.HashMapMacros.foreach[K, V, U]
    override def sum[U >: (K, V)](implicit num: Numeric[U], ctx: Scheduler): U = macro internal.HashMapMacros.sum[K, V, U]
    override def product[U >: (K, V)](implicit num: Numeric[U], ctx: Scheduler): U = macro internal.HashMapMacros.product[K, V, U]
    override def find[U >: (K, V)](p: U => Boolean)(implicit ctx: Scheduler): Option[(K, V)] = macro internal.HashMapMacros.find[K, V, U]
    override def exists[U >: (K, V)](p: U => Boolean)(implicit ctx: Scheduler): Boolean = macro internal.HashMapMacros.exists[K, V, U]
    override def forall[U >: (K, V)](p: U => Boolean)(implicit ctx: Scheduler): Boolean = macro internal.HashMapMacros.forall[K, V, U]
    override def map[T, That](mp: ((K, V)) => T)(implicit cmf: CanMergeFrom[Par[HashMap[K, V]], T, That], ctx: Scheduler): That = macro internal.HashMapMacros.map[K, V, T, That]
    override def flatMap[T, That](mp: ((K, V)) => TraversableOnce[T])(implicit cmf: CanMergeFrom[Par[HashMap[K, V]], T, That], ctx: Scheduler) = macro internal.HashMapMacros.flatMap[K, V, T, That]

    def seq = hashmap
  }

  class HashSetOps[T](val hashset: Par[HashSet[T]]) extends AnyVal with Reducables.OpsLike[T, Par[HashSet[T]]] {
    def stealer: Stealer[T] = {
      val contents = hashset.seq.hashTableContents
      new HashSetStealer(contents, 0, contents.table.length)
    }
    override def aggregate[S](z: S)(combop: (S, S) => S)(seqop: (S, T) => S)(implicit ctx: Scheduler) = macro internal.HashSetMacros.aggregate[T, S]
    override def foreach[U >: T](action: U => Unit)(implicit ctx: Scheduler): Unit = macro internal.HashSetMacros.foreach[T, U]
    override def mapReduce[R](mapper: T => R)(reducer: (R, R) => R)(implicit ctx: Scheduler): R = macro internal.HashSetMacros.mapReduce[T, R]
    override def reduce[U >: T](op: (U, U) => U)(implicit ctx: Scheduler) = macro internal.HashSetMacros.reduce[T, U]
    override def fold[U >: T](z: => U)(op: (U, U) => U)(implicit ctx: Scheduler): U = macro internal.HashSetMacros.fold[T, U]
    override def sum[U >: T](implicit num: Numeric[U], ctx: Scheduler): U = macro internal.HashSetMacros.sum[T, U]
    override def product[U >: T](implicit num: Numeric[U], ctx: Scheduler): U = macro internal.HashSetMacros.product[T, U]
    override def min[U >: T](implicit ord: Ordering[U], ctx: Scheduler): U = macro internal.HashSetMacros.min[T, U]
    override def max[U >: T](implicit ord: Ordering[U], ctx: Scheduler): U = macro internal.HashSetMacros.max[T, U]
    override def count[U >: T](p: U => Boolean)(implicit ctx: Scheduler): Int = macro internal.HashSetMacros.count[T, U]
    override def find[U >: T](pred: U => Boolean)(implicit ctx: Scheduler): Option[T] = macro internal.HashSetMacros.find[T, U]
    override def exists[U >: T](p: U => Boolean)(implicit ctx: Scheduler): Boolean = macro internal.HashSetMacros.exists[T, U]
    override def forall[U >: T](p: U => Boolean)(implicit ctx: Scheduler): Boolean = macro internal.HashSetMacros.forall[T, U]

    override def filter[That](pred: T => Boolean)(implicit cmf: CanMergeFrom[Par[HashSet[T]], T, That], ctx: Scheduler) = macro internal.HashSetMacros.filter[T, That]
    override def flatMap[S, That](func: T => TraversableOnce[S])(implicit cmf: CanMergeFrom[Par[HashSet[T]], S, That], ctx: Scheduler) = macro internal.HashSetMacros.flatMap[T, S, That]

    override def map[S, That](func: T => S)(implicit cmf: CanMergeFrom[Par[HashSet[T]], S, That], ctx: Scheduler): That = macro internal.HashSetMacros.map[T, S, That]
    def seq = hashset
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

  def newHashMapMerger[@specialized(Int, Long) K <: AnyVal: ClassTag, @specialized(Int, Long, Float, Double) V <: AnyVal: ClassTag](callee: Par[HashMap[K, V]])(implicit ctx: Scheduler) = {
    new HashMapDiscardingMerger[K, V](HashBuckets.DISCRIMINANT_BITS, HashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, ctx)
  }

  def newHashMapMerger[@specialized(Int, Long) K: ClassTag, @specialized(Int, Long, Float, Double) V: ClassTag](implicit ctx: Scheduler) = {
    new HashMapDiscardingMerger[K, V](HashBuckets.DISCRIMINANT_BITS, HashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, ctx)
  }

  def newHashMapMerger[K, V](callee: Par[HashMap[K, V]])(implicit ctx: Scheduler) = 
    new HashMapDiscardingMerger[Object, Object](HashBuckets.DISCRIMINANT_BITS, HashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, ctx).asInstanceOf[HashMapDiscardingMerger[K, V]]


  def newHashMapCombiningMerger[@specialized(Int, Long) K <: AnyVal: ClassTag, @specialized(Int, Long, Float, Double) V <: AnyVal: ClassTag](callee: Par[HashMap[K, V]], valueCombiner: (V, V) => V)(implicit ctx: Scheduler) = {
    new HashMapCombiningMerger[K, V](HashBuckets.DISCRIMINANT_BITS, HashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, valueCombiner, ctx)
  }

  def newHashMapCombiningMerger[K, V <: AnyRef](callee: Par[HashMap[K, V]], valueCombiner: (V, V) => V)(implicit ctx: Scheduler) =  {
    def castingCombiner(a: Object, b:Object ):Object = valueCombiner(a.asInstanceOf[V], b.asInstanceOf[V])
    new HashMapCombiningMerger[Object, Object](HashBuckets.DISCRIMINANT_BITS, HashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, castingCombiner, ctx)
  }

  def newHashMapCombiningMerger[@specialized(Int, Long) K: ClassTag, @specialized(Int, Long, Float, Double) V: ClassTag](implicit ctx: Scheduler, valueCombiner: (V, V) => V) = {
    new HashMapCombiningMerger[K, V](HashBuckets.DISCRIMINANT_BITS, HashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, valueCombiner, ctx)
  }

  def newHashMapCollectingMerger[@specialized(Int, Long) K <: AnyVal: ClassTag, @specialized(Int, Long, Float, Double) V <: AnyVal: ClassTag, That <: AnyRef, Repr](callee: Par[HashMap[K, V]])(implicit ctx: Scheduler, cmf: CanMergeFrom[Repr, V, That]) = {
    new HashMapCollectingMerger[K, V, That, Repr](HashBuckets.DISCRIMINANT_BITS, HashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, cmf, ctx)
  }

    def newHashMapCollectingMerger[@specialized(Int, Long) K: ClassTag, @specialized(Int, Long, Float, Double) V: ClassTag, That <:AnyRef, Repr](implicit ctx: Scheduler, cmf:CanMergeFrom[Repr, V, That]) = {
    new HashMapCollectingMerger[K, V, That, Repr](HashBuckets.DISCRIMINANT_BITS, HashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, cmf, ctx)
  }


  /**
   * Discards values for already existing keys
   */
  class HashMapDiscardingMerger[@specialized(Int, Long) K: ClassTag, @specialized(Int, Long, Float, Double) V: ClassTag](
    val width: Int,
    val loadFactor: Int,
    val seed: Int,
    val ctx: Scheduler) extends HashBuckets[K, (K, V), HashMapDiscardingMerger[K, V], Par[HashMap[K, V]]] with HashTable.HashUtils[K] {
    val keys = new Array[Conc.Buffer[K]](1 << width)
    val vals = new Array[Conc.Buffer[V]](1 << width)

    def clearBucket(i: Int) {
      keys(i) = null
      vals(i) = null
    }

    def mergeBucket(idx: Int, that: HashMapDiscardingMerger[K, V], res: HashMapDiscardingMerger[K, V]) {
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

    def +=(kv: (K, V)): HashMapDiscardingMerger[K, V] = this.put(kv._1, kv._2)
    def put(k: K, v: V) = {
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
    def resultKernel(target: HashBuckets.DefaultEntries[K, V]) = new HashMapSimpleMergerResultKernel(width, keys, vals, target)
    def newHashBucket = new HashMapDiscardingMerger(width, loadFactor, seed, ctx)

    def result = {
      val expectedSize = keys.foldLeft(0) { (acc, x) =>
        if (x ne null) acc + x.size else acc
      }
      val table = new HashBuckets.DefaultEntries[K, V](width, expectedSize, loadFactor, seed)
      val stealer = (0 until keys.length).toPar.stealer
      val kernel = resultKernel(table)
      val size = ctx.invokeParallelOperation(stealer, kernel)
      table.setSize(size)
      (new HashMap(table.hashTableContents)).toPar
    }
    override def toString = "HashMapDiscardingMerger(%s)".format(keys.mkString(", "))

  }

  class HashMapCollectingMerger[@specialized(Int, Long) K: ClassTag, @specialized(Int, Long, Float, Double) V: ClassTag, That <: AnyRef, Repr](
    val width: Int,
    val loadFactor: Int,
    val seed: Int,
    val cmf: CanMergeFrom[Repr, V, That],
    val ctx: Scheduler) extends HashBuckets[K, (K, V), HashMapCollectingMerger[K, V, That, Repr], Par[HashMap[K, That]]] with HashTable.HashUtils[K] {

    val keys = new Array[Conc.Buffer[K]](1 << width)
    val vals = new Array[Conc.Buffer[V]](1 << width)
    def clearBucket(i: Int) {
      keys(i) = null
      vals(i) = null
    }

    def mergeBucket(idx: Int, that: HashMapCollectingMerger[K, V, That, Repr], res: HashMapCollectingMerger[K, V, That, Repr]) {
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

    def +=(kv: (K, V)): HashMapCollectingMerger[K, V, That, Repr] = this.put(kv._1, kv._2)
    def put(k: K, v: V) = {
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

    def newHashBucket = new HashMapCollectingMerger(width, loadFactor, seed, cmf , ctx)

    def result : Par[HashMap[K,That]] = {
      val expectedSize = keys.foldLeft(0) { (acc, x) =>
        if (x ne null) acc + x.size else acc
      }
      val table = new HashBuckets.DefaultEntries[K, Repr](width, expectedSize, loadFactor, seed)
      val stealer = (0 until keys.length).toPar.stealer
      val kernel = new HashMapCollectingMergerResultKernel(width, keys, vals, table.asInstanceOf[HashBuckets.DefaultEntries[K, Object]], cmf)
      val size = ctx.invokeParallelOperation(stealer, kernel)
      table.setSize(size)
      val resultKernel = new HashMapCollectingMergerCallResultKernel[K, V, That]
      val res = new HashMap(table.hashTableContents)
      ctx.invokeParallelOperation[(K, Object), Unit](res.toPar.stealer.asInstanceOf[Stealer[(K, Object)]], resultKernel)
      res.asInstanceOf[HashMap[K, That]].toPar
    }

  }

  class HashMapCombiningMerger[@specialized(Int, Long) K: ClassTag, @specialized(Int, Long, Float, Double) V: ClassTag](
    val width: Int,
    val loadFactor: Int,
    val seed: Int,
    val valueCombiner: (V, V) => V,
    val ctx: Scheduler) extends HashBuckets[K, (K, V), HashMapCombiningMerger[K, V], Par[HashMap[K, V]]] with HashTable.HashUtils[K] {

    val keys = new Array[Conc.Buffer[K]](1 << width)
    val vals = new Array[Conc.Buffer[V]](1 << width)
    def clearBucket(i: Int) {
      keys(i) = null
      vals(i) = null
    }

    def mergeBucket(idx: Int, that: HashMapCombiningMerger[K, V], res: HashMapCombiningMerger[K, V]) {
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

    def +=(kv: (K, V)): HashMapCombiningMerger[K, V] = this.put(kv._1, kv._2)
    def put(k: K, v: V) = {
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

    def resultKernel(target: HashBuckets.DefaultEntries[K, V]) = new HashMapCombiningMergerResultKernel(width, keys, vals, target, valueCombiner)
    def newHashBucket = new HashMapCombiningMerger(width, loadFactor, seed, valueCombiner, ctx)

    def result = {
      val expectedSize = keys.foldLeft(0) { (acc, x) =>
        if (x ne null) acc + x.size else acc
      }
      val table = new HashBuckets.DefaultEntries[K, V](width, expectedSize, loadFactor, seed)
      val stealer = (0 until keys.length).toPar.stealer
      val kernel = resultKernel(table)
      val size = ctx.invokeParallelOperation(stealer, kernel)
      table.setSize(size)
      (new HashMap(table.hashTableContents)).toPar
    }

  }


  trait HashMapMergerResultKernel[@specialized(Int, Long) K, @specialized(Int, Long, Float, Double) V]
    extends IndexedStealer.IndexedKernel[Int, Int] {
    val width: Int
    val keys: Array[Conc.Buffer[K]]
    val vals: Array[Conc.Buffer[V]]
    override def incrementStepFactor(config: Scheduler.Config) = 1
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
    def insertEntry(key: K, value: V): Boolean
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
            if (insertEntry(k, v)) total += 1
            i += 1
          }
          total
        case _ =>
          sys.error("unreachable: " + ks)
      }

      if (keys(i) ne null) traverse(keys(i).result.normalized, vals(i).result.normalized) else 0
    }
  }

  class HashMapSimpleMergerResultKernel[@specialized(Int, Long) K, @specialized(Int, Long, Float, Double) V](
    val width: Int,
    val keys: Array[Conc.Buffer[K]],
    val vals: Array[Conc.Buffer[V]],
    val table: HashBuckets.DefaultEntries[K, V]) extends HashMapMergerResultKernel[K, V] {
    def insertEntry(key: K, value: V) = table.tryInsertEntry(key, value)
  }

  class HashMapCombiningMergerResultKernel[@specialized(Int, Long) K, @specialized(Int, Long, Float, Double) V](
    val width: Int,
    val keys: Array[Conc.Buffer[K]],
    val vals: Array[Conc.Buffer[V]],
    val table: HashBuckets.DefaultEntries[K, V],
    val valueCombiner: (V, V) => V) extends HashMapMergerResultKernel[K, V] {
    def insertEntry(key: K, value: V) = table.tryCombineEntry(key, value, valueCombiner)
  }

  class HashMapCollectingMergerResultKernel[@specialized(Int, Long) K, @specialized(Int, Long, Float, Double) V, That, Repr](
    val width: Int,
    val keys: Array[Conc.Buffer[K]],
    val vals: Array[Conc.Buffer[V]],
    val table: HashBuckets.DefaultEntries[K, Object],
    val cmf: CanMergeFrom[Repr, V, That])
 extends HashMapMergerResultKernel[K, V] {
    def valueCombiner (merger: Object, value: V) = {
    merger.asInstanceOf[Merger[V, That]]+=value
    }
    def valueInitializer (value:V) = {
      val merger = cmf.apply()
      merger +=value
      merger
        }
    override def insertEntry(key: K, value: V) = table.tryAggregateEntry[V](key, value, valueInitializer, valueCombiner)
  }

  class HashMapCollectingMergerCallResultKernel[@specialized(Int, Long) K, @specialized(Int, Long, Float, Double) V, That <: AnyRef] extends scala.collection.par.workstealing.HashTables.HashMapKernel[K, Object, Unit] {
        def zero = ()
        def combine(a: Unit, b: Unit) = ()
        def apply(node: Scheduler.Node[(K, Object), Unit], from: Int, until: Int) = {
          val stealer = node.stealer.asInstanceOf[scala.collection.par.workstealing.HashTables.HashMapStealer[K, Object]]
          val table = stealer.table
          var i = from
          while (i < until) {
            var entries = table(i)
            while (entries != null) {
              import collection.mutable
              import mutable.DefaultEntry
              val de = entries.asInstanceOf[DefaultEntry[K, Object]]
              de.value = de.value.asInstanceOf[Merger[V, That]].result
              entries = entries.next
            }
            i += 1
          }
          ()
        }
      }

  abstract class HashMapKernel[K, V, R] extends IndexedStealer.IndexedKernel[(K, V), R] {
    def apply(node: Node[(K, V), R], chunkSize: Int): R = {
      val stealer = node.stealer.asInstanceOf[HashMapStealer[K, V]]
      apply(node, stealer.nextProgress, stealer.nextUntil)
    }
    def apply(node: Node[(K, V), R], from: Int, until: Int): R
  }

  def newHashSetMerger[@specialized(Int, Long) T <: AnyVal: ClassTag](callee: Par[HashSet[T]])(implicit ctx: Scheduler) = {
    new HashSetMerger[T](HashBuckets.DISCRIMINANT_BITS, FlatHashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, ctx)
  }

  def newHashSetMerger[T](callee: Par[HashSet[T]])(implicit ctx: Scheduler) = {
    new HashSetMerger[Object](HashBuckets.DISCRIMINANT_BITS, FlatHashTable.defaultLoadFactor, HashBuckets.IRRELEVANT_BITS, ctx).asInstanceOf[HashSetMerger[T]]
  }

  class HashSetMerger[@specialized(Int, Long) T: ClassTag](
    val width: Int,
    val loadFactor: Int,
    val seed: Int,
    val ctx: Scheduler) extends HashBuckets[T, T, HashSetMerger[T], Par[HashSet[T]]] with FlatHashTable.HashUtils[T] {
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
    val table: HashBuckets.FlatEntries[T]) extends IndexedStealer.IndexedKernel[Int, Int] {
    override def incrementStepFactor(config: Scheduler.Config) = 1
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

