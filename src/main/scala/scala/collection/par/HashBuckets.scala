package scala.collection.par



import scala.reflect.ClassTag
import scala.collection.mutable.HashTable
import scala.collection.mutable.HashEntry
import scala.collection.mutable.DefaultEntry
import scala.collection.mutable.FlatHashTable



trait HashBuckets[@specialized(Int, Long) K, T, Repr <: HashBuckets[K, T, Repr, To], To]
  extends Merger[T, To] with MergerLike[T, To, Repr] {
  def width: Int

  def newHashBucket: Repr

  def mergeBucket(idx: Int, that: Repr, res: Repr): Unit

  def clearBucket(idx: Int): Unit

  final def bucketIndex(hc: Int): Int = hc >>> HashBuckets.IRRELEVANT_BITS

  def clear() {
    var i = 0
    while (i < (1 << width)) {
      clearBucket(i)
      i += 1
    }
  }

  def merge(that: Repr): Repr = {
    val res = newHashBucket

    var i = 0
    while (i < (1 << width)) {
      mergeBucket(i, that, res)
      i += 1
    }

    res
  }

}

object HashBuckets {
  val DISCRIMINANT_BITS = 5
  val IRRELEVANT_BITS = 32 - DISCRIMINANT_BITS

  class DefaultEntries[@specialized(Int, Long) K, @specialized(Int, Long, Float, Double) V](val width: Int, numelems: Int, lf: Int, _seedvalue: Int) extends HashTable[K, DefaultEntry[K, V]] {
    import HashTable._

    {
      val tableLength = math.max(1 << width, capacity(sizeForThreshold(_loadFactor, numelems)))
      _loadFactor = lf
      table = new Array[HashEntry[K, DefaultEntry[K, V]]](tableLength)
      tableSize = 0
      seedvalue = _seedvalue
      threshold = newThreshold(_loadFactor, table.length)
    }

    def setSize(sz: Int) = tableSize = sz

    def USE_OLD_VALUE[V](x: V, y: V) = x
    def IDENTITY[V](x: V) = x
    def tryInsertEntry(k: K, v: V): Boolean = tryAggregateEntry(k, v, IDENTITY[V], USE_OLD_VALUE)

    def tryCombineEntry(k: K, v: V, combiner: (V, V) => V): Boolean = tryAggregateEntry[V](k, v, IDENTITY[V], combiner)

    def tryAggregateEntry[T](k: K, v: T, zero: T => V, combiner: (V, T) => V): Boolean = {
      var h = index(elemHashCode(k))
      val olde = table(h).asInstanceOf[DefaultEntry[K, V]]

      // check if key already exists
      var ce = olde
      while (ce ne null) {
        if (ce.key == k) {
          ce.value = combiner(ce.value, v)
          h = -1
          ce = null
        } else ce = ce.next
      }

      // if key does not already exist
      if (h != -1) {
        val e = new DefaultEntry(k, zero(v))
        e.next = olde
        table(h) = e
        true
      } else false
    }

    protected def createNewEntry[X](key: K, x: X) = ???
  }

  class FlatEntries[@specialized(Int, Long) T](val width: Int, numelems: Int, lf: Int, _seedvalue: Int) extends FlatHashTable[T] {
    import FlatHashTable._

    val spills = new collection.mutable.ArrayBuffer[T]

    {
      _loadFactor = lf
      table = new Array[AnyRef](capacity(sizeForThreshold(numelems, _loadFactor)))
      tableSize = 0
      threshold = newThreshold(_loadFactor, table.length)
      seedvalue = _seedvalue
      sizeMapInit(table.length)
    }

    def setSize(sz: Int) = tableSize = sz

    def tableLength = table.length

    def tryInsertEntry(insertAt: Int, comesBefore: Int, elem: T): Int = {
      var h = insertAt
      if (h == -1) h = index(elem.hashCode)
      var entry = table(h)
      while (null != entry) {
        if (entry == elem) return 0
        h = h + 1 // we *do not* do `(h + 1) % table.length` here, because we'll never overflow!
        if (h >= comesBefore) spills.synchronized {
          spills += elem
          return 0
        }
        entry = table(h)
      }
      table(h) = elem.asInstanceOf[AnyRef]

      1
    }
  }

}
