package scala.collection.parallel



import scala.reflect.ClassTag
import scala.collection.mutable.HashTable
import scala.collection.mutable.HashEntry
import scala.collection.mutable.DefaultEntry



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
    def tryInsertEntry(k: K, v: V) {
      var h = index(elemHashCode(k))
      val olde = table(h).asInstanceOf[DefaultEntry[K, V]]

      // check if key already exists
      var ce = olde
      while (ce ne null) {
        if (ce.key == k) {
          h = -1
          ce = null
        } else ce = ce.next
      }

      // if key does not already exist
      if (h != -1) {
        val e = new DefaultEntry(k, v)
        e.next = olde
        table(h) = e
        tableSize += 1
      }
    }
    protected def createNewEntry[X](key: K, x: X) = ???
  }

}