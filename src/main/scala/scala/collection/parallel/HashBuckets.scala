package scala.collection.parallel



import scala.reflect.ClassTag



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
}