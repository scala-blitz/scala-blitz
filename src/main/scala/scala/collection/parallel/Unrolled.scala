package scala.collection.parallel



import scala.reflect.ClassTag



object Unrolled {
  val INITIAL_CHUNK_SIZE = 4
  val MAXIMUM_CHUNK_SIZE = 256

  class PairBuffer[@specialized(Int, Long) K: ClassTag, @specialized(Int, Long, Float, Double) V: ClassTag] {
    var head = new PairNode[K, V](new Array[K](INITIAL_CHUNK_SIZE), new Array[V](INITIAL_CHUNK_SIZE))
    var tail = head

    def put(k: K, v: V) = {
      var node = tail
      var pos = node.pos
      val len = node.keys.length
      if (pos == len) {
        node.next = new PairNode(new Array(math.max(len * 4, MAXIMUM_CHUNK_SIZE)), new Array(math.max(len * 4, MAXIMUM_CHUNK_SIZE)))
        tail = node.next
        pos = 0
      }
      node.keys(pos) = k
      node.vals(pos) = v
      node.pos = pos + 1
    }
  }

  class PairNode[@specialized(Int, Long) K, @specialized(Int, Long, Float, Double) V](val keys: Array[K], val vals: Array[V]) {
    var pos: Int = 0
    var next: PairNode[K, V] = null
  }
}