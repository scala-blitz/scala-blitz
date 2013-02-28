package scala.collection.workstealing



import sun.misc.Unsafe
import annotation.tailrec
import collection._
import reflect.ClassTag



class ParArray[@specialized T](val array: Array[T], val config: Workstealing.Config) extends ParIterable[T]
with ParIterableLike[T, ParArray[T]]
with IndexedWorkstealing[T] {

  import IndexedWorkstealing._
  import Workstealing.initialStep

  def size = array.length

  type N[R] = ArrayNode[T, R]

  type K[R] = ArrayKernel[T, R]

  final class ArrayNode[@specialized S, R](l: Ptr[S, R], r: Ptr[S, R])(val arr: Array[S], s: Int, e: Int, rn: Long, st: Int)
  extends IndexNode[S, R](l, r)(s, e, rn, st) {
    var lindex = start

    def next(): S = {
      val i = lindex
      lindex = i + 1
      arr(i)
    }

    def newExpanded(parent: Ptr[S, R]): ArrayNode[S, R] = {
      val r = /*READ*/range
      val p = positiveProgress(r)
      val u = until(r)
      val remaining = u - p
      val firsthalf = remaining / 2
      val secondhalf = remaining - firsthalf
      val lnode = new ArrayNode[S, R](null, null)(arr, p, p + firsthalf, createRange(p, p + firsthalf), initialStep)
      val rnode = new ArrayNode[S, R](null, null)(arr, p + firsthalf, u, createRange(p + firsthalf, u), initialStep)
      val lptr = new Ptr[S, R](parent, parent.level + 1)(lnode)
      val rptr = new Ptr[S, R](parent, parent.level + 1)(rnode)
      val nnode = new ArrayNode(lptr, rptr)(arr, start, end, r, step)
      nnode.owner = this.owner
      nnode
    }

  }

  abstract class ArrayKernel[@specialized S, R] extends IndexKernel[S, R] {
    override def isNotRandom = true
  }

  def newRoot[R] = {
    val work = new ArrayNode[T, R](null, null)(array, 0, size, createRange(0, size), initialStep)
    val root = new Ptr[T, R](null, 0)(work)
    root
  }

}


object ParArray {
  import collection.mutable.ArrayBuffer

  private val COMBINER_CHUNK_SIZE_LIMIT = 1024

  private class Chunk[@specialized T: ClassTag](val array: Array[T]) {
    var elements = -1
  }

  final class LinkedCombiner[@specialized T: ClassTag](chsz: Int, larr: Array[T], lpos: Int, chs: ArrayBuffer[Chunk[T]], clr: Boolean)
  extends Combiner[T, ParArray[T], LinkedCombiner[T]] {
    private[ParArray] var chunksize: Int = chsz
    private[ParArray] var lastarr: Array[T] = larr
    private[ParArray] var lastpos: Int = lpos
    private[ParArray] var chunks: ArrayBuffer[Chunk[T]] = chs

    def this() = this(0, null, 0, null, true)

    if (clr) clear()

    private[ParArray] def createChunk(): Chunk[T] = {
      chunksize = math.min(chunksize * 2, COMBINER_CHUNK_SIZE_LIMIT)
      val array = implicitly[ClassTag[T]].newArray(chunksize)
      val chunk = new Chunk[T](array)
      chunk
    }

    private[ParArray] def closeLast() {
      chunks.last.elements = lastpos
    }

    def +=(elem: T): LinkedCombiner[T] = {
      if (lastpos < chunksize) {
        lastarr(lastpos) = elem
        lastpos += 1
        this
      } else {
        closeLast()
        val lastnode = createChunk()
        lastarr = lastnode.array
        lastpos = 0
        chunks += lastnode
        +=(elem)
      }
    }
  
    def result: ParArray[T] = ???
  
    def combine(that: LinkedCombiner[T]): LinkedCombiner[T] = {
      this.closeLast()
      
      val res = new LinkedCombiner(
        math.max(this.chunksize, that.chunksize),
        that.lastarr,
        that.lastpos,
        this.chunks ++= that.chunks,
        false
      )
      
      this.clear()
      that.clear()

      res
    }

    def clear(): Unit = {
      chunksize = 4
      val lastnode = createChunk()
      lastarr = lastnode.array
      lastpos = 0
      chunks = ArrayBuffer(lastnode)
    }

  }

}





















