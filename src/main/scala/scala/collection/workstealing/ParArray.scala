package scala.collection.workstealing



import sun.misc.Unsafe
import annotation.tailrec
import scala.collection._
import scala.reflect.ClassTag



class ParArray[@specialized T: ClassTag](val array: Array[T], val config: Workstealing.Config) extends ParIterable[T]
with ParIterableLike[T, ParArray[T]]
with IndexedWorkstealing[T] {

  import IndexedWorkstealing._
  import Workstealing.initialStep

  def size = array.length

  type N[R] = ArrayNode[T, R]

  type K[R] = ArrayKernel[T, R]

  protected[this] def newCombiner = new ParArray.ArrayCombiner[T]

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

  private val COMBINER_CHUNK_SIZE_LIMIT = 1024

  trait Tree[T] {
    def level: Int
    def size: Int
  }

  final class Chunk[@specialized T: ClassTag](val array: Array[T]) extends Tree[T] {
    var size = -1
    def level = 0
  }

  final class Node[@specialized T](val left: Tree[T], val right: Tree[T], val level: Int, val size: Int) extends Tree[T]

  abstract class ChunkCombiner[@specialized T: ClassTag, To](init: Boolean)
  extends Combiner[T, To] with CombinerLike[T, To, ChunkCombiner[T, To]] {
    private[ParArray] var chunksize: Int = _
    private[ParArray] var lastarr: Array[T] = _
    private[ParArray] var lastpos: Int = _
    private[ParArray] var lasttree: Chunk[T] = _
    private[ParArray] var chunkstack: List[Tree[T]] = _

    def this() = this(true)

    if (init) clear()

    private[ParArray] def createChunk(): Chunk[T] = {
      chunksize = math.min(chunksize * 2, COMBINER_CHUNK_SIZE_LIMIT)
      val array = implicitly[ClassTag[T]].newArray(chunksize)
      val chunk = new Chunk[T](array)
      chunk
    }

    private[ParArray] def closeLast() {
      lasttree.size = lastpos
    }

    private[ParArray] def mergeStack() = {
      @tailrec def merge(stack: List[Tree[T]]): List[Tree[T]] = stack match {
        case (c1: Chunk[T]) :: (c2: Chunk[T]) :: tail =>
          merge(new Node[T](c2, c1, 1, c1.size + c2.size) :: tail)
        case (n1: Node[T]) :: (n2: Node[T]) :: tail if n1.level == n2.level =>
          merge(new Node[T](n2, n1, n1.level + 1, n1.size + n2.size) :: tail)
        case _ =>
          stack
      }

      chunkstack = merge(chunkstack)
    }

    private[ParArray] def tree = {
      @tailrec def merge(stack: List[Tree[T]]): Tree[T] = stack match {
        case t1 :: t2 :: tail => merge(new Node(t2, t1, math.max(t1.level, t2.level) + 1, t1.size + t2.size) :: tail)
        case t :: Nil => t
      }

      merge(chunkstack)
    }

    def +=(elem: T): ChunkCombiner[T, To] = {
      if (lastpos < chunksize) {
        lastarr(lastpos) = elem
        lastpos += 1
        this
      } else {
        closeLast()
        lasttree = createChunk()
        lastarr = lasttree.array
        lastpos = 0
        chunkstack = lasttree :: chunkstack
        mergeStack()
        +=(elem)
      }
    }

    def newChunkCombiner(shouldInit: Boolean): ChunkCombiner[T, To]

    def result: To
  
    def combine(that: ChunkCombiner[T, To]): ChunkCombiner[T, To] = {
      this.closeLast()

      val res = newChunkCombiner(false)
      res.chunksize = math.max(this.chunksize, that.chunksize)
      res.lastarr = that.lastarr
      res.lastpos = that.lastpos
      res.lasttree = that.lasttree
      res.chunkstack = that.chunkstack ::: List(this.tree)
      
      this.clear()
      that.clear()

      res
    }

    def clear(): Unit = {
      chunksize = 4
      lasttree = createChunk()
      lastarr = lasttree.array
      lastpos = 0
      chunkstack = List(lasttree)
    }

  }

  final class ArrayCombiner[@specialized T: ClassTag](init: Boolean) extends ChunkCombiner[T, ParArray[T]](init) {
    def this() = this(true)
    def newChunkCombiner(shouldInit: Boolean) = new ArrayCombiner(shouldInit)
    def result = ???
  }

  final class TreeCombiner[@specialized T: ClassTag](init: Boolean) extends ChunkCombiner[T, Tree[T]](init) {
    def this() = this(true)
    def newChunkCombiner(shouldInit: Boolean) = new TreeCombiner(shouldInit)
    def result = tree
  }

}





















