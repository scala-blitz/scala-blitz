package scala.collection.workstealing



import sun.misc.Unsafe
import annotation.tailrec
import collection._



class ParArray[@specialized T](val array: Array[T], val config: Workstealing.Config)
extends IndexedWorkstealing[T]
with ParIterableOperations[T] {

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















