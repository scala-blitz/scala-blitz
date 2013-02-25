package scala.collection.parallel



import sun.misc.Unsafe
import annotation.tailrec
import collection._



class RangeWorkstealing(val range: Range, val config: Workstealing.Config) extends IndexedWorkstealing[Int] {

  import IndexedWorkstealing._
  import Workstealing.initialStep

  def size = range.size

  type N[R] = RangeNode[R]

  type K[R] = RangeKernel[R]

  final class RangeNode[R](l: Ptr[Int, R], r: Ptr[Int, R])(val rng: Range, s: Int, e: Int, rn: Long, st: Int)
  extends IndexNode[Int, R](l, r)(s, e, rn, st) {
    var lindex = start

    def next(): Int = {
      val i = lindex
      lindex = i + 1
      rng.apply(i)
      //rng.start + i * rng.step // does not bring considerable performance gain
    }

    def newExpanded(parent: Ptr[Int, R]): RangeNode[R] = {
      val r = /*READ*/range
      val p = positiveProgress(r)
      val u = until(r)
      val remaining = u - p
      val firsthalf = remaining / 2
      val secondhalf = remaining - firsthalf
      val lnode = new RangeNode[R](null, null)(rng, p, p + firsthalf, createRange(p, p + firsthalf), initialStep)
      val rnode = new RangeNode[R](null, null)(rng, p + firsthalf, u, createRange(p + firsthalf, u), initialStep)
      val lptr = new Ptr[Int, R](parent, parent.level + 1)(lnode)
      val rptr = new Ptr[Int, R](parent, parent.level + 1)(rnode)
      val nnode = new RangeNode(lptr, rptr)(rng, start, end, r, step)
      nnode.owner = this.owner
      nnode
    }

  }

  abstract class RangeKernel[R] extends IndexKernel[Int, R] {
    def applyIndex(node: RangeNode[R], p: Int, np: Int) = {
      val rangestart = node.rng.start
      val step = node.rng.step
      val from = rangestart + step * p
      val to = rangestart + step * (np - 1)

      if (step == 1) applyRange1(node, from, to)
      else applyRange(node, from, to, step)
    }
    def applyRange(node: RangeNode[R], p: Int, np: Int, step: Int): R
    def applyRange1(node: RangeNode[R], p: Int, np: Int): R
  }

  def newRoot[R] = {
    val work = new RangeNode[R](null, null)(range, 0, size, createRange(0, size), initialStep)
    val root = new Ptr[Int, R](null, 0)(work)
    root
  }

}





