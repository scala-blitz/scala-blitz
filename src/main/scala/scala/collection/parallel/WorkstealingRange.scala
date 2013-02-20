package scala.collection.parallel



import sun.misc.Unsafe
import annotation.tailrec
import collection._



class WorkstealingRange extends IndexedWorkstealingCollection[Int] {

  import IndexedWorkstealingCollection._
  import WorkstealingCollection.initialStep

  val size = sys.props("size").toInt

  type N[R] = RangeNode[R]

  type K[R] = RangeKernel[R]

  final class RangeNode[R](l: Ptr[R], r: Ptr[R])(s: Int, e: Int, rn: Long, st: Int)
  extends IndexNode[R](l, r)(s, e, rn, st) {
    var lindex = start
    var rindex = end

    def next(): Int = {
      val x = lindex
      lindex = x + 1
      x
    }

    def newExpanded(parent: Ptr[R]): RangeNode[R] = {
      val r = /*READ*/range
      val p = positiveProgress(r)
      val u = until(r)
      val remaining = u - p
      val firsthalf = remaining / 2
      val secondhalf = remaining - firsthalf
      val lnode = new RangeNode[R](null, null)(p, p + firsthalf, createRange(p, p + firsthalf), initialStep)
      val rnode = new RangeNode[R](null, null)(p + firsthalf, u, createRange(p + firsthalf, u), initialStep)
      val lptr = new Ptr[R](parent, parent.level + 1)(lnode)
      val rptr = new Ptr[R](parent, parent.level + 1)(rnode)
      val nnode = new RangeNode(lptr, rptr)(start, end, r, step)
      nnode.owner = this.owner
      nnode
    }

  }  

  abstract class RangeKernel[R] extends Kernel[R] {
    def applyRange(from: Int, until: Int): R

    override def workOn(tree: Ptr[R]): Boolean = {
      val node = /*READ*/tree.child.repr
      var lsum = zero
      var rsum = zero
      var incCount = 0
      val incFreq = incrementFrequency
      val ms = maxStep
      var looping = true
      val rand = WorkstealingCollection.localRandom
      while (looping) {
        val currstep = /*READ*/node.step
        val currrange = /*READ*/node.range
        val p = progress(currrange)
        val u = until(currrange)
  
        if (!stolen(currrange) && !completed(currrange)) {
          if (rand.nextBoolean) {
            val newp = math.min(u, p + currstep)
            val newrange = createRange(newp, u)
  
            // do some work on the left
            if (node.casRange(currrange, newrange)) lsum = combine(lsum, applyRange(p, newp))
          } else {
            val newu = math.max(p, u - currstep)
            val newrange = createRange(p, newu)
  
            // do some work on the right
            if (node.casRange(currrange, newrange)) rsum = combine(applyRange(newu, u), rsum)
          }
  
          // update step
          incCount = (incCount + 1) % incFreq
          if (incCount == 0) node.step = /*WRITE*/math.min(ms, currstep * 2)
        } else looping = false
      }
  
      // complete node information
      completeNode[R](lsum, rsum, tree, this)
    }
  }

  def newRoot[R] = {
    val work = new RangeNode[R](null, null)(0, size, createRange(0, size), initialStep)
    val root = new Ptr[R](null, 0)(work)
    root
  }

}















