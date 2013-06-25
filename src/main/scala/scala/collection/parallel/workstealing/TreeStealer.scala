package scala.collection.parallel
package workstealing



import scala.annotation.tailrec
import scala.reflect.ClassTag



trait TreeStealer[T, N >: Null <: AnyRef] extends Stealer[T] {
  import TreeStealer._

  val root: N
  val totalSize: Int
  val classTag: ClassTag[N]
  val pathStack = new Array[Int](cacheAligned(depthBound(totalSize) + 1))
  val nodeStack = classTag.newArray(cacheAligned(depthBound(totalSize) + 1))
  var depth: Int = 0

  def totalChildren(n: N): Int
  def child(n: N, idx: Int): N
  def isLeaf(n: N): Boolean
  def estimateSubtree(n: N, depth: Int, totalSize: Int): Int
  def depthBound(totalSize: Int): Int

  private def checkBounds(idx: Int) = {
    if (idx < 0 || idx >= pathStack.length) throw new IndexOutOfBoundsException("idx: " + idx + ", length: " + pathStack.length)
  }

  final def OFFSET(idx: Int): Int = STACK_BASE_OFFSET + idx * STACK_INDEX_SCALE

  final def READ_STACK(idx: Int): Int = {
    checkBounds(idx)
    unsafe.getIntVolatile(pathStack, OFFSET(idx))
  }

  final def WRITE_STACK(idx: Int, nv: Int) {
    checkBounds(idx)
    unsafe.putIntVolatile(pathStack, OFFSET(idx), nv)
  }

  final def CAS_STACK(idx: Int, ov: Int, nv: Int): Boolean = {
    checkBounds(idx)
    unsafe.compareAndSwapInt(pathStack, OFFSET(idx), ov, nv)
  }

  final def encodeStolen(origin: Int, total: Int, progress: Int): Int = {
    SPECIAL_MASK |
    STOLEN_MASK |
    ((origin << ORIGIN_SHIFT) & ORIGIN_MASK) |
    ((total << TOTAL_SHIFT) & TOTAL_MASK) |
    ((progress << PROGRESS_SHIFT) & PROGRESS_MASK)
  }

  final def toStolen(code: Int) = code | STOLEN_MASK

  final def toUnstolen(code: Int) = code & ~STOLEN_MASK

  final def encodeCompleted(origin: Int, total: Int, progress: Int): Int = {
    SPECIAL_MASK |
    COMPLETED_MASK |
    ((origin << ORIGIN_SHIFT) & ORIGIN_MASK) |
    ((total << TOTAL_SHIFT) & TOTAL_MASK) |
    ((progress << PROGRESS_SHIFT) & PROGRESS_MASK)
  }

  final def encode(origin: Int, total: Int, progress: Int): Int = {
    SPECIAL_MASK |
    ((origin << ORIGIN_SHIFT) & ORIGIN_MASK) |
    ((total << TOTAL_SHIFT) & TOTAL_MASK) |
    ((progress << PROGRESS_SHIFT) & PROGRESS_MASK)
  }

  final def completed(code: Int): Boolean = ((code & COMPLETED_MASK) >>> COMPLETED_SHIFT) != 0

  final def stolen(code: Int): Boolean = ((code & STOLEN_MASK) >>> STOLEN_SHIFT) != 0

  final def origin(code: Int): Int = ((code & ORIGIN_MASK) >>> ORIGIN_SHIFT)

  final def total(code: Int): Int =  ((code & TOTAL_MASK) >>> TOTAL_SHIFT)

  final def progress(code: Int): Int = ((code & PROGRESS_MASK) >>> PROGRESS_SHIFT)

  final def special(code: Int): Boolean = (code & SPECIAL_MASK) != 0

  final def terminal(code: Int): Boolean = (code & TERM_MASK) != 0

  final def decodeString(code: Int): String = if (special(code)) {
    "([%s%s]%d,%d,%d)".format(
      if (completed(code)) "C" else " ",
      if (stolen(code)) "!" else " ",
      origin(code),
      total(code),
      progress(code)
    )
  } else if (terminal(code)) "TERM"
  else code.toString

  final def push(ov: Int, nv: Int): Boolean = if (CAS_STACK(depth + 1, ov, nv)) {
    val cidx = origin(nv)
    nodeStack(depth + 1) = child(nodeStack(depth), cidx)
    depth += 1
    true
  } else false

  final def pop(ov: Int, nv: Int): Boolean = if (CAS_STACK(depth, ov, nv)) {
    nodeStack(depth) = null
    depth -= 1
    true
  } else false

  final def switch(ov: Int, nv: Int): Boolean = if (CAS_STACK(depth, ov, nv)) {
    true
  } else false

  final def peekprev = if (depth > 0) READ_STACK(depth - 1) else 0

  final def peekcurr = READ_STACK(depth)

  final def peeknext = READ_STACK(depth + 1)

}


object TreeStealer {

  val STACK_BASE_OFFSET = unsafe.arrayBaseOffset(classOf[Array[Int]])
  val STACK_INDEX_SCALE = unsafe.arrayIndexScale(classOf[Array[Int]])
  val PROGRESS_SHIFT = 0
  val PROGRESS_MASK = 0x000000ff
  val ORIGIN_SHIFT = 8
  val ORIGIN_MASK = 0x0000ff00
  val TOTAL_SHIFT = 16
  val TOTAL_MASK = 0x00ff0000
  val STOLEN_SHIFT = 24
  val STOLEN_MASK = 0x01000000
  val COMPLETED_SHIFT = 25
  val COMPLETED_MASK = 0x02000000
  val TERM_SHIFT = 30
  val TERM_MASK = 0x40000000
  val SPECIAL_SHIFT = 31
  val SPECIAL_MASK = 0x80000000

  def cacheAligned(sz: Int) = ((sz - 1) / 16 + 1) * 16

  trait ChunkIterator[@specialized(Int, Long, Float, Double) T] {
    def hasNext: Boolean
    def next(): T
  }

  trait External[@specialized(Int, Long, Float, Double) T, N >: Null <: AnyRef] extends TreeStealer[T, N] {
    val chunkIterator: ChunkIterator[T]

    def newStealer: TreeStealer.External[T, N]

    def resetIterator(n: N): Unit

    def elementAt(n: N, idx: Int): T

    def rootInit() {
      pathStack(0) = encode(1, totalChildren(root), 1)
      nodeStack(0) = root
    }

    def hasNext = chunkIterator.hasNext

    def next() = chunkIterator.next()

    final def state = {
      val code = READ_STACK(0)
      if (completed(code)) Stealer.Completed
      else if (stolen(code)) Stealer.StolenOrExpanded
      else Stealer.AvailableOrOwned
    }

    @tailrec final def advance(step: Int): Int = if (depth < 0) -1 else {
      def isLast(prev: Int, curr: Int) = prev == 0 || {
        val prevnode = nodeStack(depth - 1)
        total(prev) == origin(curr)
      }

      val next = peeknext
      val curr = peekcurr
      val prev = peekprev

      if (completed(curr)) {
        markCompleted()
        -1
      } else if (special(prev) && stolen(prev) || stolen(curr)) {
        markStolen()
        -1
      } else {
        val totalch = total(curr)
        val currprogress = progress(curr)
        if (currprogress < totalch || totalch == 0) {
          if (next == 0) {
            val currnode = nodeStack(depth)
            val estimate = estimateSubtree(currnode, depth, totalSize)
            if (isLeaf(currnode) || estimate < step) {
              // decide to batch - pop to completed
              val last = isLast(prev, curr)
              val ncurr = if (last) {
                if (depth > 0) 0 else encodeCompleted(origin(curr), totalch, 0)
              } else encode(progress(prev), totalch, 0)
              if (pop(curr, ncurr)) {
                resetIterator(currnode)
                estimate
              } else advance(step)
            } else {
              // or not to batch - push
              val nextnode = child(currnode, currprogress)
              val nnext = encode(currprogress, totalChildren(nextnode), 1)
              push(next, nnext)
              advance(step)
            }
          } else {
            if (currprogress == origin(next)) {
              // next origin identical - switch
              val ncurr = encode(origin(curr), totalch, currprogress + 1)
              switch(curr, ncurr)
            } else {
              // next origin different - push
              val currnode = nodeStack(depth)
              val nextnode = child(currnode, currprogress)
              val nnext = encode(currprogress, totalChildren(nextnode), 1)
              push(next, nnext)
            }
            advance(step)
          }
        } else {
          if (next == 0) {
            val last = isLast(prev, curr)
            if (last) {
              if (depth > 0) {
                // last node below root - pop to 0
                pop(curr, 0)
              } else {
                // at root mark stealer as completed
                val ncurr = encodeCompleted(origin(curr), totalch, 0)
                pop(curr, ncurr)
              }
            } else {
              // not last - pop to completed
              val ncurr = encode(origin(curr), totalch, 0)
              pop(curr, ncurr)
            }
            advance(step)
          } else {
            if (currprogress == origin(next)) {
              // next origin identical - error!
              sys.error("error state: " + this + ", " + currprogress + ", " + origin(next))
            } else {
              // next origin different - push
              val currnode = nodeStack(depth)
              val nextnode = child(currnode, currprogress)
              val nnext = encode(currprogress, totalChildren(nextnode), 1)
              push(next, nnext)
            }
            advance(step)
          }
        }
      }
    }

    @tailrec final def markCompleted(): Boolean = {
      val code = READ_STACK(0)
      if (stolen(code)) {
        markStolen()
        false
      } else {
        val ncode = encodeCompleted(origin(code), total(code), progress(code))
        if (CAS_STACK(0, code, ncode)) true
        else markCompleted()
      }
    }

    @tailrec final def markStolen(): Boolean = {
      @tailrec def stealAll(idx: Int) {
        val code = READ_STACK(idx)
        if (code == TERM_MASK) {
          // do nothing -- stealing completed
        } else if (code == 0) {
          // replace with a terminator
          if (!CAS_STACK(idx, 0, TERM_MASK)) stealAll(idx)
        } else if (stolen(code)) {
          // steal on the next level
          stealAll(idx + 1)
        } else {
          // encode a theft
          val ncode = toStolen(code)
          if (CAS_STACK(idx, code, ncode)) stealAll(idx + 1)
          else stealAll(idx)
        }
      }

      val code = READ_STACK(0)
      if (completed(code)) false
      else {
        val ncode = encodeStolen(origin(code), total(code), progress(code))
        if (CAS_STACK(0, code, ncode)) {
          stealAll(1)
          true
        } else markStolen()
      }
    }

    def split: (Stealer[T], Stealer[T]) = {
      val left = newStealer
      val right = newStealer

      var d = 0
      var currnode: N = root
      var splitFound = false
      while (d < pathStack.length && !splitFound) {
        val code = READ_STACK(d)
        val prog = progress(code)
        val tot = total(code)
        if (terminal(code)) {
          val rcode = right.READ_STACK(d - 1)
          val nrcode = 0
          left.depth = d - 1
          right.depth = d - 2
          right.WRITE_STACK(d - 1, nrcode)
          d = pathStack.length
        } else if (prog != 0 && prog < tot) {
          val offset = (tot - prog) / 2
          val ntot = prog + offset
          val lcode = encode(origin(code), ntot, prog)
          val rcode = encode(origin(code), tot, prog + offset + 1)

          left.nodeStack(d) = currnode
          left.depth = d
          left.WRITE_STACK(d, lcode)
          if (prog == 1 && prog == ntot && terminal(READ_STACK(d + 1))) {
            WRITE_STACK(d + 1, encodeStolen(prog, totalChildren(child(currnode, 1)), 1))
            WRITE_STACK(d + 2, TERM_MASK)
          }

          right.nodeStack(d) = currnode
          right.depth = d
          right.WRITE_STACK(d, rcode)
          val rnext = encode(prog + offset, 0, 0)
          right.WRITE_STACK(d + 1, rnext)
          splitFound = true
        } else {
          left.nodeStack(d) = currnode
          left.WRITE_STACK(d, toUnstolen(code))
          right.nodeStack(d) = currnode
          right.WRITE_STACK(d, toUnstolen(code))
        }
        if (prog != 0 && tot != 0) currnode = child(currnode, prog)
        else currnode = null
        d += 1
      }
      while (d < pathStack.length) {
        val code = READ_STACK(d)
        val prog = progress(code)
        val tot = total(code)
        if (terminal(code)) {
          left.WRITE_STACK(d, 0)
          d = pathStack.length
        } else if (code == 0) {
          // done
          d = pathStack.length
        } else {
          var ncode = toUnstolen(code)
          val prev = left.READ_STACK(d - 1)
          if (progress(ncode) == 0 && total(prev) == origin(ncode)) ncode = 0
          left.nodeStack(d) = currnode
          if (ncode != 0 && progress(ncode) != 0) left.depth = d
          left.WRITE_STACK(d, ncode)
        }
        if (prog != 0 && tot != 0) currnode = child(currnode, prog)
        else currnode = null
        d += 1
      }

      (left, right)
    }

    def elementsRemainingEstimate: Int = ???

    override def toString = "TreeStealer.External(\ndepth: %d\npath:  %s\nnodes: %s\n)".format(
      depth,
      pathStack.map(x => "%1$12s".format(decodeString(x))).mkString(", "),
      nodeStack map {
        case null => null
        case x => x.getClass.getSimpleName.takeRight(4) + "@" + System.identityHashCode(x).toString.takeRight(4)
      } map {
        case s => "%1$12s".format(s)
      } mkString(", ")
    )

  }

}




