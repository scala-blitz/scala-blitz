package scala.collection.par
package workstealing



import scala.annotation.tailrec
import scala.reflect.ClassTag



class BinaryTreeStealer[T, Node >: Null <: AnyRef: ClassTag](val root: Node, val startingDepth: Int, val totalElems: Int, val binary: BinaryTreeStealer.Binary[T, Node])
extends Stealer[T] {
  import BinaryTreeStealer._

  /* chunk iteration */
  val subtreeIterator = new SubtreeIterator(binary)
  val onceIterator = new OnceIterator[T]
  var iterator: Iterator[T] = _

  /* local state */
  var localDepth = 0
  val localStack = new Array[Node](binary.depthBound(totalElems, startingDepth))

  final def topLocal = localStack(localDepth - 1)

  /* atomic state */
  @volatile var stack: Long = _

  final def READ_STACK = unsafe.getLongVolatile(this, STACK_OFFSET)

  final def WRITE_STACK(nv: Long) = unsafe.putLongVolatile(this, STACK_OFFSET, nv)

  final def CAS_STACK(ov: Long, nv: Long) = unsafe.compareAndSwapLong(this, STACK_OFFSET, ov, nv)

  /* stealer api */

  def next(): T = iterator.next()

  def hasNext: Boolean = iterator.hasNext

  def state: Stealer.State = {
    val s = READ_STACK
    val statebits = s & 0x3
    if (statebits == AVAILABLE || statebits == UNINITIALIZED) Stealer.AvailableOrOwned
    else if (statebits == COMPLETED) Stealer.Completed
    else Stealer.StolenOrExpanded
  }

  final def advance(step: Int): Int = {
    val s = READ_STACK
    val statebits = s & 0x3
    if (statebits != AVAILABLE && statebits != UNINITIALIZED) -1
    else {
      def pushLocal(stack: Long, v: Long, node: Node): Long = {
        localStack(localDepth) = node
        val newstack = stack | (v << (localDepth * 2 + 2))
        localDepth += 1
        newstack
      }
      def topLocal = localStack(localDepth - 1)
      def topMask(stack: Long) = {
        val dep = localDepth * 2
        (stack & (0x3 << dep)) >> dep
      }
      def popLocal(stack: Long): Long = {
        localDepth -= 1
        localStack(localDepth) = null
        stack & ~(0x3 << (localDepth * 2 + 2))
      }
      def switchLocal(stack: Long, v: Long): Long = {
        val dep = localDepth * 2
        (stack & ~(0x3 << dep)) | (v << dep)
      }

      var estimatedChunkSize = -1
      var nextstack = s

      if (statebits == AVAILABLE) {
        var tm = topMask(nextstack)

        if (tm == S || (tm == T && binary.isEmptyLeaf(binary.right(topLocal)))) {
          nextstack = popLocal(nextstack)
          while (topMask(nextstack) == R && localDepth > 0) nextstack = popLocal(nextstack)
          if (localDepth == 0) {
            estimatedChunkSize = -1
            nextstack = (nextstack & ~0x3) | COMPLETED
          } else {
            estimatedChunkSize = 1
            nextstack = switchLocal(nextstack, T)
            onceIterator.set(binary.value(topLocal))
            iterator = onceIterator
          }
        } else if (tm == T) {
          nextstack = switchLocal(nextstack, R)
          var node = binary.right(topLocal)
          var bound = binary.sizeBound(totalElems, startingDepth + localDepth)
          while (!binary.isEmptyLeaf(binary.left(node)) && step < bound) {
            nextstack = pushLocal(nextstack, L, node)
            node = binary.left(node)
            bound = binary.sizeBound(totalElems, startingDepth + localDepth)
          }
          if (step < bound) {
            nextstack = pushLocal(nextstack, S, node)
            subtreeIterator.set(node)
            iterator = subtreeIterator
          } else {
            nextstack = pushLocal(nextstack, T, node)
            onceIterator.set(binary.value(node))
            iterator = onceIterator
          }
          estimatedChunkSize = bound
        } else throw new IllegalStateException(this.toString)
      } else {
        nextstack = AVAILABLE
        var node = root
        while (!binary.isEmptyLeaf(binary.left(node))) {
          nextstack = pushLocal(nextstack, L, node)
          node = binary.left(node)
        }
        nextstack = pushLocal(nextstack, T, node)
        onceIterator.set(binary.value(node))
        iterator = onceIterator
        estimatedChunkSize = 1
      }

      while (!CAS_STACK(s, nextstack)) {
        val nstatebits = READ_STACK & 0x3
        if (nstatebits == STOLEN || nstatebits == COMPLETED) return -1
      }

      estimatedChunkSize
    }
  }

  @tailrec final def markCompleted(): Boolean = {
    val s = READ_STACK
    val statebits = s & 0x3
    if (statebits == AVAILABLE) {
      val cs = (s & ~0x3) | COMPLETED
      if (CAS_STACK(s, cs)) true
      else markCompleted()
    } else false
  }

  @tailrec final def markStolen(): Boolean = {
    val s = READ_STACK
    val statebits = s & 0x3
    if (statebits == AVAILABLE) {
      val ss = (s & ~0x3) | STOLEN
      if (CAS_STACK(s, ss)) true
      else markStolen()
    } else false
  }

  def split: (Stealer[T], Stealer[T]) = ???

  def elementsRemainingEstimate: Int = ???

  override def toString = {
    def show(s: Long) = {
      val statebits = s & 0x3
      val state = statebits match {
        case UNINITIALIZED => "UN"
        case AVAILABLE => "AV"
        case STOLEN => "ST"
        case COMPLETED => "CO"
      }
      var pos = 2
      var stack = " "
      while (pos < 64) {
        stack += " " + ((s & (0x3 << pos)) >> pos match {
          case L => "L"
          case R => "R"
          case S => "S"
          case T => "T"
        })
        pos += 2
      }
      state + stack
    }

    s"BinTreeStealer(startDepth: $startingDepth, localDepth: $localDepth, #elems: $totalElems) {\n" +
    s"  stack: ${show(READ_STACK)}\n" +
    s"  local: ${localStack.map(_ != null).mkString(", ")}\n" +
    s"}"
  }

}


object BinaryTreeStealer {

  val STACK_OFFSET = unsafe.objectFieldOffset(classOf[BinaryTreeStealer[_, _]].getDeclaredField("stack"))

  val UNINITIALIZED = 0x0
  val STOLEN = 0x1
  val COMPLETED = 0x2
  val AVAILABLE = 0x3

  val L = 0x1
  val R = 0x2
  val T = 0x0
  val S = 0x3

  trait Binary[T, Node >: Null <: AnyRef] {
    def sizeBound(total: Int, depth: Int): Int
    def depthBound(total: Int, depth: Int): Int
    def isEmptyLeaf(n: Node): Boolean
    def left(n: Node): Node
    def right(n: Node): Node
    def value(n: Node): T
  }

  class SubtreeIterator[T, Node >: Null <: AnyRef](val binary: Binary[T, Node]) extends Iterator[T] {
    private var root: Node = _
    def set(n: Node) {
      root = n
    }
    def next() = ???
    def hasNext = ???
  }

  class OnceIterator[T] extends Iterator[T] {
    private var elem: T = _
    def set(v: T) {
      hasNext = true
      elem = v
    }
    var hasNext = false
    def next() = {
      hasNext = false
      elem
    }
  }

}




