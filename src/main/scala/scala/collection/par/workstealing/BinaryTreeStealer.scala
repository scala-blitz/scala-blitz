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
  val localStack = new Array[Node](MAX_TREE_DEPTH)

  final def pushLocal(stack: Long, v: Long, node: Node): Long = {
    localStack(localDepth) = node
    val newstack = stack | (v << (localDepth * 2 + 2))
    localDepth += 1
    newstack
  }

  final def topLocal = localStack(localDepth - 1)

  final def topMask(stack: Long) = {
    val dep = localDepth * 2
    (stack & (0x3L << dep)) >>> dep
  }

  final def popLocal(stack: Long): Long = {
    localDepth -= 1
    localStack(localDepth) = null
    stack & ~(0x3L << (localDepth * 2 + 2))
  }

  final def switchLocal(stack: Long, v: Long): Long = {
    val dep = localDepth * 2
    (stack & ~(0x3L << dep)) | (v << dep)
  }

  /* atomic state */
  @volatile var stack: Long = _

  final def READ_STACK = unsafe.getLongVolatile(this, STACK_OFFSET)

  final def WRITE_STACK(nv: Long) = unsafe.putLongVolatile(this, STACK_OFFSET, nv)

  final def CAS_STACK(ov: Long, nv: Long) = unsafe.compareAndSwapLong(this, STACK_OFFSET, ov, nv)

  override def duplicated: BinaryTreeStealer[T, Node] = {
    val d = new BinaryTreeStealer(root, startingDepth, totalElems, binary)

    if (this.iterator ne null) {
      val (a, b) = this.iterator.duplicate
      this.iterator = a
      d.iterator = b
    }

    d.localDepth = this.localDepth
    Array.copy(this.localStack, 0, d.localStack, 0, this.localStack.length)
    d.stack = this.stack

    d
  }

  /* stealer api */

  def next(): T = iterator.next()

  def hasNext: Boolean = iterator.hasNext

  def state: Stealer.State = {
    val s = READ_STACK
    val statebits = s & 0x3L
    if (statebits == AVAILABLE || statebits == UNINITIALIZED) Stealer.AvailableOrOwned
    else if (statebits == COMPLETED) Stealer.Completed
    else Stealer.StolenOrExpanded
  }

  final def nextBatch(step: Int): Int = {
    val s = READ_STACK
    val statebits = s & 0x3L
    if (statebits != AVAILABLE && statebits != UNINITIALIZED) -1
    else {
      var estimatedChunkSize = -1
      var nextstack = s

      if (statebits == AVAILABLE) {
        val tm = topMask(nextstack)

        if (tm == S || (tm == T && binary.isEmptyLeaf(binary.right(topLocal)))) {
          nextstack = popLocal(nextstack)
          while (topMask(nextstack) == R && localDepth > 0) nextstack = popLocal(nextstack)
          if (localDepth == 0) {
            estimatedChunkSize = -1
            nextstack = (nextstack & ~0x3L) | COMPLETED
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
          while (!binary.isEmptyLeaf(binary.left(node)) && bound >= step) {
            nextstack = pushLocal(nextstack, L, node)
            node = binary.left(node)
            bound = binary.sizeBound(totalElems, startingDepth + localDepth)
          }
          if (bound < step) {
            nextstack = pushLocal(nextstack, S, node)
            subtreeIterator.set(node)
            iterator = subtreeIterator
            estimatedChunkSize = bound
          } else {
            nextstack = pushLocal(nextstack, T, node)
            onceIterator.set(binary.value(node))
            iterator = onceIterator
            estimatedChunkSize = 1
          }
        } else throw new IllegalStateException(this.toString)
      } else {
        if (root == null) {
          estimatedChunkSize = -1
          nextstack = COMPLETED
        } else {
          nextstack = AVAILABLE
          var node = root
          var bound = binary.sizeBound(totalElems, startingDepth + localDepth)
          while (!binary.isEmptyLeaf(binary.left(node)) && bound >= step) {
            nextstack = pushLocal(nextstack, L, node)
            node = binary.left(node)
            bound = binary.sizeBound(totalElems, startingDepth + localDepth)
          }
          if (bound < step) {
            nextstack = pushLocal(nextstack, S, node)
            subtreeIterator.set(node)
            iterator = subtreeIterator
            estimatedChunkSize = bound
          } else {
            nextstack = pushLocal(nextstack, T, node)
            onceIterator.set(binary.value(node))
            iterator = onceIterator
            estimatedChunkSize = 1
          }
        }
      }

      while (!CAS_STACK(s, nextstack)) {
        val nstatebits = READ_STACK & 0x3L
        if (nstatebits == STOLEN || nstatebits == COMPLETED) return -1
      }

      estimatedChunkSize
    }
  }

  @tailrec final def markCompleted(): Boolean = {
    val s = READ_STACK
    val statebits = s & 0x3L
    if (statebits == AVAILABLE) {
      val cs = (s & ~0x3L) | COMPLETED
      if (CAS_STACK(s, cs)) true
      else markCompleted()
    } else if (statebits == UNINITIALIZED) {
      val cs = COMPLETED
      if (CAS_STACK(s, cs)) true
      else markCompleted()
    } else false
  }

  @tailrec final def markStolen(): Boolean = {
    val s = READ_STACK
    val statebits = s & 0x3L
    if (statebits == AVAILABLE) {
      val ss = (s & ~0x3L) | STOLEN
      if (CAS_STACK(s, ss)) true
      else markStolen()
    } else if (statebits == UNINITIALIZED) {
      val ss = STOLEN | (S << 2) | (S << 4)
      if (CAS_STACK(s, ss)) true
      else markStolen()
    } else false
  }

  def split: (Stealer[T], Stealer[T]) = {
    var node = root
    var depth = 0
    val origstack = READ_STACK
    var stack = origstack
    stack = stack >>> 2

    while ((stack & 0x3L) == R) {
      node = binary.right(node)
      depth += 1
      stack = stack >>> 2
    }

    // uninitialized stolen: SS
    if ((stack & 0xf) == (S | (S << 2))) {
      val rstack = (L << 2) | (S << 4)
      return (
        if (binary.isEmptyLeaf(node)) new Stealer.Empty[T]
        else new BinaryTreeStealer(binary.left(node), startingDepth + 1, totalElems, binary),
        if (binary.isEmptyLeaf(node)) new Stealer.Empty[T]
        else BinaryTreeStealer(root, startingDepth, totalElems, binary, rstack)
      )
    }

    // R*S
    if ((stack & 0x3L) == S || ((stack & 0x3L) == T && binary.isEmptyLeaf(binary.right(node)))) {
      return (
        new Stealer.Empty[T],
        new Stealer.Empty[T]
      )
    }

    // R*T
    if ((stack & 0x3L) == T) {
      return (
        new Stealer.Empty[T],
        new BinaryTreeStealer(binary.right(node), startingDepth + depth + 1, totalElems, binary)
      )
    }

    if ((stack & 0x3L) == L) {
      val nextstack = stack >>> 2

      // R*LS
      if ((nextstack & 0x3L) == S) {
        return (
          new Stealer.Single[T](binary.value(node)),
          new BinaryTreeStealer(binary.right(node), startingDepth + depth + 1, totalElems, binary)
        )
      }

      // R*LT
      if ((nextstack & 0x3L) == T) {
        val lroot = binary.right(binary.left(node))
        val rstack = (origstack & ((1L << (2 + depth * 2)) - 1)) | (L << (2 + depth * 2)) | (S << (4 + depth * 2))
        return (
          if (binary.isEmptyLeaf(lroot)) new Stealer.Empty[T]
          else new BinaryTreeStealer(lroot, startingDepth + depth + 1, totalElems, binary),
          BinaryTreeStealer(root, startingDepth, totalElems, binary, rstack)
        )
      }

      // R*LR
      if ((nextstack & 0x3L) == R) {
        val lstack = (origstack >>> (6L + depth * 2)) << 2
        val rstack = (origstack & ((1L << (2 + depth * 2)) - 1)) | (L << (2 + depth * 2)) | (S << (4 + depth * 2))
        return (
          BinaryTreeStealer(binary.right(binary.left(node)), startingDepth + depth + 2, totalElems, binary, lstack),
          BinaryTreeStealer(root, startingDepth, totalElems, binary, rstack)
        )
      }

      // R*LL...
      if ((nextstack & 0x3L) == L) {
        val lstack = (origstack >>> (4L + depth * 2)) << 2
        val rstack = (origstack & ((1L << (2 + depth * 2)) - 1)) | (L << (2 + depth * 2)) | (S << (4 + depth * 2))
        return (
          BinaryTreeStealer(binary.left(node), startingDepth + depth + 1, totalElems, binary, lstack),
          BinaryTreeStealer(root, startingDepth, totalElems, binary, rstack)
        )
      }
    }

    throw new IllegalStateException(this.toString)
  }

  def elementsRemainingEstimate: Int = binary.sizeBound(totalElems, startingDepth)

  override def toString = s"BinaryTreeStealer($localDepth)"

  def toString0 = {
    val formattedStack = localStack.map {
      x => if (binary.isEmptyLeaf(x)) "( )" else s" ${binary.value(x)} "
    } mkString(", ")

    s"BinTreeStealer(startDepth: $startingDepth, localDepth: $localDepth, #elems: $totalElems) {\n" +
    s"  stack: ${showStack(READ_STACK)}\n" +
    s"  local: $formattedStack\n" +
    s"}"
  }

}


object BinaryTreeStealer {

  def showStack(s: Long) = {
    val statebits = s & 0x3L
    val state = statebits match {
      case UNINITIALIZED => "UN"
      case AVAILABLE => "AV"
      case STOLEN => "ST"
      case COMPLETED => "CO"
    }
    var pos = 2
    var stack = " "
    while (pos < 64) {
      stack += " " + (((s & (0x3L << pos)) >>> pos) match {
        case L => "L"
        case R => "R"
        case S => "S"
        case T => "T"
      })
      pos += 2
    }
    state + stack
  }

  def apply[T, Node >: Null <: AnyRef: ClassTag](root: Node, startingDepth: Int, totalElems: Int, binary: BinaryTreeStealer.Binary[T, Node], guide: Long) = {
    val stealer = new BinaryTreeStealer(root, startingDepth, totalElems, binary)

    var node = root
    var stack = guide >>> 2
    var top = stack & 0x3L
    while (top != T && top != S) {
      stealer.localStack(stealer.localDepth) = node
      stealer.localDepth += 1
      if (top == L) node = binary.left(node)
      else node = binary.right(node)
      stack = stack >>> 2
      top = stack & 0x3L
    }
    stealer.localStack(stealer.localDepth) = node
    stealer.localDepth += 1
    stealer.WRITE_STACK(AVAILABLE | ((guide >>> 2) << 2))

    stealer
  }

  val STACK_OFFSET = unsafe.objectFieldOffset(classOf[BinaryTreeStealer[_, _]].getDeclaredField("stack"))

  val UNINITIALIZED = 0x0L
  val STOLEN = 0x1L
  val COMPLETED = 0x2L
  val AVAILABLE = 0x3L

  val L = 0x1L
  val R = 0x2L
  val T = 0x0L
  val S = 0x3L

  trait Binary[T, Node >: Null <: AnyRef] {
    def sizeBound(total: Int, depth: Int): Int
    def depthBound(total: Int, depth: Int): Int
    def isEmptyLeaf(n: Node): Boolean
    def left(n: Node): Node
    def right(n: Node): Node
    def value(n: Node): T
  }

  final val MAX_TREE_DEPTH = 31

  class SubtreeIterator[T, Node >: Null <: AnyRef](val binary: Binary[T, Node]) extends Iterator[T] {
    import binary._

    private val stack: Array[Node] = new Array[AnyRef](MAX_TREE_DEPTH + 1).asInstanceOf[Array[Node]]
    private var stackPos = 0 // can be removed but no need as we'll have same object size

    def set(n: Node) {
      // println("set " + n)
      var i = 1;
      while (i < MAX_TREE_DEPTH + 1) {
        stack(i) = null
        i = i + 1
      }
      stack(0) = n
      stackPos = 0
    }

    override def duplicate = {
      val that = new SubtreeIterator(binary)
      java.lang.System.arraycopy(this.stack, 0, that.stack, 0, MAX_TREE_DEPTH + 1)
      that.stackPos = this.stackPos
      (this, that)
    }

    /*  posible stack states are encoded by 2 last entries of stack, ***** (current) (next)
     *  if next != null - we're returning
     *  if next = current.left - than from left, else 
     *  if next = current - that from 'current'
     *  relies on uniqueness of 'node' refferences
     */
    @tailrec
    final def next() = {
      //println("next " + stackPos + " " + stack.mkString("{", " | ", "}"))
      val current = stack(stackPos)
      val next = stack(stackPos + 1)
      if(next!=null) {// we're returning
        //println("we are returning")
        if(left(current) eq next) { // we're returning from leftSubree, return self and clean return flag(next stack entry)
          stack(stackPos + 1) = current
          //println(" returning from left subtree")
          value(current)
        } else if((next eq current) && (right(current) ne null)) { // we've done with this node, go to right sibling
          stack(stackPos + 1) = right(current)
          //println(" going into right sibling")
          stackPos = stackPos + 1
          this.next()
        } else { // we're returning from the right subree, rollup
          stack(stackPos + 1) = null
          stackPos = stackPos - 1
          //println(" rollup")
          this.next()
        }
      } else { // we're diging into new subtree
        if (left(current) ne null) {
          stack(stackPos + 1) = left(current)
          stackPos = stackPos + 1
          this.next()
        } else { // there's no left sibling, return self
          stack(stackPos + 1) = current
          value(current)
        }
      }
    }

    def hasNext = {
      //println("hasNext " + stackPos + "" + stack.mkString("{", " | ", "}"))
      var i = stackPos
      var result = false
      var resultSet = false
      while((i>=0) && !resultSet){
        val current = stack(i)
        val next = stack(i + 1)
        if((next eq null)||  // we need to traverse subtree rooted at current
          (next eq left(current)) || // we need to traverse current
          (next eq current) && (right(current) ne null)){ // we need to traverse right subtree of current
          resultSet = true
          result = true
        }
        i = i - 1
      }
      result
    }

    override def toString = {
    s"SubtreeIterator {\n" +
    s"  stack: ${stack.mkString("{", " | ", "}")}\n" +
    s"  stackPos: ${stackPos}\n" +
    s"}"
  }
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




