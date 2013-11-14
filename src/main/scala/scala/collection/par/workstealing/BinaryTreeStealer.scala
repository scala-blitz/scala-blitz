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
  localStack(0) = root

  // go to the leftmost node
  var initialized = false
  var initialStack = 0L
  
  {
    var pos = 2
    var node = root
    while (!binary.isEmptyLeaf(node)) {
      node = binary.left(node)
      initialStack = initialStack | (L << pos)
      pos += 2
    }
  }

  /* atomic state */
  @volatile var stack: Long = _

  final def READ_STACK = unsafe.getLongVolatile(this, STACK_OFFSET)

  final def WRITE_STACK(nv: Long) = unsafe.putLongVolatile(this, STACK_OFFSET, nv)

  final def CAS_STACK(ov: Long, nv: Long) = unsafe.compareAndSwapLong(this, STACK_OFFSET, ov, nv)

  def next(): T = iterator.next()

  def hasNext: Boolean = iterator.hasNext

  def state: Stealer.State = {
    val s = READ_STACK
    val statebits = s & 0x3
    if (statebits == AVAILABLE) Stealer.AvailableOrOwned
    else if (statebits == COMPLETED) Stealer.Completed
    else Stealer.StolenOrExpanded
  }

  final def advance(step: Int): Int = {
    val s = READ_STACK
    val statebits = s & 0x3
    if (statebits != AVAILABLE) -1
    else {
      if (initialized) {
        ???
      } else {
        ???
      }
    }
  }

  @tailrec final def markCompleted(): Boolean = {
    val s = READ_STACK
    val statebits = s & 0x3
    if (statebits == AVAILABLE) {
      val cs = s | COMPLETED
      if (CAS_STACK(s, cs)) true
      else markCompleted()
    } else false
  }

  @tailrec final def markStolen(): Boolean = {
    val s = READ_STACK
    val statebits = s & 0x3
    if (statebits == AVAILABLE) {
      val ss = s | STOLEN
      if (CAS_STACK(s, ss)) true
      else markStolen()
    } else false
  }

  def split: (Stealer[T], Stealer[T]) = ???

  def elementsRemainingEstimate: Int = ???  

}


object BinaryTreeStealer {

  val STACK_OFFSET = unsafe.objectFieldOffset(classOf[BinaryTreeStealer[_, _]].getDeclaredField("stack"))

  val STOLEN = 0x1
  val COMPLETED = 0x2
  val AVAILABLE = 0x0

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




