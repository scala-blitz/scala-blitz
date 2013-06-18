package scala.collection.parallel
package workstealing



import scala.annotation.tailrec
import scala.reflect.ClassTag



trait TreeStealer[T, N >: Null <: AnyRef] extends Stealer[T] {
  import TreeStealer._

  val isTree: IsTree[N]
  val root: N
  val totalSize: Int
  val pathStack = new Array[Int](isTree.depthBound(totalSize) + 1)
  val nodeStack = isTree.classTag.newArray(isTree.depthBound(totalSize) + 1)
  var depth: Int = 0

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

  def encodeStolen(origin: Int, progress: Int): Int = {
    SPECIAL_MASK |
    STOLEN_MASK |
    ((origin << ORIGIN_SHIFT) & ORIGIN_MASK) |
    ((progress << PROGRESS_SHIFT) & PROGRESS_MASK)
  }

  def encodeCompleted(origin: Int, progress: Int): Int = {
    SPECIAL_MASK |
    COMPLETED_MASK |
    ((origin << ORIGIN_SHIFT) & ORIGIN_MASK) |
    ((progress << PROGRESS_SHIFT) & PROGRESS_MASK)
  }

  def completed(code: Int): Boolean = ((code & COMPLETED_MASK) >>> COMPLETED_SHIFT) != 0

  def stolen(code: Int): Boolean = ((code & STOLEN_MASK) >>> STOLEN_SHIFT) != 0

  def origin(code: Int): Int = ((code & ORIGIN_MASK) >>> ORIGIN_SHIFT)

  def progress(code: Int): Int = ((code & PROGRESS_MASK) >>> PROGRESS_SHIFT)

  def push(ov: Int, nv: Int): Boolean = if (CAS_STACK(depth + 1, ov, nv)) {
    val cidx = origin(nv)
    nodeStack(depth + 1) = isTree.child(nodeStack(depth), cidx)
    depth += 1
    true
  } else false

  def pop(ov: Int, nv: Int): Boolean = if (CAS_STACK(depth, ov, nv)) {
    nodeStack(depth) = null
    depth -= 1
    true
  } else false

  def switch(ov: Int, nv: Int): Boolean = if (CAS_STACK(depth, ov, nv)) {
    true
  } else false

}


object TreeStealer {

  val STACK_BASE_OFFSET = unsafe.arrayBaseOffset(classOf[Array[Int]])
  val STACK_INDEX_SCALE = unsafe.arrayIndexScale(classOf[Array[Int]])
  val PROGRESS_SHIFT = 0
  val PROGRESS_MASK = 0x00000fff
  val ORIGIN_SHIFT = 12
  val ORIGIN_MASK = 0x00fff000
  val STOLEN_SHIFT = 24
  val STOLEN_MASK = 0x01000000
  val COMPLETED_SHIFT = 25
  val COMPLETED_MASK = 0x02000000
  val SPECIAL_SHIFT = 31
  val SPECIAL_MASK = 0x80000000

  abstract class IsTree[N] {
    def totalChildren(n: N): Int
    def child(n: N, idx: Int): N
    def isLeaf(n: N): Boolean
    def isExternal: Boolean
    def estimateSubtree(depth: Int, totalSize: Int): Int
    def depthBound(totalSize: Int): Int
    def classTag: ClassTag[N]
  }

  abstract class IsExternalTree[@specialized(Int, Long, Float, Double) T, N] extends IsTree[N] {
    def isExternal = true
    def elementAtLeaf(idx: Int): T
  }

  trait External[@specialized(Int, Long, Float, Double) T, N >: Null <: AnyRef] extends TreeStealer[T, N] {
    val isTree: IsExternalTree[T, N]

    final def state = {
      val code = READ_STACK(0)
      if (completed(code)) Stealer.Completed
      else if (stolen(code)) Stealer.StolenOrExpanded
      else Stealer.AvailableOrOwned
    }

    final def advance(step: Int): Int = ???

    @tailrec final def markCompleted(): Boolean = {
      val code = READ_STACK(0)
      if (stolen(code)) {
        markStolen()
        false
      } else {
        val ncode = encodeCompleted(origin(code), progress(code))
        if (CAS_STACK(0, code, ncode)) true
        else markCompleted()
      }
    }

    final def markStolen(): Boolean = ???

    def split: (Stealer[T], Stealer[T]) = ???

    def elementsRemainingEstimate: Int = ???

  }

}




