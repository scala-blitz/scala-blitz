package scala.collection.workstealing



import sun.misc.Unsafe
import annotation.tailrec
import scala.collection._



trait TreeWorkstealing[T, TreeType >: Null <: AnyRef] extends Workstealing[T] {

  import TreeWorkstealing._

  type N[R] <: TreeNode[T, R]

  type K[R] <: TreeKernel[T, R]

  implicit val isTree: IsTree[TreeType]

  abstract class TreeNode[@specialized S, R](l: Ptr[S, R], r: Ptr[S, R])(val root: TreeType, val stack: Array[AnyRef], initialStep: Int)
  extends Node[S, R](l, r)(initialStep) {
    var padding0: Int = 0
    var padding1: Int = 0
    var padding2: Int = 0
    var padding3: Int = 0
    //var padding4: Int = 0
    //var padding5: Int = 0
    var current: TreeType = null
    var pos: Int = 0

    final def OFFSET(idx: Int): Long = STACK_BASE_OFFSET + idx * STACK_INDEX_SCALE

    final def READ_STACK(idx: Int) = Utils.unsafe.getObject(stack, OFFSET(idx))

    final def CAS_STACK(idx: Int, ov: AnyRef, nv: AnyRef) = Utils.unsafe.compareAndSwapObject(stack, OFFSET(idx), ov, nv)

    private def snatch(node: TreeType) = ???

    private def push(ov: AnyRef, nv: AnyRef): Boolean = ???

    private def pop(ov: AnyRef, nv: AnyRef): Boolean = ???

    private def switch(ov: AnyRef): Boolean = ???

    private def peekcurr: AnyRef = READ_STACK(pos)

    private def peekprev: AnyRef = if (pos > 0) READ_STACK(pos - 1) else NO_PARENT

    private def peeknext: AnyRef = READ_STACK(pos + 1)

    private def isStolen(v: AnyRef) = v.isInstanceOf[StolenValue]

    @tailrec private def move(step: Int): Int = {
      val next = peeknext
      val curr = peekcurr
      val prev = peekprev
      if (isStolen(prev)) {
        markStolen()
        return -1
      } // otherwise neither were the other two stolen when they were read!
      val isLeft = prev match {
        case NO_PARENT => true
        case INNER_DONE => false
        case tree => true
      }

      curr match {
        case INNER_DONE => next match {
          case SUBTREE_DONE =>
            push(SUBTREE_DONE, current.right)
            move(step)
          case null =>
            if (isLeft) pop(INNER_DONE, SUBTREE_DONE)
            else pop(INNER_DONE, null)
            move(step)
        }
        case tree =>
          -1
      }
    }

    /* node interface */

    final def elementsRemaining = ???

    final def elementsCompleted = ???

    final def state = ???

    final def advance(step: Int): Int = move(step)

    final def markCompleted(): Boolean = ???

    final def markStolen(): Boolean = ???    
  }

  abstract class TreeKernel[@specialized S, R] extends Kernel[S, R] {
  }

}


object TreeWorkstealing {

  trait IsTree[TreeType >: Null <: AnyRef] {
    def left(tree: TreeType): TreeType
    def right(tree: TreeType): TreeType
    def size(tree: TreeType): Int
    def height(tree: TreeType): Int
    def isLeaf(tree: TreeType): Boolean
  }

  implicit class TreeOps[T >: Null <: AnyRef: IsTree](tree: T) {
    def left = implicitly[IsTree[T]].left(tree)
    def right = implicitly[IsTree[T]].right(tree)
    def size = implicitly[IsTree[T]].size(tree)
    def height = implicitly[IsTree[T]].height(tree)
    def isLeaf = implicitly[IsTree[T]].isLeaf(tree)
  }

  val STACK_BASE_OFFSET = Utils.unsafe.arrayBaseOffset(classOf[Array[AnyRef]])

  val STACK_INDEX_SCALE = Utils.unsafe.arrayIndexScale(classOf[Array[AnyRef]])

  /* iteration stack values */

  abstract class SpecialValue

  abstract class StolenValue extends SpecialValue

  val NO_PARENT = new SpecialValue {}

  val INNER_DONE = new SpecialValue {}

  val SUBTREE_DONE = new SpecialValue {}

}









