package scala.collection.workstealing



import sun.misc.Unsafe
import annotation.tailrec
import scala.collection._
import scala.reflect.ClassTag



trait TreeWorkstealing[T, TreeType >: Null <: AnyRef] extends Workstealing[T] {

  import TreeWorkstealing._

  type N[R] <: TreeNode[T, R]

  type K[R] <: TreeKernel[T, R]

  implicit val isTree: IsTree[TreeType]

  abstract class TreeNode[@specialized S, R](l: Ptr[S, R], r: Ptr[S, R])(val root: TreeType, val stack: Array[AnyRef], initialStep: Int)
  extends Node[S, R](l, r)(initialStep) {
    var padding0: Int = 0
    //var padding1: Int = 0
    //var padding2: Int = 0
    //var padding3: Int = 0
    //var padding4: Int = 0
    //var padding5: Int = 0
    var pos: Int = 0
    var current: TreeType = null
    val iterstack = new Array[AnyRef](stack.length)
    var iterleft = -1
    var iterdepth = 0

    init()

    private def init() {
      while (stack(pos) ne null) pos += 1
      pos -= 1
    }

    final def OFFSET(idx: Int): Long = STACK_BASE_OFFSET + idx * STACK_INDEX_SCALE

    final def READ_STACK(idx: Int) = Utils.unsafe.getObject(stack, OFFSET(idx))

    final def CAS_STACK(idx: Int, ov: AnyRef, nv: AnyRef) = Utils.unsafe.compareAndSwapObject(stack, OFFSET(idx), ov, nv)

    private def snatch(node: TreeType) = ???

    private def push(ov: AnyRef, nv: TreeType): Boolean = if (CAS_STACK(pos + 1, ov, nv)) {
      pos += 1
      true
    } else false

    private def pop(ov: AnyRef, nv: AnyRef): Boolean = if (CAS_STACK(pos, ov, nv)) {
      pos -= 1
      true
    } else false

    private def switch(ov: AnyRef): Boolean = if (CAS_STACK(pos, ov, INNER_DONE)) {
      current = ov.asInstanceOf[TreeType]
      true
    } else false

    private def peekcurr: AnyRef = READ_STACK(pos)

    private def peekprev: AnyRef = if (pos > 0) READ_STACK(pos - 1) else NO_PARENT

    private def peeknext: AnyRef = READ_STACK(pos + 1)

    private def isStolen(v: AnyRef) = v.isInstanceOf[StolenValue]

    @tailrec private def move(step: Int): Int = if (pos < 0) -1 else {
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
        case tree: TreeType => next match {
          case SUBTREE_DONE =>
            switch(tree)
            val sz = tree.size - tree.left.size - tree.right.size
            prepareIteration(sz, SINGLE_NODE, tree)
            sz
          case null =>
            val nv = if (isLeft) SUBTREE_DONE else null
            if (tree.isLeaf || tree.size <= step) {
              if (pop(tree, nv)) {
                prepareIteration(tree.size, 0, tree)
                tree.size
              } else move(step)
            } else {
              push(null, tree.left)
              move(step)
            }
        }
      }
    }

    def prepareIteration(chunk: Int, depth: Int, current: TreeType) {
      iterleft = chunk
      iterdepth = depth
      iterstack(0) = current
    }

    /* node interface */

    final def elementsRemaining = ???

    final def elementsCompleted = ???

    final def state = ???

    final def advance(step: Int): Int = {
      val chunk = move(step)
      iterleft = chunk
      chunk
    }

    final def next(): S = if (iterleft > 0) {
      iterleft -= 1
      if (iterdepth == SINGLE_NODE) iterstack(0).asInstanceOf[TreeType].element[S]
      else {
        //val res = iterstack(iterdepth)
        null.asInstanceOf[S]
      }
    } else throw new NoSuchElementException

    final def markCompleted(): Boolean = ???

    final def markStolen(): Boolean = ???

  }

  abstract class TreeKernel[@specialized S, R] extends Kernel[S, R] {
  }

}


object TreeWorkstealing {

  trait IsTree[TreeType >: Null <: AnyRef] {
    def element[@specialized S](tree: TreeType): S
    def left(tree: TreeType): TreeType
    def right(tree: TreeType): TreeType
    def size(tree: TreeType): Int
    def height(tree: TreeType): Int
    def isLeaf(tree: TreeType): Boolean
  }

  implicit class TreeOps[T >: Null <: AnyRef](val tree: T) extends AnyVal {
    def element[@specialized S](implicit isTree: IsTree[T]) = isTree.element(tree)
    def left(implicit isTree: IsTree[T]) = isTree.left(tree)
    def right(implicit isTree: IsTree[T]) = isTree.right(tree)
    def size(implicit isTree: IsTree[T]) = isTree.size(tree)
    def height(implicit isTree: IsTree[T]) = isTree.height(tree)
    def isLeaf(implicit isTree: IsTree[T]) = isTree.isLeaf(tree)
  }

  def initializeStack[TreeType >: Null <: AnyRef: IsTree](root: TreeType): Array[AnyRef] = {
    if (root eq null) Array(SUBTREE_DONE, null)
    else {
      val array = new Array[AnyRef](root.height + 2)
      array(0) = root
      array
    }
  }

  val STACK_BASE_OFFSET = Utils.unsafe.arrayBaseOffset(classOf[Array[AnyRef]])

  val STACK_INDEX_SCALE = Utils.unsafe.arrayIndexScale(classOf[Array[AnyRef]])

  val SINGLE_NODE = -1

  /* iteration stack values */

  abstract class SpecialValue

  abstract class StolenValue extends SpecialValue

  val NO_PARENT = new SpecialValue {}

  val INNER_DONE = new SpecialValue {}

  val SUBTREE_DONE = new SpecialValue {}

}









