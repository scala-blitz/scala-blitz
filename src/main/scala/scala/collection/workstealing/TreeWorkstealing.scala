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

  abstract class TreeNode[@specialized S, R](l: Ptr[S, R], r: Ptr[S, R])(val root: TreeType, val stack: Array[AnyRef], val totalElems: Int, initStep: Int)
  extends Node[S, R](l, r)(initStep) {
    var padding0: Int = 0
    var padding1: Int = 0
    //var padding2: Int = 0
    //var padding3: Int = 0
    //var padding4: Int = 0
    //var padding5: Int = 0
    var pos: Int = 0
    var current: TreeType = null
    var totalLeft: Int = totalElems
    val iter = createIterator

    override def nodeString = "TreeNode(%s)(%s)".format(
      if (owner == null) "none" else "worker " + owner.index,
      stack.mkString(", ")
    )

    def createIterator: TreeIterator[S]

    def newTreeNode(l: Ptr[S, R], r: Ptr[S, R])(root: TreeType, stack: Array[AnyRef], totalElems: Int, initStep: Int): TreeNode[S, R]

    trait TreeIterator[@specialized Q] {
      def initializeWithSubtree(t: TreeType, elems: Int): Unit
      def next(): Q
    }

    abstract class DefaultIterator[@specialized Q](val stack: Array[TreeType], var depth: Int, var left: Int)
    extends TreeIterator[Q] {
      def this() = this(isTree.tag.newArray(stack.length), 0, 0)

      def extractElement(tree: TreeType): Q

      final def initializeWithSubtree(t: TreeType, elems: Int) = prepare(t, elems)
      def push(v: TreeType) {
        depth += 1
        stack(depth) = v
      }
      def pop() = if (depth >= 0) {
        val res = stack(depth)
        depth -= 1
        res
      } else null
      def peek = if (depth >= 0) stack(depth) else null
      def valuePeek(d: Int): Q = extractElement(stack(d))
      def prepare(current: TreeType, chunk: Int) {
        if (chunk == SINGLE_NODE) {
          left = 1
          depth = SINGLE_NODE
          stack(0) = current
        } else {
          left = chunk
          depth = 0
          stack(0) = current
          while (!peek.isLeaf) push(peek.left)
        }
      }
      def move() {
        val n = pop()
        if (isTree.external) {
          if (peek != null && !peek.isLeaf) {
            val inner = pop()
            push(inner.right)
            while (!peek.isLeaf) push(peek.left)
          }
        } else {
          if (!n.isLeaf) {
            push(n.right)
            while (!peek.isLeaf) push(peek.left)
          }
        }
      }
      def next(): Q = if (left > 0) {
        left -= 1
        if (depth == SINGLE_NODE) valuePeek(0)
        else {
          val res = valuePeek(depth)
          move()
          res
        }
      } else throw new NoSuchElementException
    }

    init()

    private def init() {
      @tailrec def init(tree: TreeType, depth: Int): Int = {
        val top = READ_STACK(depth)
        if ((top eq SUBTREE_DONE) || (top eq null) || (top eq STOLEN_COMPLETED) || (top eq STOLEN_NULL)) depth - 1
        else {
          val goRight = (top eq STOLEN_RIGHT) || (top eq INNER_DONE)
          if (goRight) current = tree
          val child = if (goRight) tree.right else tree.left
          init(child, depth + 1)
        }
      }
      pos = init(root, 0)
    }

    final def OFFSET(idx: Int): Long = STACK_BASE_OFFSET + idx * STACK_INDEX_SCALE

    final def READ_STACK(idx: Int) = Utils.unsafe.getObject(stack, OFFSET(idx))

    final def CAS_STACK(idx: Int, ov: AnyRef, nv: AnyRef) = Utils.unsafe.compareAndSwapObject(stack, OFFSET(idx), ov, nv)

    final def WRITE_STACK(idx: Int, nv: AnyRef) = Utils.unsafe.putObject(stack, OFFSET(idx), nv)

    private def snatch(idx: Int, v: AnyRef): Boolean = v match {
      case s: StolenValue =>
        // nothing
        true
      case INNER_DONE =>
        CAS_STACK(idx, INNER_DONE, STOLEN_RIGHT)
      case SUBTREE_DONE =>
        if (READ_STACK(idx - 1) eq STOLEN_RIGHT) CAS_STACK(idx, SUBTREE_DONE, STOLEN_NULL)
        else CAS_STACK(idx, SUBTREE_DONE, STOLEN_COMPLETED)
      case null =>
        if (READ_STACK(idx - 1) eq STOLEN_RIGHT) CAS_STACK(idx, null, STOLEN_COMPLETED)
        else CAS_STACK(idx, null, STOLEN_NULL)
      case tree: TreeType =>
        if (tree.isLeaf) CAS_STACK(idx, tree, STOLEN_NULL)
        else CAS_STACK(idx, tree, STOLEN_LEFT)
    }

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

    private def safeReadStack(idx: Int): AnyRef = if (idx >= 0) READ_STACK(idx) else NO_PARENT

    private def peekprev: AnyRef = safeReadStack(pos - 1)

    private def peeknext: AnyRef = READ_STACK(pos + 1)

    private def isStolen(v: AnyRef) = v.isInstanceOf[StolenValue]

    private def checkIfLeft(parent: AnyRef) = parent match {
      case NO_PARENT => true
      case INNER_DONE => false
      case tree => true
    }

    @tailrec private def move(step: Int): Int = if (pos < 0) -1 else {
      val next = peeknext
      val curr = peekcurr
      val prev = peekprev
      if (isStolen(prev) || (prev == NO_PARENT && isStolen(curr))) {
        markStolen()
        return -1
      } // otherwise neither were the other two stolen when they were read!
      val isLeft = checkIfLeft(prev)

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
        case tree: TreeType if isTree.check(tree) => next match {
          case SUBTREE_DONE =>
            switch(tree)
            iter.initializeWithSubtree(tree, SINGLE_NODE)
            if (isTree.external) 0 else 1
          case null =>
            val nv = if (isLeft) SUBTREE_DONE else null
            if (tree.isLeaf || tree.size <= step) {
              if (pop(tree, nv)) {
                iter.initializeWithSubtree(tree, tree.size)
                tree.size
              } else move(step)
            } else {
              push(null, tree.left)
              move(step)
            }
        }
      }
    }

    /**
     *  Precondition: depth > 0
     */
    @tailrec private def steal(depth: Int): Unit = READ_STACK(depth) match {
      case STOLEN_NULL =>
        // done
      case sv: StolenValue =>
        steal(depth + 1)
      case tree =>
        snatch(depth, tree)
        steal(depth)
    }

    @tailrec private def countCompletedElements(tree: TreeType, depth: Int, count: Int): Int = {
      val v = READ_STACK(depth) match {
        case cv: Completed => cv.v
        case x => x
      }
      v match {
        case INNER_DONE | STOLEN_RIGHT =>
          val sz = if (isTree.external) 0 else 1
          if (tree.isLeaf) sz
          else countCompletedElements(tree.right, depth + 1, tree.left.size + sz + count)
        case SUBTREE_DONE | null | STOLEN_COMPLETED =>
          count + tree.size
        case STOLEN_NULL =>
          count + 0
        case STOLEN_LEFT | _ =>
          countCompletedElements(tree.left, depth + 1, count)
      }
    }

    private def decode() {
      @tailrec def decode(tree: TreeType, depth: Int): Unit = READ_STACK(depth) match {
        case STOLEN_LEFT =>
          WRITE_STACK(depth, tree)
          decode(tree.left, depth + 1)
        case STOLEN_RIGHT =>
          WRITE_STACK(depth, INNER_DONE)
          decode(tree.right, depth + 1)
        case STOLEN_NULL if tree eq null =>
          WRITE_STACK(depth, null)
        case STOLEN_NULL =>
          val isLeft = checkIfLeft(safeReadStack(depth - 1))
          if (isLeft) WRITE_STACK(depth, null)
          else WRITE_STACK(depth, SUBTREE_DONE)
        case STOLEN_COMPLETED =>
          val isLeft = checkIfLeft(safeReadStack(depth - 1))
          if (isLeft) WRITE_STACK(depth, SUBTREE_DONE)
          else WRITE_STACK(depth, null)
          decode(null, depth + 1)
        case x =>
          sys.error("unreachable: " + x + " at depth " + depth + ", reread: " + READ_STACK(depth) + " node: " + this.nodeString)
      }
      decode(root, 0)
    }

    @tailrec private def unwindStack(): Unit = READ_STACK(0) match {
      case sv: StolenValue =>
        markStolen()
      case cv: Completed =>
        // done
      case tree =>
        CAS_STACK(0, tree, new Completed(tree))
        unwindStack()
    }

    /* node interface */

    final def elementsRemaining = totalElems - elementsCompleted

    final def elementsCompleted = countCompletedElements(root, 0, 0)

    final def state = READ_STACK(0) match {
      case s: StolenValue =>
        markStolen()
        Workstealing.StolenOrExpanded
      case c: Completed =>
        Workstealing.Completed
      case _ =>
        Workstealing.AvailableOrOwned
    }

    final def advance(step: Int): Int = if (totalLeft <= 0) {
      unwindStack()
      -1
    } else {
      val chunk = move(math.min(totalLeft, step))
      totalLeft -= chunk
      chunk
    }

    final def next(): S = iter.next()

    final def markCompleted(): Boolean = {
      while (state eq Workstealing.AvailableOrOwned) advance(Int.MaxValue)
      state eq Workstealing.Completed
    }

    @tailrec final def markStolen(): Boolean = READ_STACK(0) match {
      case cv: Completed =>
        false
      case sv: StolenValue =>
        steal(1)
        true
      case tree =>
        snatch(0, tree)
        markStolen()
    }

    def newExpanded(parent: Ptr[S, R]): TreeNode[S, R] = {
      val elemsLeft = elementsRemaining
      var minForLeft = elemsLeft / 2

      val snapshot = newTreeNode(null, null)(root, stack, totalElems, Workstealing.initialStep)
      snapshot.decode()
      val lstack = snapshot.stack.clone
      val lpos = snapshot.pos
      val lcurrent = snapshot.current
      @tailrec def take(inLeft: Int, request: Int): Int = if (inLeft < minForLeft) {
        val chunk = snapshot.advance(request)
        if (chunk != -1) take(inLeft + chunk, request - chunk)
        else inLeft
      } else inLeft
      val totalInLeft = take(0, minForLeft)
      val rstack = snapshot.stack.clone
      val rpos = snapshot.pos
      val rcurrent = snapshot.current

      val lnode = newTreeNode(null, null)(root, lstack, totalInLeft, Workstealing.initialStep)
      lnode.current = lcurrent
      lnode.pos = lpos
      val rnode = newTreeNode(null, null)(root, rstack, elemsLeft - totalInLeft, Workstealing.initialStep)
      rnode.current = rcurrent
      rnode.pos = rpos
      val lptr = new Ptr[S, R](parent, parent.level + 1)(lnode)
      val rptr = new Ptr[S, R](parent, parent.level + 1)(rnode)
      val nnode = newTreeNode(lptr, rptr)(root, stack, totalElems, Workstealing.initialStep)
      nnode.owner = this.owner
      nnode
    }

  }

  abstract class TreeKernel[@specialized S, R] extends Kernel[S, R] {
  }

}


object TreeWorkstealing {

  abstract class IsTree[TreeType >: Null <: AnyRef] {
    def left(tree: TreeType): TreeType
    def right(tree: TreeType): TreeType
    def size(tree: TreeType): Int
    def height(tree: TreeType): Int
    def isLeaf(tree: TreeType): Boolean
    def tag: ClassTag[TreeType]
    def external: Boolean

    def innerSize = if (external) 0 else 1
    def check(x: AnyRef) = tag.runtimeClass.isInstance(x)
  }

  implicit class TreeOps[T >: Null <: AnyRef](val tree: T) extends AnyVal {
    def left(implicit isTree: IsTree[T]) = isTree.left(tree)
    def right(implicit isTree: IsTree[T]) = isTree.right(tree)
    def size(implicit isTree: IsTree[T]) = isTree.size(tree)
    def height(implicit isTree: IsTree[T]) = isTree.height(tree)
    def isLeaf(implicit isTree: IsTree[T]) = isTree.isLeaf(tree)
  }

  def cacheLineSizeMultiple(sz: Int) = (sz / 16 + 1) * 16

  def initializeStack[TreeType >: Null <: AnyRef, S](root: TreeType)(implicit isTree: IsTree[TreeType]): Array[AnyRef] = {
    if (root eq null) Array(SUBTREE_DONE, null)
    else {
      val array = new Array[AnyRef](cacheLineSizeMultiple(root.height + 2))
      array(0) = root
      array
    }
  }

  val STACK_BASE_OFFSET = Utils.unsafe.arrayBaseOffset(classOf[Array[AnyRef]])

  val STACK_INDEX_SCALE = Utils.unsafe.arrayIndexScale(classOf[Array[AnyRef]])

  val SINGLE_NODE = -1

  /* iteration stack values */

  abstract class SpecialValue

  val NO_PARENT = new SpecialValue {
    override def toString = "NO_PARENT"
  }

  val INNER_DONE = new SpecialValue {
    override def toString = "INNER_DONE"
  }

  val SUBTREE_DONE = new SpecialValue {
    override def toString = "SUBTREE_DONE"
  }

  abstract class StolenValue extends SpecialValue

  val STOLEN_LEFT = new StolenValue {
    override def toString = "STOLEN_LEFT"
  }

  val STOLEN_RIGHT = new StolenValue {
    override def toString = "STOLEN_RIGHT"
  }

  val STOLEN_COMPLETED = new StolenValue {
    override def toString = "STOLEN_COMPLETED"
  }

  val STOLEN_NULL = new StolenValue {
    override def toString = "STOLEN_NULL"
  }

  final class Completed(val v: AnyRef) extends SpecialValue {
    override def toString = "Completed(" + v + ")"
  }

}









