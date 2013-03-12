package scala.collection.workstealing



import scala.annotation.unchecked.{uncheckedVariance => uV}
import scala.reflect.ClassTag



trait Conc[T] extends ParIterable[T]
with ParIterableLike[T, Conc[T]]
with TreeWorkstealing[T, Conc[T]] {

  type N[R] = ConcNode[R]

  type K[R] = ConcKernel[T, R]

  final class ConcNode[R](l: Ptr[T, R], r: Ptr[T, R])(rt: Conc[T], st: Array[AnyRef], fe: Int, te: Int, is: Int)
  extends TreeNode[T, R](l, r)(rt, st, fe, te, is) {
    def createIterator = new DefaultIterator[T] {
      def extractElement(t: Conc[T]) = t.element
    }
    def newTreeNode(l: Ptr[T, R], r: Ptr[T, R])(root: Conc[T], stack: Array[AnyRef], firstElem: Int, totalElems: Int, initStep: Int) = {
      new ConcNode(l, r)(root, stack, firstElem, totalElems, initStep)
    }
  }

  abstract class ConcKernel[S, R] extends TreeKernel[S, R] {
  }

  protected[this] def newCombiner = ???

  implicit final def isTree = Conc.isTree[T]

  def config = Workstealing.DefaultConfig // TODO see how to fix this

  def newRoot[R]: Ptr[T, R] = {
    val root = this
    val stack = TreeWorkstealing.initializeStack(root)(isTree)
    val wsnd = new ConcNode[R](null, null)(root, stack, 0, root.size, config.initialStep)
    new Ptr[T, R](null, 0)(wsnd)
  }

  def ||(that: Conc[T]) = Conc.||(this, that)

  def ||(elem: T) = Conc.||(this, Conc.Single(elem))

  def size: Int

  def height: Int

  def left: Conc[T]

  def right: Conc[T]

  def element: T

}


object Conc {

  import TreeWorkstealing.IsTree

  final implicit def nil2concT[T](n: Nil.type) = Nil.asInstanceOf[Conc[T]]

  class ConcCombiner[@specialized T] extends Combiner[T, Conc[T]] with CombinerLike[T, Conc[T], ConcCombiner[T]] {
    private var elements: Conc[T] = Nil

    def +=(elem: T): this.type = {
      elements = elements || Single(elem)
      this
    }

    def clear() {
      elements = Nil
    }

    def combine(that: ConcCombiner[T]) = {
      this.elements = this.elements || that.elements
      this
    }

    def result = elements
  }

  private val concListIsTree = new IsTree[Conc[Nothing]] {
    def left(tree: Conc[Nothing]) = tree.left
    def right(tree: Conc[Nothing]) = tree.right
    def size(tree: Conc[Nothing]) = tree.size
    def height(tree: Conc[Nothing]) = tree.height
    def isLeaf(tree: Conc[Nothing]) = tree match {
      case _ || _ => false
      case _ => true
    }
    def tag: ClassTag[Conc[Nothing]] = implicitly[ClassTag[Conc[Nothing]]]
    def external = true
  }

  final def isTree[T] = concListIsTree.asInstanceOf[IsTree[Conc[T]]]

  case class ||[T](left: Conc[T], right: Conc[T]) extends Conc[T] {
    val size: Int = left.size + right.size
    val height: Int = math.max(left.height, right.height) + 1
    def element = throw new UnsupportedOperationException
  }

  case class Single[@specialized T](element: T) extends Conc[T] {
    def size = 1
    def height = 0
    def left = throw new UnsupportedOperationException
    def right = throw new UnsupportedOperationException
  }

  case object Nil extends Conc[Nothing] {
    def size = 0
    def height = 0
    def left = throw new UnsupportedOperationException
    def right = throw new UnsupportedOperationException
    def element = throw new UnsupportedOperationException
  }

  implicit class ConcOps[T](val elem: T) extends AnyVal {
    def ||(that: T) = Single(elem) || Single(that)
    def ||(that: Conc[T]) = Single(elem) || that
  }

}

