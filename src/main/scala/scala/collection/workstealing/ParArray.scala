package scala.collection.workstealing



import sun.misc.Unsafe
import annotation.tailrec
import scala.collection._
import scala.reflect.ClassTag



class ParArray[@specialized T: ClassTag](val array: Array[T], val config: Workstealing.Config) extends ParIterable[T]
with ParIterableLike[T, ParArray[T]]
with IndexedWorkstealing[T] {

  import IndexedWorkstealing._

  def this(sz: Int, c: Workstealing.Config) = this(new Array[T](sz), c)

  def size = array.length

  type N[R] = ArrayNode[T, R]

  type K[R] = ArrayKernel[T, R]

  protected[this] def newCombiner = new ParArray.ArrayCombiner[T]

  final class ArrayNode[@specialized S, R](l: Ptr[S, R], r: Ptr[S, R])(val arr: Array[S], s: Int, e: Int, rn: Long, st: Int)
  extends IndexNode[S, R](l, r)(s, e, rn, st) {
    var lindex = start

    def next(): S = {
      val i = lindex
      lindex = i + 1
      arr(i)
    }

    def newExpanded(parent: Ptr[S, R], worker: Workstealing.Worker, kernel: Kernel[S, R]): ArrayNode[S, R] = {
      val r = /*READ*/range
      val p = positiveProgress(r)
      val u = until(r)
      val remaining = u - p
      val firsthalf = remaining / 2
      val secondhalf = remaining - firsthalf
      val lnode = new ArrayNode[S, R](null, null)(arr, p, p + firsthalf, createRange(p, p + firsthalf), config.initialStep)
      val rnode = new ArrayNode[S, R](null, null)(arr, p + firsthalf, u, createRange(p + firsthalf, u), config.initialStep)
      val lptr = new Ptr[S, R](parent, parent.level + 1)(lnode)
      val rptr = new Ptr[S, R](parent, parent.level + 1)(rnode)
      val nnode = new ArrayNode(lptr, rptr)(arr, start, end, r, step)
      nnode.owner = this.owner
      nnode
    }

  }

  abstract class ArrayKernel[@specialized S, R] extends IndexKernel[S, R] {
    override def isNotRandom = true
  }

  def newRoot[R] = {
    val work = new ArrayNode[T, R](null, null)(array, 0, size, createRange(0, size), config.initialStep)
    val root = new Ptr[T, R](null, 0)(work)
    root
  }

}


object ParArray {

  private val COMBINER_CHUNK_SIZE_LIMIT = 4096

  trait Tree {
    def level: Int
    def size: Int
    def nodeString: String
  }

  class Chunk[@specialized T: ClassTag](val array: Array[T]) extends Tree {
    var size = Int.MinValue
    def level = 0
    override def nodeString = "Chunk(total: " + size + ")"
  }

  class Node[@specialized T](val left: Tree, val right: Tree, val level: Int, val size: Int) extends Tree {
    override def nodeString = "Node(" + level + ", " + size + ")(" + left.nodeString + ", " + right.nodeString + ")"
  }

  abstract class ChunkCombiner[@specialized T: ClassTag, To](init: Boolean)
  extends Combiner[T, To] with CombinerLike[T, To, ChunkCombiner[T, To]] {
    private[ParArray] var chunksize: Int = _
    private[ParArray] var lastarr: Array[T] = _
    private[ParArray] var lastpos: Int = _
    private[ParArray] var lasttree: Chunk[T] = _
    private[ParArray] var chunkstack: List[Tree] = _

    def this() = this(true)

    if (init) clear()

    private[ParArray] def createChunk(): Chunk[T] = {
      chunksize = math.min(chunksize * 4, COMBINER_CHUNK_SIZE_LIMIT)
      val array = implicitly[ClassTag[T]].newArray(chunksize)
      val chunk = new Chunk[T](array)
      chunk
    }

    private[ParArray] def closeLast() {
      lasttree.size = lastpos
      mergeStack()
    }

    private[ParArray] def mergeStack() = {
      @tailrec def merge(stack: List[Tree]): List[Tree] = stack match {
        case (c1: Chunk[T]) :: (c2: Chunk[T]) :: tail =>
          merge(new Node[T](c2, c1, 1, c1.size + c2.size) :: tail)
        case (n1: Node[T]) :: (n2: Node[T]) :: tail if n1.level == n2.level =>
          merge(new Node[T](n2, n1, n1.level + 1, n1.size + n2.size) :: tail)
        case _ =>
          stack
      }

      chunkstack = merge(chunkstack)
    }

    private[ParArray] def computeTree = {
      @tailrec def merge(stack: List[Tree]): Tree = stack match {
        case t1 :: t2 :: tail => merge(new Node(t2, t1, math.max(t1.level, t2.level) + 1, t1.size + t2.size) :: tail)
        case t :: Nil => t
      }

      merge(chunkstack)
    }

    def +=(elem: T): ChunkCombiner[T, To] = {
      if (lastpos < chunksize) {
        lastarr(lastpos) = elem
        lastpos += 1
        this
      } else {
        closeLast()
        lasttree = createChunk()
        lastarr = lasttree.array
        lastpos = 0
        chunkstack = lasttree :: chunkstack
        +=(elem)
      }
    }

    def newChunkCombiner(shouldInit: Boolean): ChunkCombiner[T, To]

    def result: To
  
    def combine(that: ChunkCombiner[T, To]): ChunkCombiner[T, To] = {
      this.closeLast()

      val res = newChunkCombiner(false)
      res.chunksize = math.max(this.chunksize, that.chunksize)
      res.lastarr = that.lastarr
      res.lastpos = that.lastpos
      res.lasttree = that.lasttree
      res.chunkstack = that.chunkstack ::: List(this.computeTree)

      res
    }

    def clear(): Unit = {
      chunksize = 16
      lasttree = createChunk()
      lastarr = lasttree.array
      lastpos = 0
      chunkstack = lasttree :: Nil
    }

  }

  val treeIsTree = {
    import TreeWorkstealing._
    new IsTree[Tree] {
      def left(tree: Tree): Tree = tree.asInstanceOf[Node[_]].left
      def right(tree: Tree): Tree = tree.asInstanceOf[Node[_]].right
      def size(tree: Tree): Int = tree.size
      def height(tree: Tree): Int = tree.level
      def isLeaf(tree: Tree): Boolean = tree.isInstanceOf[Chunk[_]]
      def tag: ClassTag[Tree] = implicitly[ClassTag[Tree]]
      def external = true
    }
  }

  final class ArrayCombiner[@specialized T: ClassTag](init: Boolean) extends ChunkCombiner[T, ParArray[T]](init)
  with TreeWorkstealing[T, Tree] {
    def this() = this(true)

    val isTree = treeIsTree

    def config = Workstealing.DefaultConfig

    def size = computeTree.size

    type N[R] = ArrayCombinerNode[T, R]

    final class ArrayCombinerNode[@specialized S, R](l: Ptr[S, R], r: Ptr[S, R])(rt: Tree, st: Array[AnyRef], fe: Int, te: Int, is: Int)
    extends TreeNode[S, R](l, r)(rt, st, fe, te, is) {

      final class ExternalTreeIterator[@specialized Q](var chunk: Array[Q], var pos: Int, var total: Int)
      extends TreeIterator[Q] {
        def this() = this(null, 0, 0)
  
        final var subtree: Chunk[Q] = null
        final def isSingle = chunk eq null
        final def elements = if (isSingle) isTree.innerSize else total - pos
        @tailrec final def initializeWithSubtree(t: Tree, elems: Int) = t match {
          case nd: ParArray.Node[q] =>
            if (nd.size == 1 && elems == 1) {
              if (nd.left.size == 1) initializeWithSubtree(nd.left, elems)
              else initializeWithSubtree(nd.right, elems)
            } else if (nd.size == 0 && elems == 0) {
              chunk = null
              pos = 0
              total = 0
              subtree = null
            } else {
              assert(elems == TreeWorkstealing.SINGLE_NODE, nd.size + ", elems " + elems + " vs. " + TreeWorkstealing.SINGLE_NODE)
              chunk = null
              pos = 0
              total = 0
              subtree = null
            }
          case ch: Chunk[Q] =>
            chunk = ch.array
            pos = 0
            total = ch.size
            subtree = ch
        }
        def next() = if (pos < total) {
          val res = chunk(pos)
          pos += 1
          res
        } else throw new NoSuchElementException
        override def toString = s"ExternalTreeIterator(position: $pos, total: $total)"
      }

      override def minimumStealThreshold = COMBINER_CHUNK_SIZE_LIMIT

      def createIterator = new ExternalTreeIterator[S]

      def newTreeNode(l: Ptr[S, R], r: Ptr[S, R])(root: Tree, stack: Array[AnyRef], firstElem: Int, totalElems: Int, initStep: Int) = {
        new ArrayCombinerNode(l, r)(root, stack, firstElem, totalElems, initStep)
      }
    }

    def newChunkCombiner(shouldInit: Boolean) = new ArrayCombiner(shouldInit)
    
    def newRoot[R]: Ptr[T, R] = {
      val root = computeTree
      val stack = TreeWorkstealing.initializeStack(root)(treeIsTree)
      val wsnd = new ArrayCombinerNode[T, R](null, null)(root, stack, 0, root.size, config.initialStep)
      new Ptr[T, R](null, 0)(wsnd)
    }

    def result = {
      closeLast()
      val wstree = newRoot[Unit]
      val array = new Array[T](wstree.child.repr.root.size)

      type Status = ParIterableLike.CopyToArrayStatus
      val kernel = new TreeKernel[T, Status] {
        override def maximumChunkSize = 1
        override def beforeWorkOn(tree: Ptr[T, Status], node: Node[T, Status]) {
        }
        override def afterCreateRoot(root: Ptr[T, Status]) {
          root.child.lresult = new Status(0, 0)
        }
        override def afterExpand(old: Node[T, Status], node: Node[T, Status]) {
          val completed = node.elementsCompleted
          val arrstart = old.lresult.arrayStart + completed
          val leftElemsRemaining = node.left.child.elementsRemaining
          //val oldElemsRemaining = old.elementsRemaining
          //val rightElemsRemaining = node.right.child.elementsRemaining
          //assert(oldElemsRemaining == leftElemsRemaining + rightElemsRemaining,
          //  "elems remaining: " + oldElemsRemaining + " != " + leftElemsRemaining + " + " + rightElemsRemaining +
          //  "\nold " + old.nodeString +
          //  "\nleft " + node.left.toString(0) +
          //  "\nright " + node.right.toString(0) + 
          //  "\ntree: " + wstree.child.repr.root.nodeString
          //)
          val leftarrstart = arrstart
          val rightarrstart = arrstart + leftElemsRemaining
          assert(leftarrstart <= array.length, leftarrstart)

          node.left.child.lresult = new Status(leftarrstart, leftarrstart)
          node.right.child.lresult = new Status(rightarrstart, rightarrstart)
        }
        def zero = null
        def combine(a: Status, b: Status) = null
        def apply(node: N[Status], chunkSize: Int): Status = node.iter.subtree match {
          case null =>
            // internal node is empty - nothing to do
            null
          case ch: Chunk[T] =>
            val pos = node.lresult.arrayProgress
            try {
              if (chunkSize > 0) System.arraycopy(ch.array, 0, array, pos, chunkSize)
            } catch {
              case e: Exception =>
                println(chunkSize + " starting from " + pos)
                println(node.nodeString)
            }
            node.lresult.arrayProgress = pos + chunkSize
            null
        }
      }
      invokeParallelOperation(kernel)

      new ParArray(array, config)
    }

  }



  final class TreeCombiner[@specialized T: ClassTag](init: Boolean) extends ChunkCombiner[T, Tree](init) {
    def this() = this(true)
    
    def newChunkCombiner(shouldInit: Boolean) = new TreeCombiner(shouldInit)
    
    def result = computeTree
  }

}





















