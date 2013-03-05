package scala.collection.workstealing
package test



import scala.reflect.ClassTag
import scala.collection._



object TreeWorkstealingTest extends App {

  case class Tree(val left: Tree, val right: Tree, val height: Int, val size: Int) {
    def isLeaf = left == null
  }

  val treeIsTree = new TreeWorkstealing.IsTree[Tree] {
    def left(tree: Tree): Tree = tree.left
    def right(tree: Tree): Tree = tree.right
    def size(tree: Tree): Int = tree.size
    def height(tree: Tree): Int = tree.height
    def isLeaf(tree: Tree): Boolean = tree.isLeaf
    def tag: ClassTag[Tree] = implicitly[ClassTag[Tree]]
  }

  val workstealing = new TreeWorkstealing[Unit, Tree] {
    val isTree = treeIsTree
    def config = ???
    def newRoot[R]: Ptr[Unit, R] = ???
    def size: Int = ???
  }

  def node(root: Tree): workstealing.TreeNode[Unit, Unit] = {
    val stack = TreeWorkstealing.initializeStack(root)(treeIsTree)
    new workstealing.TreeNode[Unit, Unit](null, null)(root, stack, 1) {
      def newExpanded(parent: workstealing.Ptr[Unit, Unit]) = ???
    }
  }

  def testAdvance(root: Tree, step: Int, expected: Seq[Int]) {
    val seen = mutable.ArrayBuffer[Int]()
    val wsnd = node(root)
    var loop = true
    while (loop) {
      val chunk = wsnd.advance(step)
      //println(chunk)
      seen += chunk
      if (chunk == -1) loop = false
    }
    assert(seen == expected, seen.mkString(", ") + " vs. expected " + expected.mkString(", "))
  }

  def leaf = Tree(null, null, 0, 1)
  def dual = bind(leaf, leaf)
  def bind(l: Tree, r: Tree) = Tree(l, r, math.max(l.height, r.height) + 1, l.size + r.size + 1)

  testAdvance(
    leaf,
    1,
    Seq(1, -1)
  )

  testAdvance(
    dual,
    1,
    Seq(1, 1, 1, -1)
  )

  testAdvance(
    dual,
    2,
    Seq(1, 1, 1, -1)
  )

  testAdvance(
    bind(leaf, dual),
    1,
    Seq(1, 1, 1, 1, 1, -1)
  )

  testAdvance(
    bind(leaf, dual),
    4,
    Seq(1, 1, 3, -1)
  )

  testAdvance(
    bind(dual, bind(leaf, dual)),
    8,
    Seq(3, 1, 5, -1)
  )

}







