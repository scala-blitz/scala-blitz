package scala.collection.parallel.workstealing.methods



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.reflect.ClassTag
import scala.collection.parallel.workstealing._
import scala.collection.parallel.workstealing.WorkstealingTreeScheduler.Node
import scala.collection.parallel.generic._
import scala.collection.parallel.Par
import scala.collection.parallel.Merger
import scala.collection.immutable.HashSet
import Optimizer.c2opt



object HashTrieSetMacros {

  def aggregate[T: c.WeakTypeTag, S: c.WeakTypeTag](c: Context)(z: c.Expr[S])(combop: c.Expr[(S, S) => S])(seqop: c.Expr[(S, T) => S])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[S] = {
    import c.universe._

    val (seqlv, seqoper) = c.nonFunctionToLocal[(S, T) => S](seqop)
    val (comblv, comboper) = c.nonFunctionToLocal[(S, S) => S](combop)
    val (zv, zg) = c.nonFunctionToLocal[S](z)
    val calleeExpression = c.Expr[Trees.HashSetOps[T]](c.applyPrefix)
    val result = reify {
      import scala._
      import collection.parallel
      import parallel._
      import workstealing._
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel = new scala.collection.parallel.workstealing.Trees.HashSetKernel[T, S] {
        def zero = z.splice
        def combine(a: S, b: S) = comboper.splice.apply(a, b)
        def apply(node: Node[T, S], ci: Trees.TrieChunkIterator[T, HashSet[T]]): S = {
          def apply(node: HashSet[T], acc: S): S = node match {
            case hs1: HashSet.HashSet1[_] =>
              seqoper.splice(acc, Trees.key(hs1))
            case hst: HashSet.HashTrieSet[_] =>
              var i = 0
              var sum = acc
              val elems = hst.elems
              while (i < elems.length) {
                sum = apply(elems(i), sum)
                i += 1
              }
              sum
            case hsc: HashSet[T] =>
              hsc.foldLeft(acc)(seqoper.splice)
          }

          if (ci.hasNext) apply(ci.root, zero) else zero
        }
      }
      ctx.splice.invokeParallelOperation(stealer, kernel)
    }

    c.inlineAndReset(result)
  }

  def transformerKernel[T: c.WeakTypeTag, S: c.WeakTypeTag, That: c.WeakTypeTag](c: Context)(callee: c.Expr[Trees.HashSetOps[T]], mergerExpr: c.Expr[Merger[S, That]], applyer: c.Expr[(Merger[S, That], T) => Any]): c.Expr[Trees.HashSetKernel[T, Merger[S, That]]] = {
    import c.universe._

    reify {
      import scala.collection.parallel.workstealing.WorkstealingTreeScheduler
      import scala.collection.parallel.workstealing.WorkstealingTreeScheduler.{ Ref, Node }
      new Trees.HashSetKernel[T, Merger[S, That]] {
        override def beforeWorkOn(tree: Ref[T, Merger[S, That]], node: Node[T, Merger[S, That]]) {
          node.WRITE_INTERMEDIATE(mergerExpr.splice)
        }
        def zero = null
        def combine(a: Merger[S, That], b: Merger[S, That]) =
          if (a eq null) b
          else if (b eq null) a
          else if (a eq b) a
          else a merge b
        def apply(node: Node[T, Merger[S, That]], ci: Trees.TrieChunkIterator[T, HashSet[T]]): Merger[S, That] = {
          def apply(node: HashSet[T], cmb: Merger[S, That]): Unit = node match {
            case hs1: HashSet.HashSet1[_] =>
              applyer.splice(cmb, Trees.key(hs1))
            case hst: HashSet.HashTrieSet[_] =>
              var i = 0
              val elems = hst.elems
              while (i < elems.length) {
                apply(elems(i), cmb)
                i += 1
              }
            case hsc: HashSet[T] =>
              for (e <- hsc) applyer.splice(cmb, e)
          }

          if (ci.hasNext) apply(ci.root, node.READ_INTERMEDIATE)
          node.READ_INTERMEDIATE
        }
      }
    }
  }

  def map[T: c.WeakTypeTag, S: c.WeakTypeTag, That: c.WeakTypeTag](c: Context)(func: c.Expr[T => S])(cmf: c.Expr[CanMergeFrom[Par[HashSet[T]], S, That]], ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[That] = {
    import c.universe._

    val (fv, f) = c.nonFunctionToLocal[T => S](func)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[Trees.HashSetOps[T]](c.applyPrefix), "callee")
    val mergerExpr = reify {
      cmf.splice(callee.splice.hashset)
    }
    val tkernel = transformerKernel[T, S, That](c)(callee, mergerExpr, reify { (merger: Merger[S, That], elem: T) => merger += f.splice(elem) })

    val operation = reify {
      import scala.collection.parallel._
      import scala.collection.parallel.workstealing.Trees
      import scala.collection.parallel.workstealing.WorkstealingTreeScheduler
      import scala.collection.parallel.workstealing.WorkstealingTreeScheduler.{ Ref, Node }
      fv.splice
      cv.splice
      val stealer = callee.splice.stealer
      val kernel = tkernel.splice
      val cmb = ctx.splice.invokeParallelOperation(stealer, kernel)
      cmb.result
    }

    c.inlineAndReset(operation)
  }
}