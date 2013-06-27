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

}