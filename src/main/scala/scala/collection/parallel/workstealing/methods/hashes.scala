package scala.collection.parallel.workstealing.methods



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.reflect.ClassTag
import scala.collection.parallel.workstealing._
import scala.collection.parallel.workstealing.WorkstealingTreeScheduler.Node
import scala.collection.parallel.generic._
import scala.collection.parallel.Par
import scala.collection.parallel.Merger
import scala.collection.mutable.HashMap
import Optimizer.c2opt



object HashMapMacros {

  def aggregate[K: c.WeakTypeTag, V: c.WeakTypeTag, S: c.WeakTypeTag](c: Context)(z: c.Expr[S])(combop: c.Expr[(S, S) => S])(seqop: c.Expr[(S, (K, V)) => S])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[S] = {
    import c.universe._

    val (seqlv, seqoper) = c.nonFunctionToLocal[(S, (K, V)) => S](seqop)
    val (comblv, comboper) = c.nonFunctionToLocal[(S, S) => S](combop)
    val (zv, zg) = c.nonFunctionToLocal[S](z)
    val calleeExpression = c.Expr[Hashes.HashMapOps[K, V]](c.applyPrefix)
    val result = reify {
      import scala._
      import collection.parallel
      import parallel._
      import workstealing._
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel = new scala.collection.parallel.workstealing.Hashes.HashMapKernel[K, V, S] {
        def zero = z.splice
        def combine(a: S, b: S) = comboper.splice.apply(a, b)
        def apply(node: WorkstealingTreeScheduler.Node[(K, V), S], from: Int, until: Int) = {
          val stealer = node.stealer.asInstanceOf[scala.collection.parallel.workstealing.Hashes.HashMapStealer[K, V]]
          val table = stealer.table
          var i = from
          var sum = zero
          while (i < until) {
            var entries = table(i)
            while (entries != null) {
              import collection.mutable
              import mutable.DefaultEntry
              val de = entries.asInstanceOf[DefaultEntry[K, V]]
              val kv = (de.key, de.value)
              sum = seqoper.splice(sum, kv)
              entries = entries.next
            }
            i += 1
          }
          sum
        }
      }
      ctx.splice.invokeParallelOperation(stealer, kernel)
    }

    c.inlineAndReset(result)
  }

  def fold[K: c.WeakTypeTag, V: c.WeakTypeTag, U >: (K, V): c.WeakTypeTag](c: Context)(z: c.Expr[U])(op: c.Expr[(U, U) => U])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
    aggregate[K, V, U](c)(z)(op)(op)(ctx)
  }

  def count[K: c.WeakTypeTag, V: c.WeakTypeTag, U >: (K, V): c.WeakTypeTag](c: Context)(p: c.Expr[U => Boolean])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[Int] = {
    import c.universe._

    val (pv, pred) = c.nonFunctionToLocal[U => Boolean](p)
    val combop = reify { (x: Int, y: Int) => x + y }
    val seqop = reify { (cnt: Int, x: U) => if (pred.splice(x)) cnt + 1 else cnt }
    val z = reify { 0 }

    reify {
      pv.splice
      aggregate[K, V, Int](c)(z)(combop)(seqop)(ctx).splice
    }
  }

  def transformerKernel[K: c.WeakTypeTag, V: c.WeakTypeTag, S: c.WeakTypeTag, That: c.WeakTypeTag](c: Context)(callee: c.Expr[Hashes.HashMapOps[K, V]], mergerExpr: c.Expr[Merger[S, That]], applyer: c.Expr[(Merger[S, That], (K, V)) => Any]): c.Expr[Hashes.HashMapKernel[K, V, Merger[S, That]]] = {
    import c.universe._

    reify {
      import scala.collection.parallel.workstealing.WorkstealingTreeScheduler
      import scala.collection.parallel.workstealing.WorkstealingTreeScheduler.{ Ref, Node }
      new Hashes.HashMapKernel[K, V, Merger[S, That]] {
        override def beforeWorkOn(tree: Ref[(K, V), Merger[S, That]], node: Node[(K, V), Merger[S, That]]) {
          node.WRITE_INTERMEDIATE(mergerExpr.splice)
        }
        def zero = null
        def combine(a: Merger[S, That], b: Merger[S, That]) =
          if (a eq null) b
          else if (b eq null) a
          else if (a eq b) a
          else a merge b
        def apply(node: Node[(K, V), Merger[S, That]], from: Int, until: Int) = {
          val stealer = node.stealer.asInstanceOf[scala.collection.parallel.workstealing.Hashes.HashMapStealer[K, V]]
          val table = stealer.table
          val merger = node.READ_INTERMEDIATE
          var i = from
          while (i < until) {
            var entries = table(i)
            while (entries != null) {
              import collection.mutable
              import mutable.DefaultEntry
              val de = entries.asInstanceOf[DefaultEntry[K, V]]
              val kv = (de.key, de.value)
              applyer.splice(merger, kv)
              entries = entries.next
            }
            i += 1
          }
          merger
        }
      }
    }
  }

  def filter[K: c.WeakTypeTag, V: c.WeakTypeTag](c: Context)(pred: c.Expr[((K, V)) => Boolean])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[Par[HashMap[K, V]]] = {
    import c.universe._

    val (pv, p) = c.nonFunctionToLocal[((K, V)) => Boolean](pred)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[Hashes.HashMapOps[K, V]](c.applyPrefix), "callee")
    val mergerExpr = c.Expr[Hashes.HashMapMerger[K, V]] {
      Apply(Select(Ident(newTermName("Hashes")), newTermName("newHashMapMerger")), List(Select(callee.tree, newTermName("hashmap"))))
    }
    val tkernel = transformerKernel[K, V, (K, V), Par[HashMap[K, V]]](c)(callee, mergerExpr, reify { (merger: Merger[(K, V), Par[HashMap[K, V]]], elem: (K, V)) => if (p.splice(elem)) merger += elem })

    val operation = reify {
      import scala.collection.parallel._
      import scala.collection.parallel.workstealing.Hashes
      import scala.collection.parallel.workstealing.WorkstealingTreeScheduler
      import scala.collection.parallel.workstealing.WorkstealingTreeScheduler.{ Ref, Node }
      pv.splice
      cv.splice
      val stealer = callee.splice.stealer
      val kernel = tkernel.splice
      val cmb = ctx.splice.invokeParallelOperation(stealer, kernel)
      cmb.result
    }

    c.inlineAndReset(operation)
  }

}