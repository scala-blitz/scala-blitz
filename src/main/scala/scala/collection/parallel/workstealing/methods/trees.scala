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
import scala.collection.immutable.HashMap
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


  def mapReduce[T: c.WeakTypeTag, S >: T : c.WeakTypeTag,  M: c.WeakTypeTag](c: Context)(mp: c.Expr[S => M])(combop: c.Expr[(M, M) => M])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[M] = {
    import c.universe._

    val (mpv, mpg) = c.nonFunctionToLocal[T => M](mp)
    val (comblv, comboper) = c.nonFunctionToLocal[(M, M) => M](combop)

    val calleeExpression = c.Expr[Trees.HashSetOps[T]](c.applyPrefix)
    val result = reify {
      import scala._
      import collection.parallel
      import parallel._
      import workstealing._
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      mpv.splice
      comblv.splice
      val kernel = new scala.collection.parallel.workstealing.Trees.HashSetKernel[T, ResultCell[M]] {
        def zero = new ResultCell[M]()
        override def beforeWorkOn(tree: WorkstealingTreeScheduler.Ref[T, ResultCell[M]], node: WorkstealingTreeScheduler.Node[T, ResultCell[M]]) {
          node.WRITE_INTERMEDIATE(new ResultCell[M])
        }
        def combine(a: ResultCell[M], b: ResultCell[M]) = {
          if(a.isEmpty) b else if (b.isEmpty) a else {
            val r = new ResultCell[M]
            r.result= comboper.splice.apply(a.result, b.result)
            r
        }
        }
        def apply(node: Node[T, ResultCell[M]], ci: Trees.TrieChunkIterator[T, HashSet[T]]): ResultCell[M] = {
          def combineRC(acc:ResultCell[M], v:T) =  {
            if(acc.isEmpty) acc.result = mpg.splice(v)
            else acc.result = comboper.splice(acc.result, mpg.splice(v))
            acc
          }
        
          def apply(node: HashSet[T], acc: ResultCell[M]): ResultCell[M] = node match {
            case hs1: HashSet.HashSet1[_] =>
              combineRC(acc,Trees.key(hs1))
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
              hsc.foldLeft(acc){(a:ResultCell[M],b:T) => combineRC(a,b)}
          }
          val cur = node.READ_INTERMEDIATE
          if (ci.hasNext) apply(ci.root, cur) else cur
        }
      }
      val result = ctx.splice.invokeParallelOperation(stealer, kernel)
      if(result.isEmpty) throw new java.lang.UnsupportedOperationException("empty.reduce")
      else result.result
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


object HashTrieMapMacros {

  def aggregate[K: c.WeakTypeTag, V: c.WeakTypeTag, S: c.WeakTypeTag](c: Context)(z: c.Expr[S])(combop: c.Expr[(S, S) => S])(seqop: c.Expr[(S, (K, V)) => S])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[S] = {
    import c.universe._

    val (seqlv, seqoper) = c.nonFunctionToLocal[(S, (K, V)) => S](seqop)
    val (comblv, comboper) = c.nonFunctionToLocal[(S, S) => S](combop)
    val (zv, zg) = c.nonFunctionToLocal[S](z)
    val calleeExpression = c.Expr[Trees.HashMapOps[K, V]](c.applyPrefix)
    val result = reify {
      import scala._
      import collection.parallel
      import parallel._
      import workstealing._
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel = new scala.collection.parallel.workstealing.Trees.HashMapKernel[K, V, S] {
        def zero = z.splice
        def combine(a: S, b: S) = comboper.splice.apply(a, b)
        def apply(node: Node[(K, V), S], ci: Trees.TrieChunkIterator[(K, V), HashMap[K, V]]): S = {
          def apply(node: HashMap[K, V], acc: S): S = node match {
            case hm1: HashMap.HashMap1[_, _] =>
              seqoper.splice(acc, Trees.kv(hm1))
            case hmt: HashMap.HashTrieMap[_, _] =>
              var i = 0
              var sum = acc
              val elems = hmt.elems
              while (i < elems.length) {
                sum = apply(elems(i), sum)
                i += 1
              }
              sum
            case hmc: HashMap[K, V] =>
              hmc.foldLeft(acc)(seqoper.splice)
          }

          if (ci.hasNext) apply(ci.root, zero) else zero
        }
      }
      ctx.splice.invokeParallelOperation(stealer, kernel)
    }

    c.inlineAndReset(result)
  }

  def transformerKernel[K: c.WeakTypeTag, V: c.WeakTypeTag, S: c.WeakTypeTag, That: c.WeakTypeTag](c: Context)(callee: c.Expr[Trees.HashMapOps[K, V]], mergerExpr: c.Expr[Merger[S, That]], applyer: c.Expr[(Merger[S, That], (K, V)) => Any]): c.Expr[Trees.HashMapKernel[K, V, Merger[S, That]]] = {
    import c.universe._

    reify {
      import scala.collection.parallel.workstealing.WorkstealingTreeScheduler
      import scala.collection.parallel.workstealing.WorkstealingTreeScheduler.{ Ref, Node }
      new Trees.HashMapKernel[K, V, Merger[S, That]] {
        override def beforeWorkOn(tree: Ref[(K, V), Merger[S, That]], node: Node[(K, V), Merger[S, That]]) {
          node.WRITE_INTERMEDIATE(mergerExpr.splice)
        }
        def zero = null
        def combine(a: Merger[S, That], b: Merger[S, That]) =
          if (a eq null) b
          else if (b eq null) a
          else if (a eq b) a
          else a merge b
        def apply(node: Node[(K, V), Merger[S, That]], ci: Trees.TrieChunkIterator[(K, V), HashMap[K, V]]): Merger[S, That] = {
          def apply(node: HashMap[K, V], cmb: Merger[S, That]): Unit = node match {
            case hm1: HashMap.HashMap1[_, _] =>
              applyer.splice(cmb, Trees.kv(hm1))
            case hmt: HashMap.HashTrieMap[_, _] =>
              var i = 0
              val elems = hmt.elems
              while (i < elems.length) {
                apply(elems(i), cmb)
                i += 1
              }
            case hmc: HashMap[K, V] =>
              for (kv <- hmc) applyer.splice(cmb, kv)
          }

          if (ci.hasNext) apply(ci.root, node.READ_INTERMEDIATE)
          node.READ_INTERMEDIATE
        }
      }
    }
  }

  def map[K: c.WeakTypeTag, V: c.WeakTypeTag, S: c.WeakTypeTag, That: c.WeakTypeTag](c: Context)(func: c.Expr[((K, V)) => S])(cmf: c.Expr[CanMergeFrom[Par[HashMap[K, V]], S, That]], ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[That] = {
    import c.universe._

    val (fv, f) = c.nonFunctionToLocal[((K, V)) => S](func)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[Trees.HashMapOps[K, V]](c.applyPrefix), "callee")
    val mergerExpr = reify {
      cmf.splice(callee.splice.hashmap)
    }
    val tkernel = transformerKernel[K, V, S, That](c)(callee, mergerExpr, reify { (merger: Merger[S, That], kv: (K, V)) =>
      val s: S = f.splice(kv: (K, V))
      merger += s
    })

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
