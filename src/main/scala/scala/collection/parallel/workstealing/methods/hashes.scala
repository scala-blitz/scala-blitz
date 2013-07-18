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

  def reduce[K: c.WeakTypeTag, V: c.WeakTypeTag, T >: (K, V): c.WeakTypeTag](c: Context)(op: c.Expr[(T, T) => T])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[T] = {
    import c.universe._

    mapReduce[K, V, T](c)(reify { u: ((K, V)) => u })(op)(ctx)
  }

  def mapReduce[K: c.WeakTypeTag, V: c.WeakTypeTag, R: c.WeakTypeTag](c: Context)(mapper: c.Expr[((K, V)) => R])(reducer: c.Expr[(R, R) => R])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[R] = {
    import c.universe._

    val (lv, op) = c.nonFunctionToLocal[(R, R) => R](reducer)
    val (mv, mop) = c.nonFunctionToLocal[((K, V)) => R](mapper)
    val calleeExpression = c.Expr[Hashes.HashMapOps[K, V]](c.applyPrefix)
    val result = reify {
      import collection.parallel
      import parallel.workstealing._
      import parallel.workstealing.ResultCell

      lv.splice
      val callee = calleeExpression.splice
      val stealer = callee.stealer

      val kernel = new scala.collection.parallel.workstealing.Hashes.HashMapKernel[K, V, ResultCell[R]] {
        override def beforeWorkOn(tree: WorkstealingTreeScheduler.Ref[(K, V), ResultCell[R]], node: WorkstealingTreeScheduler.Node[(K, V), ResultCell[R]]) {
          node.WRITE_INTERMEDIATE(new ResultCell[R])
        }
        def zero = new ResultCell[R]
        def combine(a: ResultCell[R], b: ResultCell[R]) = {
          if (a eq b) a
          else if (a.isEmpty) b
          else if (b.isEmpty) a
          else {
            val r = new ResultCell[R]
            r.result = op.splice(a.result, b.result)
            r
          }
        }
        def apply(node: WorkstealingTreeScheduler.Node[(K, V), ResultCell[R]], from: Int, until: Int) = {
          val rc = node.READ_INTERMEDIATE
          if (from < until) {
            val stealer = node.stealer.asInstanceOf[scala.collection.parallel.workstealing.Hashes.HashMapStealer[K, V]]
            val table = stealer.table

            import collection.mutable
            import mutable.{ DefaultEntry, HashEntry }
            var i = from
            var entries: HashEntry[K, DefaultEntry[K, V]] = null;
            while (i < until && entries == null) {
              entries = table(i)
              i += 1
            }
            if (entries != null) {

              val de = entries.asInstanceOf[DefaultEntry[K, V]]
              val kv = (de.key, de.value)
              var sum = mop.splice.apply(kv)
              entries = entries.next
              while (entries != null) {
                val de = entries.asInstanceOf[DefaultEntry[K, V]]
                val kv = (de.key, de.value)
                sum = op.splice(sum, mop.splice.apply(kv))
                entries = entries.next
              }

              while (i < until) {
                entries = table(i)
                while (entries != null) {
                  val de = entries.asInstanceOf[DefaultEntry[K, V]]
                  val kv = (de.key, de.value)
                  sum = op.splice(sum, mop.splice.apply(kv))
                  entries = entries.next
                }
                i += 1
              }

              if (rc.isEmpty) rc.result = sum
              else rc.result = op.splice(rc.result, sum)
            }
          }
          rc
        }

      }
      val result = ctx.splice.invokeParallelOperation(stealer, kernel)
      result
    }

    val operation = reify {
      val res = result.splice
      if (res.isEmpty) throw new java.lang.UnsupportedOperationException("empty.reduce")
      else res.result
    }

    c.inlineAndReset(operation)
  }

  def min[K: c.WeakTypeTag, V: c.WeakTypeTag, T >: (K, V): c.WeakTypeTag](c: Context)(ord: c.Expr[Ordering[T]], ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[T] = {
    import c.universe._

    val (ordv, ordg) = c.nonFunctionToLocal[Ordering[T]](ord)
    val op = reify { (x: T, y: T) => if (ordg.splice.compare(x, y) < 0) x else y }
    reify {
      ordv.splice
      reduce[K, V, T](c)(op)(ctx).splice
    }
  }

  def max[K: c.WeakTypeTag, V: c.WeakTypeTag, T >: (K, V): c.WeakTypeTag](c: Context)(ord: c.Expr[Ordering[T]], ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[T] = {
    import c.universe._

    val (ordv, ordg) = c.nonFunctionToLocal[Ordering[T]](ord)
    val op = reify { (x: T, y: T) => if (ordg.splice.compare(x, y) > 0) x else y }
    reify {
      ordv.splice
      reduce[K, V, T](c)(op)(ctx).splice
    }
  }

  def product[K: c.WeakTypeTag, V: c.WeakTypeTag, T >: (K, V): c.WeakTypeTag](c: Context)(num: c.Expr[Numeric[T]], ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[T] = {
    import c.universe._

    val (numv, numg) = c.nonFunctionToLocal[Numeric[T]](num)
    val op = reify { (x: T, y: T) => numg.splice.times(x, y) }
    val one = reify { numg.splice.one }
    reify {
      numv.splice
      aggregate[K, V, T](c)(one)(op)(op)(ctx).splice
    }
  }

  def sum[K: c.WeakTypeTag, V: c.WeakTypeTag, T >: (K, V): c.WeakTypeTag](c: Context)(num: c.Expr[Numeric[T]], ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[T] = {
    import c.universe._

    val (numv, numg) = c.nonFunctionToLocal[Numeric[T]](num)
    val op = reify { (x: T, y: T) => numg.splice.plus(x, y) }
    val zero = reify { numg.splice.zero }
    reify {
      numv.splice
      aggregate[K, V, T](c)(zero)(op)(op)(ctx).splice
    }
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

  def foreach[K: c.WeakTypeTag, V: c.WeakTypeTag, U >: (K, V): c.WeakTypeTag](c: Context)(action: c.Expr[U => Unit])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[Unit] = {
    import c.universe._

    val (actv, actg) = c.nonFunctionToLocal[U => Unit](action)
    val combop = reify { (x: Unit, y: Unit) => x }
    val seqop = reify { (cnt: Unit, x: U) => actg.splice.apply(x) }
    val z = reify { () }

    reify {
      actv.splice
      aggregate[K, V, Unit](c)(z)(combop)(seqop)(ctx).splice
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

  def filter[K: c.WeakTypeTag, V: c.WeakTypeTag, That: c.WeakTypeTag](c: Context)(pred: c.Expr[((K, V)) => Boolean])(cmf: c.Expr[CanMergeFrom[Par[HashMap[K, V]], ((K, V)), That]], ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[That] = {
    import c.universe._

    val (pv, p) = c.nonFunctionToLocal[((K, V)) => Boolean](pred)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[Hashes.HashMapOps[K, V]](c.applyPrefix), "callee")
    val (cmfv, canmerge) = c.nonFunctionToLocal[CanMergeFrom[Par[HashMap[K, V]], ((K, V)), That]](cmf, "cmf")
    val mergerExpr = reify { canmerge.splice.apply(callee.splice.hashmap) }

    val tkernel = transformerKernel[K, V, (K, V), That](c)(callee, mergerExpr, reify { (merger: Merger[(K, V), That], elem: (K, V)) => if (p.splice(elem)) merger += elem })

    val operation = reify {
      import scala.reflect.ClassTag
      import scala.collection.parallel._
      import scala.collection.parallel.workstealing.Hashes
      import scala.collection.parallel.workstealing.WorkstealingTreeScheduler
      import scala.collection.parallel.workstealing.WorkstealingTreeScheduler.{ Ref, Node }
      pv.splice
      cv.splice
      cmfv.splice
      val stealer = callee.splice.stealer
      val kernel = tkernel.splice
      val cmb = ctx.splice.invokeParallelOperation(stealer, kernel)
      cmb.result
    }

    c.inlineAndReset(operation)
  }

  def map[K: c.WeakTypeTag, V: c.WeakTypeTag, T: c.WeakTypeTag , That: c.WeakTypeTag](c: Context)(mp: c.Expr[((K, V)) => T])(cmf: c.Expr[CanMergeFrom[Par[HashMap[K, V]], T, That]], ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[That] = {
    import c.universe._

    val (mpv, mpg) = c.nonFunctionToLocal[((K, V)) => T](mp)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[Hashes.HashMapOps[K, V]](c.applyPrefix), "callee")
    val (cmfv, canmerge) = c.nonFunctionToLocal[CanMergeFrom[Par[HashMap[K, V]], T, That]](cmf, "cmf")
    val mergerExpr = reify { canmerge.splice.apply(callee.splice.hashmap) }

    val tkernel = transformerKernel[K, V, T, That](c)(callee, mergerExpr, reify { (merger: Merger[T, That], elem: (K, V)) => merger += mpg.splice.apply(elem) })

    val operation = reify {
      import scala.reflect.ClassTag
      import scala.collection.parallel._
      import scala.collection.parallel.workstealing.Hashes
      import scala.collection.parallel.workstealing.WorkstealingTreeScheduler
      import scala.collection.parallel.workstealing.WorkstealingTreeScheduler.{ Ref, Node }
      mpv.splice
      cv.splice
      cmfv.splice
      val stealer = callee.splice.stealer
      val kernel = tkernel.splice
      val cmb = ctx.splice.invokeParallelOperation(stealer, kernel)
      cmb.result
    }

    c.inlineAndReset(operation)
  }

  def flatMap[K: c.WeakTypeTag, V: c.WeakTypeTag, T: c.WeakTypeTag, That: c.WeakTypeTag](c: Context)(mp: c.Expr[((K, V)) => TraversableOnce[T]])(cmf: c.Expr[CanMergeFrom[Par[HashMap[K, V]], T, That]], ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[That] =   {  
import c.universe._

    val (mpv, mpg) = c.nonFunctionToLocal[((K, V)) => TraversableOnce[T]](mp)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[Hashes.HashMapOps[K, V]](c.applyPrefix), "callee")
    val (cmfv, canmerge) = c.nonFunctionToLocal[CanMergeFrom[Par[HashMap[K, V]], T, That]](cmf, "cmf")
    val mergerExpr = reify { canmerge.splice.apply(callee.splice.hashmap) }

    val tkernel = transformerKernel[K, V, T, That](c)(callee, mergerExpr, reify { (merger: Merger[T, That], elem: (K, V)) => mpg.splice.apply(elem).foreach{merger += _} })

    val operation = reify {
      import scala.reflect.ClassTag
      import scala.collection.parallel._
      import scala.collection.parallel.workstealing.Hashes
      import scala.collection.parallel.workstealing.WorkstealingTreeScheduler
      import scala.collection.parallel.workstealing.WorkstealingTreeScheduler.{ Ref, Node }
      mpv.splice
      cv.splice
      cmfv.splice
      val stealer = callee.splice.stealer
      val kernel = tkernel.splice
      val cmb = ctx.splice.invokeParallelOperation(stealer, kernel)
      cmb.result
    }

    c.inlineAndReset(operation)
  }


  def find[K: c.WeakTypeTag, V: c.WeakTypeTag, T >: (K, V): c.WeakTypeTag](c: Context)(p: c.Expr[T => Boolean])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[Option[(K, V)]] = {
    import c.universe._

    val (lv, pred) = c.nonFunctionToLocal[((K, V)) => Boolean](p)

    val calleeExpression = c.Expr[Hashes.HashMapOps[K, V]](c.applyPrefix)
    reify {
      import scala.collection.parallel.workstealing.WorkstealingTreeScheduler
      import scala.collection.parallel.workstealing.WorkstealingTreeScheduler.{ Ref, Node }
      val kernel = new Hashes.HashMapKernel[K, V, Option[(K, V)]] {
        def zero = None
        def combine(a: Option[(K, V)], b: Option[(K, V)]) = if (a.isDefined) a else b
        def apply(node: Node[(K, V), Option[(K, V)]], from: Int, until: Int) = {
          val stealer = node.stealer.asInstanceOf[scala.collection.parallel.workstealing.Hashes.HashMapStealer[K, V]]
          val table = stealer.table
          var i = from
          var result: (K, V) = null
          while (i < until && result == null) {
            var entries = table(i)
            while (entries != null && result == null) {
              import collection.mutable
              import mutable.DefaultEntry
              val de = entries.asInstanceOf[DefaultEntry[K, V]]
              val kv = (de.key, de.value)
              if (pred.splice(kv)) result = kv
              entries = entries.next
            }
            i += 1
          }
          if (result == null) None else Some(result)
        }
      }
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      ctx.splice.invokeParallelOperation(stealer, kernel)
    }
  }

  def forall[K: c.WeakTypeTag, V: c.WeakTypeTag, T >: (K, V): c.WeakTypeTag](c: Context)(p: c.Expr[T => Boolean])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[Boolean] = {
    import c.universe._

    val np = reify {
      (x: (K, V)) => !p.splice(x)
    }
    val found = find[K, V, (K, V)](c)(np)(ctx)
    reify {
      found.splice.isEmpty
    }
  }

  def exists[K: c.WeakTypeTag, V: c.WeakTypeTag, T >: (K, V): c.WeakTypeTag](c: Context)(p: c.Expr[T => Boolean])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[Boolean] = {
    import c.universe._

    val found = find[K, V, T](c)(p)(ctx)
    reify {
      found.splice.nonEmpty
    }
  }

}
