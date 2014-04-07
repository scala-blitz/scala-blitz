package scala.collection.par.workstealing.internal



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.reflect.ClassTag
import scala.collection.par.workstealing._
import scala.collection.par.Scheduler
import scala.collection.par.Scheduler.Node
import scala.collection.par.generic._
import scala.collection.par.Par
import scala.collection.par.Merger
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap
import Optimizer.c2opt
import scala.reflect.macros.blackbox.{Context => BlackboxContext}


object HashMapMacros {

  def aggregate[K: c.WeakTypeTag, V: c.WeakTypeTag, S: c.WeakTypeTag](c: BlackboxContext)(z: c.Expr[S])(combop: c.Expr[(S, S) => 
S])(seqop: c.Expr[(S, (K, V)) => S])(ctx: c.Expr[Scheduler]): c.Expr[S] = {
    import c.universe._

    val (seqlv, seqoper) = c.nonFunctionToLocal[(S, (K, V)) => S](seqop)
    val (comblv, comboper) = c.nonFunctionToLocal[(S, S) => S](combop)
    val (zv, zg) = c.nonFunctionToLocal[S](z)
    val calleeExpression = c.Expr[HashTables.HashMapOps[K, V]](c.applyPrefix)
    val result = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel = new scala.collection.par.workstealing.HashTables.HashMapKernel[K, V, S] {
        seqlv.splice
        comblv.splice
        zv.splice
        def zero = zg.splice
        def combine(a: S, b: S) = comboper.splice.apply(a, b)
        def apply(node: Scheduler.Node[(K, V), S], from: Int, until: Int) = {
          val stealer = node.stealer.asInstanceOf[scala.collection.par.workstealing.HashTables.HashMapStealer[K, V]]
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

  def reduce[K: c.WeakTypeTag, V: c.WeakTypeTag, T >: (K, V): c.WeakTypeTag](c: BlackboxContext)(op: c.Expr[(T, T) => T])(ctx: 
c.Expr[Scheduler]): c.Expr[T] = {
    import c.universe._

    mapReduce[K, V, T](c)(reify { u: ((K, V)) => u })(op)(ctx)
  }

  def mapReduce[K: c.WeakTypeTag, V: c.WeakTypeTag, R: c.WeakTypeTag](c: BlackboxContext)(mapper: c.Expr[((K, V)) => R])(reducer: 
c.Expr[(R, R) => R])(ctx: c.Expr[Scheduler]): c.Expr[R] = {
    import c.universe._

    val (lv, op) = c.nonFunctionToLocal[(R, R) => R](reducer)
    val (mv, mop) = c.nonFunctionToLocal[((K, V)) => R](mapper)
    val calleeExpression = c.Expr[HashTables.HashMapOps[K, V]](c.applyPrefix)
    val result = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._
      import par.workstealing.ResultCell

      lv.splice
      mv.splice
      val callee = calleeExpression.splice
      val stealer = callee.stealer

      val kernel = new scala.collection.par.workstealing.HashTables.HashMapKernel[K, V, ResultCell[R]] {
        override def beforeWorkOn(tree: Scheduler.Ref[(K, V), ResultCell[R]], node: 
Scheduler.Node[(K, V), ResultCell[R]]) {
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
        def apply(node: Scheduler.Node[(K, V), ResultCell[R]], from: Int, until: Int) = {
          val rc = node.READ_INTERMEDIATE
          if (from < until) {
            val stealer = node.stealer.asInstanceOf[scala.collection.par.workstealing.HashTables.HashMapStealer[K, V]]
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

  def min[K: c.WeakTypeTag, V: c.WeakTypeTag, T >: (K, V): c.WeakTypeTag](c: BlackboxContext)(ord: c.Expr[Ordering[T]], ctx: 
c.Expr[Scheduler]): c.Expr[T] = {
    import c.universe._

    val (ordv, ordg) = c.nonFunctionToLocal[Ordering[T]](ord)
    val op = reify { (x: T, y: T) => if (ordg.splice.compare(x, y) < 0) x else y }
    reify {
      ordv.splice
      reduce[K, V, T](c)(op)(ctx).splice
    }
  }

  def max[K: c.WeakTypeTag, V: c.WeakTypeTag, T >: (K, V): c.WeakTypeTag](c: BlackboxContext)(ord: c.Expr[Ordering[T]], ctx: 
c.Expr[Scheduler]): c.Expr[T] = {
    import c.universe._

    val (ordv, ordg) = c.nonFunctionToLocal[Ordering[T]](ord)
    val op = reify { (x: T, y: T) => if (ordg.splice.compare(x, y) > 0) x else y }
    reify {
      ordv.splice
      reduce[K, V, T](c)(op)(ctx).splice
    }
  }

  def product[K: c.WeakTypeTag, V: c.WeakTypeTag, T >: (K, V): c.WeakTypeTag](c: BlackboxContext)(num: c.Expr[Numeric[T]], ctx: 
c.Expr[Scheduler]): c.Expr[T] = {
    import c.universe._

    val (numv, numg) = c.nonFunctionToLocal[Numeric[T]](num)
    val op = reify { (x: T, y: T) => numg.splice.times(x, y) }
    val one = reify { numg.splice.one }
    reify {
      numv.splice
      aggregate[K, V, T](c)(one)(op)(op)(ctx).splice
    }
  }

  def sum[K: c.WeakTypeTag, V: c.WeakTypeTag, T >: (K, V): c.WeakTypeTag](c: BlackboxContext)(num: c.Expr[Numeric[T]], ctx: 
c.Expr[Scheduler]): c.Expr[T] = {
    import c.universe._

    val (numv, numg) = c.nonFunctionToLocal[Numeric[T]](num)
    val op = reify { (x: T, y: T) => numg.splice.plus(x, y) }
    val zero = reify { numg.splice.zero }
    reify {
      numv.splice
      aggregate[K, V, T](c)(zero)(op)(op)(ctx).splice
    }
  }

  def fold[K: c.WeakTypeTag, V: c.WeakTypeTag, U >: (K, V): c.WeakTypeTag](c: BlackboxContext)(z: c.Expr[U])(op: c.Expr[(U, U) => 
U])(ctx: c.Expr[Scheduler]): c.Expr[U] = {
    aggregate[K, V, U](c)(z)(op)(op)(ctx)
  }

  def count[K: c.WeakTypeTag, V: c.WeakTypeTag, U >: (K, V): c.WeakTypeTag](c: BlackboxContext)(p: c.Expr[U => Boolean])(ctx: 
c.Expr[Scheduler]): c.Expr[Int] = {
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

  def foreach[K: c.WeakTypeTag, V: c.WeakTypeTag, U >: (K, V): c.WeakTypeTag](c: BlackboxContext)(action: c.Expr[U => Unit])(ctx: 
c.Expr[Scheduler]): c.Expr[Unit] = {
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

  def transformerKernel[K: c.WeakTypeTag, V: c.WeakTypeTag, S: c.WeakTypeTag, That: c.WeakTypeTag](c: BlackboxContext)(callee: 
c.Expr[HashTables.HashMapOps[K, V]], mergerExpr: c.Expr[Merger[S, That]], applyer: c.Expr[(Merger[S, That], (K, V)) => Any]): 
c.Expr[HashTables.HashMapKernel[K, V, Merger[S, That]]] = {
    import c.universe._

    reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par.Scheduler
      import scala.collection.par.Scheduler.{ Ref, Node }
      new HashTables.HashMapKernel[K, V, Merger[S, That]] {
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
          val stealer = node.stealer.asInstanceOf[scala.collection.par.workstealing.HashTables.HashMapStealer[K, V]]
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

  def filter[K: c.WeakTypeTag, V: c.WeakTypeTag, That: c.WeakTypeTag](c: BlackboxContext)(pred: c.Expr[((K, V)) => Boolean])(cmf: 
c.Expr[CanMergeFrom[Par[HashMap[K, V]], ((K, V)), That]], ctx: c.Expr[Scheduler]): c.Expr[That] = {
    import c.universe._

    val (pv, p) = c.nonFunctionToLocal[((K, V)) => Boolean](pred)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[HashTables.HashMapOps[K, V]](c.applyPrefix), "callee")
    val (cmfv, canmerge) = c.nonFunctionToLocal[CanMergeFrom[Par[HashMap[K, V]], ((K, V)), That]](cmf, "cmf")
    val mergerExpr = reify { canmerge.splice.apply(callee.splice.hashmap) }

    val tkernel = transformerKernel[K, V, (K, V), That](c)(callee, mergerExpr, reify { (merger: Merger[(K, V), That], elem: 
(K, V)) => if (p.splice(elem)) merger += elem })

    val operation = reify {

      import scala._
      import collection.par
      import par._
      import workstealing._


      import scala.reflect.ClassTag
      import scala.collection.par.workstealing.HashTables
      import scala.collection.par.Scheduler
      import scala.collection.par.Scheduler.{ Ref, Node }
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

  def map[K: c.WeakTypeTag, V: c.WeakTypeTag, T: c.WeakTypeTag , That: c.WeakTypeTag](c: BlackboxContext)(mp: c.Expr[((K, V)) => 
T])(cmf: c.Expr[CanMergeFrom[Par[HashMap[K, V]], T, That]], ctx: c.Expr[Scheduler]): c.Expr[That] = {
    import c.universe._

    val (mpv, mpg) = c.nonFunctionToLocal[((K, V)) => T](mp)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[HashTables.HashMapOps[K, V]](c.applyPrefix), "callee")
    val (cmfv, canmerge) = c.nonFunctionToLocal[CanMergeFrom[Par[HashMap[K, V]], T, That]](cmf, "cmf")
    val mergerExpr = reify { canmerge.splice.apply(callee.splice.hashmap) }

    val tkernel = transformerKernel[K, V, T, That](c)(callee, mergerExpr, reify { (merger: Merger[T, That], elem: (K, V)) => 
merger += mpg.splice.apply(elem) })

    val operation = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.reflect.ClassTag
      import scala.collection.par.workstealing.HashTables
      import scala.collection.par.Scheduler
      import scala.collection.par.Scheduler.{ Ref, Node }
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

  def flatMap[K: c.WeakTypeTag, V: c.WeakTypeTag, T: c.WeakTypeTag, That: c.WeakTypeTag](c: BlackboxContext)(mp: c.Expr[((K, V)) => 
TraversableOnce[T]])(cmf: c.Expr[CanMergeFrom[Par[HashMap[K, V]], T, That]], ctx: c.Expr[Scheduler]): 
c.Expr[That] =   {  
import c.universe._

    val (mpv, mpg) = c.nonFunctionToLocal[((K, V)) => TraversableOnce[T]](mp)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[HashTables.HashMapOps[K, V]](c.applyPrefix), "callee")
    val (cmfv, canmerge) = c.nonFunctionToLocal[CanMergeFrom[Par[HashMap[K, V]], T, That]](cmf, "cmf")
    val mergerExpr = reify { canmerge.splice.apply(callee.splice.hashmap) }

    val tkernel = transformerKernel[K, V, T, That](c)(callee, mergerExpr, reify { (merger: Merger[T, That], elem: (K, V)) => 
mpg.splice.apply(elem).foreach{merger += _} })

    val operation = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.reflect.ClassTag
      import scala.collection.par.workstealing.HashTables
      import scala.collection.par.Scheduler
      import scala.collection.par.Scheduler.{ Ref, Node }
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


  def find[K: c.WeakTypeTag, V: c.WeakTypeTag, T >: (K, V): c.WeakTypeTag](c: BlackboxContext)(p: c.Expr[T => Boolean])(ctx: 
c.Expr[Scheduler]): c.Expr[Option[(K, V)]] = {
    import c.universe._

    val (lv, pred) = c.nonFunctionToLocal[((K, V)) => Boolean](p)

    val calleeExpression = c.Expr[HashTables.HashMapOps[K, V]](c.applyPrefix)
    reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par.Scheduler
      import scala.collection.par.Scheduler.{ Ref, Node }
      val kernel = new HashTables.HashMapKernel[K, V, Option[(K, V)]] {
        lv.splice
        def zero = None
        def combine(a: Option[(K, V)], b: Option[(K, V)]) = if (a.isDefined) a else b
        def apply(node: Node[(K, V), Option[(K, V)]], from: Int, until: Int) = {
          val stealer = node.stealer.asInstanceOf[scala.collection.par.workstealing.HashTables.HashMapStealer[K, V]]
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
              if (pred.splice(kv)) {result = kv; setTerminationCause(ResultFound)}
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

  def forall[K: c.WeakTypeTag, V: c.WeakTypeTag, T >: (K, V): c.WeakTypeTag](c: BlackboxContext)(p: c.Expr[T => Boolean])(ctx: 
c.Expr[Scheduler]): c.Expr[Boolean] = {
    import c.universe._

    val np = reify {
      (x: (K, V)) => !p.splice(x)
    }
    val found = find[K, V, (K, V)](c)(np)(ctx)
    reify {
      found.splice.isEmpty
    }
  }

  def exists[K: c.WeakTypeTag, V: c.WeakTypeTag, T >: (K, V): c.WeakTypeTag](c: BlackboxContext)(p: c.Expr[T => Boolean])(ctx: 
c.Expr[Scheduler]): c.Expr[Boolean] = {
    import c.universe._

    val found = find[K, V, T](c)(p)(ctx)
    reify {
      found.splice.nonEmpty
    }
  }

}


object HashSetMacros {

  def aggregate[T: c.WeakTypeTag, S: c.WeakTypeTag](c: BlackboxContext)(z: c.Expr[S])(combop: c.Expr[(S, S) => S])(seqop: c.Expr[(S, 
T) => S])(ctx: c.Expr[Scheduler]): c.Expr[S] = {
    import c.universe._

    val (seqlv, seqoper) = c.nonFunctionToLocal[(S, T) => S](seqop)
    val (comblv, comboper) = c.nonFunctionToLocal[(S, S) => S](combop)
    val (zv, zg) = c.nonFunctionToLocal[S](z)
    val calleeExpression = c.Expr[HashTables.HashSetOps[T]](c.applyPrefix)
    val result = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel = new scala.collection.par.workstealing.HashTables.HashSetKernel[T, S] {
        seqlv.splice
        comblv.splice
        zv.splice
        def zero = zg.splice
        def combine(a: S, b: S) = comboper.splice.apply(a, b)
        def apply(node: Scheduler.Node[T, S], from: Int, until: Int) = {
          val stealer = node.stealer.asInstanceOf[scala.collection.par.workstealing.HashTables.HashSetStealer[T]]
          val table = stealer.table
          var i = from
          var sum = zero
          while (i < until) {
            var current = table(i)
            if (current != null) sum = seqoper.splice(sum, current.asInstanceOf[T])
            i += 1
          }
          sum
        }
      }
      ctx.splice.invokeParallelOperation(stealer, kernel)
    }

    c.inlineAndReset(result)
  }

  def find[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(pred: c.Expr[U => Boolean])(ctx: c.Expr[Scheduler]): c.Expr[Option[T]] = {
    import c.universe._

    val (predv, predg) = c.nonFunctionToLocal[T => Boolean](pred)
    val calleeExpression = c.Expr[HashTables.HashSetOps[T]](c.applyPrefix)
    val result = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel = new scala.collection.par.workstealing.HashTables.HashSetKernel[T, Option[T]] {
        predv.splice
        def zero = None
        def combine(a: Option[T], b: Option[T]) = if (a.isDefined) a else b
        def apply(node: Scheduler.Node[T, Option[T]], from: Int, until: Int) = {
          val stealer = node.stealer.asInstanceOf[scala.collection.par.workstealing.HashTables.HashSetStealer[T]]
          val table = stealer.table
          var i = from
          while (i < until && (table(i) == null|| !predg.splice(table(i).asInstanceOf[T]))) {
            i += 1
          }
          if(i < until) Some(table(i).asInstanceOf[T])
          else None
        }
      }
      ctx.splice.invokeParallelOperation(stealer, kernel)
    }

    c.inlineAndReset(result)
  }


  def forall[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(p: c.Expr[U => Boolean])(ctx: c.Expr[Scheduler]): c.Expr[Boolean] = {
    import c.universe._

    val np = reify {
      (x: T) => !p.splice(x)
    }
    val found = find[T, T](c)(np)(ctx)
    reify {
      found.splice.isEmpty
    }
  }

  def exists[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(p: c.Expr[U => Boolean])(ctx: c.Expr[Scheduler]): c.Expr[Boolean] = {
    import c.universe._

    val found = find[T, U](c)(p)(ctx)
    reify {
      found.splice.nonEmpty
    }
  }


  def transformerKernel[T: c.WeakTypeTag, S: c.WeakTypeTag, That: c.WeakTypeTag](c: BlackboxContext)(callee: 
c.Expr[HashTables.HashSetOps[T]], mergerExpr: c.Expr[Merger[S, That]], applyer: c.Expr[(Merger[S, That], T) => Any]): 
c.Expr[HashTables.HashSetKernel[T, Merger[S, That]]] = {
    import c.universe._

    reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par.Scheduler
      import scala.collection.par.Scheduler.{ Ref, Node }
      new scala.collection.par.workstealing.HashTables.HashSetKernel[T, Merger[S, That]] {
        override def beforeWorkOn(tree: Ref[T, Merger[S, That]], node: Node[T, Merger[S, That]]) {
          node.WRITE_INTERMEDIATE(mergerExpr.splice)
        }
        def zero = null
        def combine(a: Merger[S, That], b: Merger[S, That]) =
          if (a eq null) b
          else if (b eq null) a
          else if (a eq b) a
          else a merge b
        def apply(node: Scheduler.Node[T, Merger[S, That]], from: Int, until: Int) = {
          val stealer = node.stealer.asInstanceOf[scala.collection.par.workstealing.HashTables.HashSetStealer[T]]
          val table = stealer.table
          val cmb = node.READ_INTERMEDIATE
          var i = from
          while (i < until) {
            var current = table(i)
            if (current != null) applyer.splice(cmb, current.asInstanceOf[T])
            i += 1
          }
          node.READ_INTERMEDIATE
        }
      }
    }
  }

  def map[T: c.WeakTypeTag, S: c.WeakTypeTag, That: c.WeakTypeTag](c: BlackboxContext)(func: c.Expr[T => S])(cmf: 
c.Expr[CanMergeFrom[Par[HashSet[T]], S, That]], ctx: c.Expr[Scheduler]): c.Expr[That] = {
    import c.universe._

    val (fv, f) = c.nonFunctionToLocal[T => S](func)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[HashTables.HashSetOps[T]](c.applyPrefix), "callee")
    val mergerExpr = reify {
      cmf.splice(callee.splice.hashset)
    }
    val tkernel = transformerKernel[T, S, That](c)(callee, mergerExpr, reify { (merger: Merger[S, That], elem: T) => merger += 
f.splice(elem) })

    val operation = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par.workstealing.HashTables
      import scala.collection.par.Scheduler
      import scala.collection.par.Scheduler.{ Ref, Node }
      fv.splice
      cv.splice
      val stealer = callee.splice.stealer
      val kernel = tkernel.splice
      val cmb = ctx.splice.invokeParallelOperation(stealer, kernel)
      cmb.result
    }

    c.inlineAndReset(operation)
  }

  def filter[T: c.WeakTypeTag,  That: c.WeakTypeTag](c: BlackboxContext)(pred: c.Expr[T => Boolean])(cmf: 
c.Expr[CanMergeFrom[Par[HashSet[T]], T, That]], ctx: c.Expr[Scheduler]): c.Expr[That] = {
    import c.universe._

    val (fv, f) = c.nonFunctionToLocal[T => Boolean](pred)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[HashTables.HashSetOps[T]](c.applyPrefix), "callee")
    val mergerExpr = reify {
      cmf.splice(callee.splice.hashset)
    }
    val tkernel = transformerKernel[T, T, That](c)(callee, mergerExpr, reify { (merger: Merger[T, That], elem: T) => 
      if(f.splice(elem))merger +=elem })

    val operation = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par.workstealing.HashTables
      import scala.collection.par.Scheduler
      import scala.collection.par.Scheduler.{ Ref, Node }
      fv.splice
      cv.splice
      val stealer = callee.splice.stealer
      val kernel = tkernel.splice
      val cmb = ctx.splice.invokeParallelOperation(stealer, kernel)
      cmb.result
    }

    c.inlineAndReset(operation)
  }


  def flatMap[T: c.WeakTypeTag, S: c.WeakTypeTag, That: c.WeakTypeTag](c: BlackboxContext)(func: c.Expr[T => TraversableOnce[S]])(cmf: 
c.Expr[CanMergeFrom[Par[HashSet[T]], S, That]], ctx: c.Expr[Scheduler]): c.Expr[That] = {
    import c.universe._

    val (fv, f) = c.nonFunctionToLocal[T => TraversableOnce[S]](func)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[HashTables.HashSetOps[T]](c.applyPrefix), "callee")
    val mergerExpr = reify {
      cmf.splice(callee.splice.hashset)
    }
    val tkernel = transformerKernel[T, S, That](c)(callee, mergerExpr, reify { (merger: Merger[S, That], elem: T) =>  
f.splice(elem).foreach(x=> merger +=x) })

    val operation = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._


      import scala.collection.par.workstealing.HashTables
      import scala.collection.par.Scheduler
      import scala.collection.par.Scheduler.{ Ref, Node }
      fv.splice
      cv.splice
      val stealer = callee.splice.stealer
      val kernel = tkernel.splice
      val cmb = ctx.splice.invokeParallelOperation(stealer, kernel)
      cmb.result
    }

    c.inlineAndReset(operation)
  }


  def mapReduce[T: c.WeakTypeTag, R: c.WeakTypeTag](c: BlackboxContext)(mapper: c.Expr[T => R])(reducer: 
c.Expr[(R, R) => R])(ctx: c.Expr[Scheduler]): c.Expr[R] = {
    import c.universe._

    val (lv, op) = c.nonFunctionToLocal[(R, R) => R](reducer)
    val (mv, mop) = c.nonFunctionToLocal[T => R](mapper)
    val calleeExpression = c.Expr[HashTables.HashSetOps[T]](c.applyPrefix)
    val result = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import par.workstealing.ResultCell

      lv.splice
      mv.splice
      val callee = calleeExpression.splice
      val stealer = callee.stealer

      val kernel = new scala.collection.par.workstealing.HashTables.HashSetKernel[T, ResultCell[R]] {
        override def beforeWorkOn(tree: Scheduler.Ref[T, ResultCell[R]], node: 
Scheduler.Node[T, ResultCell[R]]) {
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
         def apply(node: Scheduler.Node[T, ResultCell[R]], from: Int, until: Int) = {
           val stealer = node.stealer.asInstanceOf[scala.collection.par.workstealing.HashTables.HashSetStealer[T]]
           val table = stealer.table
           val cmb = node.READ_INTERMEDIATE
           var i = from
           if(cmb.isEmpty) {
             while(i < until && table(i) == null)  i += 1
             if(i < until) { cmb.result = mop.splice(table(i).asInstanceOf[T]); i = i + 1;}
            }

           while (i < until) {
             var current = table(i)
             if (current != null) cmb.result = op.splice(cmb.result, mop.splice(table(i).asInstanceOf[T]))
             i += 1
           }
           cmb
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

  def reduce[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(op: c.Expr[(U, U) => U])(ctx: 
c.Expr[Scheduler]): c.Expr[U] = {
    import c.universe._

    mapReduce[T, U](c)(reify { u: T => u })(op)(ctx)
  }

  def min[TT: c.WeakTypeTag, T >: TT: c.WeakTypeTag](c: BlackboxContext)(ord: c.Expr[Ordering[T]], ctx: 
c.Expr[Scheduler]): c.Expr[TT] = {
    import c.universe._

    val (ordv, ordg) = c.nonFunctionToLocal[Ordering[T]](ord)
    val op = reify { (x: TT, y: TT) => if (ordg.splice.compare(x, y) < 0) x else y }
    reify {
      ordv.splice
      reduce[TT, TT](c)(op)(ctx).splice
    }
  }

  def max[TT: c.WeakTypeTag, T >: TT: c.WeakTypeTag](c: BlackboxContext)(ord: c.Expr[Ordering[T]], ctx: 
c.Expr[Scheduler]): c.Expr[TT] = {
    import c.universe._

    val (ordv, ordg) = c.nonFunctionToLocal[Ordering[T]](ord)
    val op = reify { (x: TT, y: TT) => if (ordg.splice.compare(x, y) > 0) x else y }
    reify {
      ordv.splice
      reduce[TT, TT](c)(op)(ctx).splice
    }
  }



  def product[TT: c.WeakTypeTag, T >: TT: c.WeakTypeTag](c: BlackboxContext)(num: c.Expr[Numeric[T]], ctx: c.Expr[Scheduler]): c.Expr[T] = {
    import c.universe._

    val (numv, numg) = c.nonFunctionToLocal[Numeric[T]](num)
    val op = reify { (x: T, y: T) => numg.splice.times(x, y) }
    val one = reify { numg.splice.one }
    reify {
      numv.splice
      aggregate[TT, T](c)(one)(op)(op)(ctx).splice
    }
  }

    def sum[TT: c.WeakTypeTag, T >: TT: c.WeakTypeTag](c: BlackboxContext)(num: c.Expr[Numeric[T]], ctx: c.Expr[Scheduler]): c.Expr[T] = {
    import c.universe._

    val (numv, numg) = c.nonFunctionToLocal[Numeric[T]](num)
    val op = reify { (x: T, y: T) => numg.splice.plus(x, y) }
    val one = reify { numg.splice.zero }
    reify {
      numv.splice
      aggregate[TT, T](c)(one)(op)(op)(ctx).splice
    }
  }

  def fold[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(z: c.Expr[U])(op: c.Expr[(U, U) => 
U])(ctx: c.Expr[Scheduler]): c.Expr[U] = {
    aggregate[T, U](c)(z)(op)(op)(ctx)
  }

  def count[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(p: c.Expr[U => Boolean])(ctx: 
c.Expr[Scheduler]): c.Expr[Int] = {
    import c.universe._

    val (pv, pred) = c.nonFunctionToLocal[T => Boolean](p)
    val combop = reify { (x: Int, y: Int) => x + y }
    val seqop = reify { (cnt: Int, x: T) => if (pred.splice(x)) cnt + 1 else cnt }
    val z = reify { 0 }

    reify {
      pv.splice
      aggregate[T, Int](c)(z)(combop)(seqop)(ctx).splice
    }
  }

  def foreach[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(action: c.Expr[U => Unit])(ctx: 
c.Expr[Scheduler]): c.Expr[Unit] = {
    import c.universe._

    val (actv, actg) = c.nonFunctionToLocal[T => Unit](action)
    val combop = reify { (x: Unit, y: Unit) => x }
    val seqop = reify { (cnt: Unit, x: T) => actg.splice.apply(x) }
    val z = reify { () }

    reify {
      actv.splice
      aggregate[T, Unit](c)(z)(combop)(seqop)(ctx).splice
    }
  }


}
