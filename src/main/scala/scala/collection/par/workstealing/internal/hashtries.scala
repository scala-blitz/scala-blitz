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
import scala.collection.immutable.HashSet
import scala.collection.immutable.HashMap
import Optimizer.c2opt
import scala.reflect.macros.blackbox.{Context => BlackboxContext}


object HashTrieSetMacros {

  def aggregate[T: c.WeakTypeTag, S: c.WeakTypeTag](c: BlackboxContext)(z: c.Expr[S])(combop: c.Expr[(S, S) => S])(seqop: c.Expr[(S, T) => S])(ctx: c.Expr[Scheduler]): c.Expr[S] = {
    import c.universe._

    val (seqlv, seqoper) = c.nonFunctionToLocal[(S, T) => S](seqop)
    val (comblv, comboper) = c.nonFunctionToLocal[(S, S) => S](combop)
    val (zv, zg) = c.nonFunctionToLocal[S](z)
    val calleeExpression = c.Expr[HashTries.HashSetOps[T]](c.applyPrefix)
    val result = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel = new scala.collection.par.workstealing.HashTries.HashSetKernel[T, S] {
        seqlv.splice
        comblv.splice
        zv.splice
        def zero = zg.splice
        def combine(a: S, b: S) = comboper.splice.apply(a, b)
        def apply(node: Node[T, S], ci: HashTries.HashSetIndexedStealer[T], elems: Int): S = {
          if (elems < 1) zero
          else {
            var sum = zero
            var got = 0
            while (got < elems) {
              val el = ci.next
              sum = seqoper.splice(sum, el)
              got += 1
            }
            sum
          }
        }
      }
      ctx.splice.invokeParallelOperation(stealer, kernel)
    }

    c.inlineAndReset(result)
  }

  def find[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(pred: c.Expr[U => Boolean])(ctx: c.Expr[Scheduler]): c.Expr[Option[T]] = {
    import c.universe._

    val (predv, predoper) = c.nonFunctionToLocal[T => Boolean](pred)
    val calleeExpression = c.Expr[HashTries.HashSetOps[T]](c.applyPrefix)
    val result = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import internal._

      predv.splice
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel = new scala.collection.par.workstealing.HashTries.HashSetKernel[T, Option[T]] {
        def zero = None
        def combine(a: Option[T], b: Option[T]) = if (a.isDefined) a else b
        def apply(node: Node[T, Option[T]], ci: HashTries.HashSetIndexedStealer[T], elems: Int): Option[T] = {
          var res: Option[T] = zero
          var got = 0
          while (res.isEmpty && got < elems) {
            val el = ci.next
            got += 1
            if (predoper.splice(el)) { res = Some(el); setTerminationCause(ResultFound) }
          }
          res
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

  def product[U: c.WeakTypeTag, T >: U: c.WeakTypeTag](c: BlackboxContext)(num: c.Expr[Numeric[T]], ctx: c.Expr[Scheduler]): c.Expr[T] = {
    import c.universe._

    val (numv, numg) = c.nonFunctionToLocal[Numeric[T]](num)

    val op = reify { (x: T, y: T) => numg.splice.times(x, y) }
    val one = reify { numg.splice.one }
    reify {
      numv.splice
      aggregate[T, T](c)(one)(op)(op)(ctx).splice
    }
  }

  def sum[U: c.WeakTypeTag, T >: U: c.WeakTypeTag](c: BlackboxContext)(num: c.Expr[Numeric[T]], ctx: c.Expr[Scheduler]): c.Expr[T] = {
    import c.universe._

    val (numv, numg) = c.nonFunctionToLocal[Numeric[T]](num)
    val op = reify { (x: T, y: T) => numg.splice.plus(x, y) }
    val zero = reify { numg.splice.zero }
    reify {
      numv.splice
      aggregate[T, T](c)(zero)(op)(op)(ctx).splice
    }
  }

  def fold[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(z: c.Expr[U])(op: c.Expr[(U, U) => U])(ctx: c.Expr[Scheduler]): c.Expr[U] = {
    aggregate[T, U](c)(z)(op)(op)(ctx)
  }

  def count[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(p: c.Expr[U => Boolean])(ctx: c.Expr[Scheduler]): c.Expr[Int] = {
    import c.universe._

    val (pv, pred) = c.nonFunctionToLocal[U => Boolean](p)
    val combop = reify { (x: Int, y: Int) => x + y }
    val seqop = reify { (cnt: Int, x: U) => if (pred.splice(x)) cnt + 1 else cnt }
    val z = reify { 0 }

    reify {
      pv.splice
      aggregate[T, Int](c)(z)(combop)(seqop)(ctx).splice
    }
  }

  def foreach[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(action: c.Expr[U => Unit])(ctx: c.Expr[Scheduler]): c.Expr[Unit] = {
    import c.universe._

    val (actv, actg) = c.nonFunctionToLocal[U => Unit](action)
    val combop = reify { (x: Unit, y: Unit) => x }
    val seqop = reify { (cnt: Unit, x: U) => actg.splice.apply(x) }
    val z = reify { () }

    reify {
      actv.splice
      aggregate[T, Unit](c)(z)(combop)(seqop)(ctx).splice
    }
  }

  def reduce[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(operator: c.Expr[(U, U) => U])(ctx: c.Expr[Scheduler]): c.Expr[U] = mapReduce[T, U, U](c)(c.universe.reify { x: U => x })(operator)(ctx)

  def min[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(ord: c.Expr[Ordering[U]], ctx: c.Expr[Scheduler]): c.Expr[T] = {
    import c.universe._

    val (ordv, ordg) = c.nonFunctionToLocal[Ordering[U]](ord)
    val op = reify { (x: T, y: T) => if (ordg.splice.compare(x, y) < 0) x else y }
    reify {
      ordv.splice
      reduce[T, T](c)(op)(ctx).splice
    }
  }

  def max[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(ord: c.Expr[Ordering[U]], ctx: c.Expr[Scheduler]): c.Expr[T] = {
    import c.universe._

    val (ordv, ordg) = c.nonFunctionToLocal[Ordering[U]](ord)
    val op = reify { (x: T, y: T) => if (ordg.splice.compare(x, y) > 0) x else y }
    reify {
      ordv.splice
      reduce[T, T](c)(op)(ctx).splice
    }
  }

  def mapReduce[T: c.WeakTypeTag, S >: T: c.WeakTypeTag, M: c.WeakTypeTag](c: BlackboxContext)(mp: c.Expr[S => M])(combop: c.Expr[(M, M) => M])(ctx: c.Expr[Scheduler]): c.Expr[M] = {
    import c.universe._

    val (mpv, mpg) = c.nonFunctionToLocal[T => M](mp)
    val (comblv, comboper) = c.nonFunctionToLocal[(M, M) => M](combop)

    val calleeExpression = c.Expr[HashTries.HashSetOps[T]](c.applyPrefix)
    val result = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import par.workstealing.ResultCell

      val callee = calleeExpression.splice
      val stealer = callee.stealer
      mpv.splice
      comblv.splice
      val kernel = new scala.collection.par.workstealing.HashTries.HashSetKernel[T, ResultCell[M]] {
        def zero = new ResultCell[M]()
        override def beforeWorkOn(tree: Scheduler.Ref[T, ResultCell[M]], node: Scheduler.Node[T, ResultCell[M]]) {
          node.WRITE_INTERMEDIATE(new ResultCell[M])
        }
        def combine(a: ResultCell[M], b: ResultCell[M]) = {
          if (a eq b) a else if (a.isEmpty) b else if (b.isEmpty) a else {
            val r = new ResultCell[M]
            r.result = comboper.splice.apply(a.result, b.result)
            r
          }
        }
        def apply(node: Node[T, ResultCell[M]], ci: HashTries.HashSetIndexedStealer[T], elems: Int): ResultCell[M] = {
          val cur = node.READ_INTERMEDIATE
          if (elems < 1) cur
          else {
            var res = mpg.splice(ci.next)
            var got = 1
            while (got < elems) {
              res = comboper.splice(res, mpg.splice(ci.next))
              got += 1
            }
            if (cur.isEmpty) cur.result = res
            else cur.result = comboper.splice(cur.result, res)
            cur
          }
        }
      }
      val result = ctx.splice.invokeParallelOperation(stealer, kernel)
      if (result.isEmpty) throw new java.lang.UnsupportedOperationException("empty.reduce")
      else result.result
    }

    c.inlineAndReset(result)
  }

  def transformerKernel[T: c.WeakTypeTag, S: c.WeakTypeTag, That: c.WeakTypeTag](c: BlackboxContext)(callee: c.Expr[HashTries.HashSetOps[T]], mergerExpr: c.Expr[Merger[S, That]], applyer: c.Expr[(Merger[S, That], T) => Any]): c.Expr[HashTries.HashSetKernel[T, Merger[S, That]]] = {
    import c.universe._

    reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par.Scheduler
      import scala.collection.par.Scheduler.{ Ref, Node }
      new HashTries.HashSetKernel[T, Merger[S, That]] {
        override def beforeWorkOn(tree: Ref[T, Merger[S, That]], node: Node[T, Merger[S, That]]) {
          node.WRITE_INTERMEDIATE(mergerExpr.splice)
        }
        def zero = null
        def combine(a: Merger[S, That], b: Merger[S, That]) =
          if (a eq null) b
          else if (b eq null) a
          else if (a eq b) a
          else a merge b
        def apply(node: Node[T, Merger[S, That]], ci: HashTries.HashSetIndexedStealer[T], elems: Int): Merger[S, That] = {
          val merger = node.READ_INTERMEDIATE
          var got = 0
          while (got < elems) {
            applyer.splice(merger, ci.next)
            got += 1
          }
          merger
        }
      }
    }
  }

  def map[T: c.WeakTypeTag, S: c.WeakTypeTag, That: c.WeakTypeTag](c: BlackboxContext)(func: c.Expr[T => S])(cmf: c.Expr[CanMergeFrom[Par[HashSet[T]], S, That]], ctx: c.Expr[Scheduler]): c.Expr[That] = {
    import c.universe._

    val (fv, f) = c.nonFunctionToLocal[T => S](func)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[HashTries.HashSetOps[T]](c.applyPrefix), "callee")
    val mergerExpr = reify {
      cmf.splice(callee.splice.hashset)
    }
    val tkernel = transformerKernel[T, S, That](c)(callee, mergerExpr, reify { (merger: Merger[S, That], elem: T) => merger += f.splice(elem) })

    val operation = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par.workstealing.HashTries
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

  def flatMap[T: c.WeakTypeTag, S: c.WeakTypeTag, That: c.WeakTypeTag](c: BlackboxContext)(func: c.Expr[T => TraversableOnce[S]])(cmf: c.Expr[CanMergeFrom[Par[HashSet[T]], S, That]], ctx: c.Expr[Scheduler]): c.Expr[That] = {
    import c.universe._

    val (fv, f) = c.nonFunctionToLocal[T => TraversableOnce[S]](func)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[HashTries.HashSetOps[T]](c.applyPrefix), "callee")
    val mergerExpr = reify {
      cmf.splice(callee.splice.hashset)
    }
    val tkernel = transformerKernel[T, S, That](c)(callee, mergerExpr, reify { (merger: Merger[S, That], elem: T) => f.splice(elem).foreach(el => merger += el) })

    val operation = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par.workstealing.HashTries
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

  def filter[T: c.WeakTypeTag, That: c.WeakTypeTag](c: BlackboxContext)(pred: c.Expr[T => Boolean])(cmf: c.Expr[CanMergeFrom[Par[HashSet[T]], T, That]], ctx: c.Expr[Scheduler]): c.Expr[That] = {
    import c.universe._

    val (fv, f) = c.nonFunctionToLocal[T => Boolean](pred)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[HashTries.HashSetOps[T]](c.applyPrefix), "callee")
    val mergerExpr = reify {
      cmf.splice(callee.splice.hashset)
    }
    val tkernel = transformerKernel[T, T, That](c)(callee, mergerExpr, reify { (merger: Merger[T, That], elem: T) => if (f.splice(elem)) merger += elem })

    val operation = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par.workstealing.HashTries
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

}

object HashTrieMapMacros {

  def aggregate[K: c.WeakTypeTag, V: c.WeakTypeTag, S: c.WeakTypeTag](c: BlackboxContext)(z: c.Expr[S])(combop: c.Expr[(S, S) => S])(seqop: c.Expr[(S, (K, V)) => S])(ctx: c.Expr[Scheduler]): c.Expr[S] = {
    import c.universe._

    val (seqlv, seqoper) = c.nonFunctionToLocal[(S, (K, V)) => S](seqop)
    val (comblv, comboper) = c.nonFunctionToLocal[(S, S) => S](combop)
    val (zv, zg) = c.nonFunctionToLocal[S](z)
    val calleeExpression = c.Expr[HashTries.HashMapOps[K, V]](c.applyPrefix)
    val result = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel = new scala.collection.par.workstealing.HashTries.HashMapKernel[K, V, S] {
        seqlv.splice
        comblv.splice
        def zero = z.splice
        def combine(a: S, b: S) = comboper.splice.apply(a, b)
        def apply(node: Node[(K, V), S], ci: HashTries.HashMapIndexedStealer[K, V], elems: Int): S = {
          var got = 0
          var res = zero
          while (got < elems) { res = seqoper.splice(res, ci.next); got += 1 }
          res
        }
      }
      ctx.splice.invokeParallelOperation(stealer, kernel)
    }

    c.inlineAndReset(result)
  }

  def find[K: c.WeakTypeTag, V: c.WeakTypeTag, U >: (K, V): c.WeakTypeTag](c: BlackboxContext)(pred: c.Expr[U => Boolean])(ctx: c.Expr[Scheduler]): c.Expr[Option[(K, V)]] = {
    import c.universe._

    val (predv, predoper) = c.nonFunctionToLocal[((K, V)) => Boolean](pred)
    val calleeExpression = c.Expr[HashTries.HashMapOps[K, V]](c.applyPrefix)
    val result = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import internal._
      predv.splice
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel = new scala.collection.par.workstealing.HashTries.HashMapKernel[K, V, Option[(K, V)]] {
        def zero = None
        def combine(a: Option[(K, V)], b: Option[(K, V)]) = if (a.isDefined) a else b
        def apply(node: Node[(K, V), Option[(K, V)]], ci: HashTries.HashMapIndexedStealer[K, V], elems: Int): Option[(K, V)] = {
          var result: Option[(K, V)] = None
          var got = 0
          while (got < elems && result.isEmpty) {
            val el = ci.next
            if (predoper.splice(el)) { result = Some(el); setTerminationCause(ResultFound) }
            got += 1
          }
          result
        }
      }
      ctx.splice.invokeParallelOperation(stealer, kernel)
    }

    c.inlineAndReset(result)
  }

  def forall[K: c.WeakTypeTag, V: c.WeakTypeTag, U >: (K, V): c.WeakTypeTag](c: BlackboxContext)(p: c.Expr[U => Boolean])(ctx: c.Expr[Scheduler]): c.Expr[Boolean] = {
    import c.universe._

    val np = reify {
      (x: U) => !p.splice(x)
    }
    val found = find[K, V, U](c)(np)(ctx)
    reify {
      found.splice.isEmpty
    }
  }

  def exists[K: c.WeakTypeTag, V: c.WeakTypeTag, U >: (K, V): c.WeakTypeTag](c: BlackboxContext)(p: c.Expr[U => Boolean])(ctx: c.Expr[Scheduler]): c.Expr[Boolean] = {
    import c.universe._

    val found = find[K, V, U](c)(p)(ctx)
    reify {
      found.splice.nonEmpty
    }
  }

  def mapReduce[K: c.WeakTypeTag, V: c.WeakTypeTag, M: c.WeakTypeTag](c: BlackboxContext)(mp: c.Expr[((K, V)) => M])(combop: c.Expr[(M, M) => M])(ctx: c.Expr[Scheduler]): c.Expr[M] = {
    import c.universe._

    val (mpv, mpg) = c.nonFunctionToLocal[((K, V)) => M](mp)
    val (comblv, comboper) = c.nonFunctionToLocal[(M, M) => M](combop)

    val calleeExpression = c.Expr[HashTries.HashMapOps[K, V]](c.applyPrefix)
    val result = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import par.workstealing.ResultCell

      val callee = calleeExpression.splice
      val stealer = callee.stealer
      mpv.splice
      comblv.splice
      val kernel = new scala.collection.par.workstealing.HashTries.HashMapKernel[K, V, ResultCell[M]] {
        def zero = new ResultCell[M]()
        override def beforeWorkOn(tree: Scheduler.Ref[(K, V), ResultCell[M]], node: Scheduler.Node[(K, V), ResultCell[M]]) {
          node.WRITE_INTERMEDIATE(new ResultCell[M])
        }
        def combine(a: ResultCell[M], b: ResultCell[M]) = {
          if (a eq b) a else if (a.isEmpty) b else if (b.isEmpty) a else {
            val r = new ResultCell[M]
            r.result = comboper.splice.apply(a.result, b.result)
            r
          }
        }
        def apply(node: Node[(K, V), ResultCell[M]], ci: HashTries.HashMapIndexedStealer[K, V], elems: Int): ResultCell[M] = {
          val cur = node.READ_INTERMEDIATE
          if (elems < 1) cur
          else {
            var got = 1
            var res = mpg.splice(ci.next)
            while (got < elems) { res = comboper.splice(res, mpg.splice(ci.next)); got += 1 }
            if (cur.isEmpty) cur.result = res
            else cur.result = comboper.splice(cur.result, res)
            cur
          }
        }
      }
      val result = ctx.splice.invokeParallelOperation(stealer, kernel)
      if (result.isEmpty) throw new java.lang.UnsupportedOperationException("empty.reduce")
      else result.result
    }

    c.inlineAndReset(result)
  }

  def transformerKernel[K: c.WeakTypeTag, V: c.WeakTypeTag, S: c.WeakTypeTag, That: c.WeakTypeTag](c: BlackboxContext)(callee: c.Expr[HashTries.HashMapOps[K, V]], mergerExpr: c.Expr[Merger[S, That]], applyer: c.Expr[(Merger[S, That], (K, V)) => Any]): c.Expr[HashTries.HashMapKernel[K, V, Merger[S, That]]] = {
    import c.universe._

    reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par.Scheduler
      import scala.collection.par.Scheduler.{ Ref, Node }
      new HashTries.HashMapKernel[K, V, Merger[S, That]] {
        override def beforeWorkOn(tree: Ref[(K, V), Merger[S, That]], node: Node[(K, V), Merger[S, That]]) {
          node.WRITE_INTERMEDIATE(mergerExpr.splice)
        }
        def zero = null
        def combine(a: Merger[S, That], b: Merger[S, That]) =
          if (a eq null) b
          else if (b eq null) a
          else if (a eq b) a
          else a merge b
        def apply(node: Node[(K, V), Merger[S, That]], ci: HashTries.HashMapIndexedStealer[K, V], elems: Int): Merger[S, That] = {
          val merger = node.READ_INTERMEDIATE
          var got = 0
          while (got < elems) { applyer.splice(merger, ci.next); got += 1 }
          merger
        }
      }
    }
  }

  def map[K: c.WeakTypeTag, V: c.WeakTypeTag, S: c.WeakTypeTag, That: c.WeakTypeTag](c: BlackboxContext)(func: c.Expr[((K, V)) => S])(cmf: c.Expr[CanMergeFrom[Par[HashMap[K, V]], S, That]], ctx: c.Expr[Scheduler]): c.Expr[That] = {
    import c.universe._

    val (fv, f) = c.nonFunctionToLocal[((K, V)) => S](func)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[HashTries.HashMapOps[K, V]](c.applyPrefix), "callee")
    val mergerExpr = reify {
      cmf.splice(callee.splice.hashmap)
    }
    val tkernel = transformerKernel[K, V, S, That](c)(callee, mergerExpr, reify { (merger: Merger[S, That], kv: (K, V)) =>
      val s: S = f.splice(kv: (K, V))
      merger += s
    })

    val operation = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par.workstealing.HashTries
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

  def product[K: c.WeakTypeTag, V: c.WeakTypeTag, T >: (K, V): c.WeakTypeTag](c: BlackboxContext)(num: c.Expr[Numeric[T]], ctx: c.Expr[Scheduler]): c.Expr[T] = {
    import c.universe._

    val (numv, numg) = c.nonFunctionToLocal[Numeric[T]](num)

    val op = reify { (x: T, y: T) => numg.splice.times(x, y) }
    val one = reify { numg.splice.one }
    reify {
      numv.splice
      aggregate[K, V, T](c)(one)(op)(op)(ctx).splice
    }
  }

  def sum[K: c.WeakTypeTag, V: c.WeakTypeTag, T >: (K, V): c.WeakTypeTag](c: BlackboxContext)(num: c.Expr[Numeric[T]], ctx: c.Expr[Scheduler]): c.Expr[T] = {
    import c.universe._

    val (numv, numg) = c.nonFunctionToLocal[Numeric[T]](num)
    val op = reify { (x: T, y: T) => numg.splice.plus(x, y) }
    val zero = reify { numg.splice.zero }
    reify {
      numv.splice
      aggregate[K, V, T](c)(zero)(op)(op)(ctx).splice
    }
  }

  def fold[K: c.WeakTypeTag, V: c.WeakTypeTag, U >: (K, V): c.WeakTypeTag](c: BlackboxContext)(z: c.Expr[U])(op: c.Expr[(U, U) => U])(ctx: c.Expr[Scheduler]): c.Expr[U] = {
    aggregate[K, V, U](c)(z)(op)(op)(ctx)
  }

  def count[K: c.WeakTypeTag, V: c.WeakTypeTag, U >: (K, V): c.WeakTypeTag](c: BlackboxContext)(p: c.Expr[U => Boolean])(ctx: c.Expr[Scheduler]): c.Expr[Int] = {
    import c.universe._

    val (pv, pred) = c.nonFunctionToLocal[U => Boolean](p)
    val combop = reify { (x: Int, y: Int) => x + y }
    val seqop = reify { (cnt: Int, x: (K, V)) => if (pred.splice(x)) cnt + 1 else cnt }
    val z = reify { 0 }

    reify {
      pv.splice
      aggregate[K, V, Int](c)(z)(combop)(seqop)(ctx).splice
    }
  }

  def foreach[K: c.WeakTypeTag, V: c.WeakTypeTag, U >: (K, V): c.WeakTypeTag](c: BlackboxContext)(action: c.Expr[U => Unit])(ctx: c.Expr[Scheduler]): c.Expr[Unit] = {
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

  def reduce[K: c.WeakTypeTag, V: c.WeakTypeTag, U >: (K, V): c.WeakTypeTag](c: BlackboxContext)(operator: c.Expr[(U, U) => U])(ctx: c.Expr[Scheduler]): c.Expr[U] = mapReduce[K, V, U](c)(c.universe.reify { x: U => x })(operator)(ctx)

  def min[K: c.WeakTypeTag, V: c.WeakTypeTag, U >: (K, V): c.WeakTypeTag](c: BlackboxContext)(ord: c.Expr[Ordering[U]], ctx: c.Expr[Scheduler]): c.Expr[(K, V)] = {
    import c.universe._

    val (ordv, ordg) = c.nonFunctionToLocal[Ordering[U]](ord)
    val op = reify { (x: (K, V), y: (K, V)) => if (ordg.splice.compare(x, y) < 0) x else y }
    reify {
      ordv.splice
      reduce[K, V, (K, V)](c)(op)(ctx).splice
    }
  }

  def max[K: c.WeakTypeTag, V: c.WeakTypeTag, U >: (K, V): c.WeakTypeTag](c: BlackboxContext)(ord: c.Expr[Ordering[U]], ctx: c.Expr[Scheduler]): c.Expr[(K, V)] = {
    import c.universe._

    val (ordv, ordg) = c.nonFunctionToLocal[Ordering[U]](ord)
    val op = reify { (x: (K, V), y: (K, V)) => if (ordg.splice.compare(x, y) > 0) x else y }
    reify {
      ordv.splice
      reduce[K, V, (K, V)](c)(op)(ctx).splice
    }
  }

  def flatMap[K: c.WeakTypeTag, V: c.WeakTypeTag, S: c.WeakTypeTag, That: c.WeakTypeTag](c: BlackboxContext)(func: c.Expr[((K, V)) => TraversableOnce[S]])(cmf: c.Expr[CanMergeFrom[Par[HashMap[K, V]], S, That]], ctx: c.Expr[Scheduler]): c.Expr[That] = {
    import c.universe._

    val (fv, f) = c.nonFunctionToLocal[((K, V)) => TraversableOnce[S]](func)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[HashTries.HashMapOps[K, V]](c.applyPrefix), "callee")
    val mergerExpr = reify {
      cmf.splice(callee.splice.hashmap)
    }
    val tkernel = transformerKernel[K, V, S, That](c)(callee, mergerExpr, reify { (merger: Merger[S, That], elem: (K, V)) => f.splice(elem).foreach(el => merger += el) })

    val operation = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par.workstealing.HashTries
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

  def filter[K: c.WeakTypeTag, V: c.WeakTypeTag, That: c.WeakTypeTag](c: BlackboxContext)(pred: c.Expr[((K, V)) => Boolean])(cmf: c.Expr[CanMergeFrom[Par[HashMap[K, V]], (K, V), That]], ctx: c.Expr[Scheduler]): c.Expr[That] = {
    import c.universe._

    val (fv, f) = c.nonFunctionToLocal[((K, V)) => Boolean](pred)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[HashTries.HashMapOps[K, V]](c.applyPrefix), "callee")
    val mergerExpr = reify {
      cmf.splice(callee.splice.hashmap)
    }
    val tkernel = transformerKernel[K, V, (K, V), That](c)(callee, mergerExpr, reify { (merger: Merger[(K, V), That], elem: (K, V)) => if (f.splice(elem)) merger += elem })

    val operation = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par.workstealing.HashTries
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

}
