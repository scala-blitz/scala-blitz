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
import Optimizer.c2opt



object ArraysMacros {

  /* macro implementations */

  def aggregate[T: c.WeakTypeTag, S: c.WeakTypeTag](c: BlackboxContext)(z: c.Expr[S])(combop: c.Expr[(S, S) => S])(seqop: c.Expr[(S, T) => S])(ctx: c.Expr[Scheduler]): c.Expr[S] = {
    import c.universe._

    val (seqlv, seqoper) = c.nonFunctionToLocal[(S, T) => S](seqop)
    val (comblv, comboper) = c.nonFunctionToLocal[(S, S) => S](combop)
    val (zv, zg) = c.nonFunctionToLocal[S](z)
    val init = c.universe.reify { a: T => seqoper.splice.apply(zg.splice, a) }
    invokeAggregateKernel[T, S](c)(seqlv, comblv, zv)(zg)(comboper)(aggregateN[T, S](c)(init, seqoper))(ctx)
  }

  def fold[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(z: c.Expr[U])(op: c.Expr[(U, U) => U])(ctx: c.Expr[Scheduler]): c.Expr[U] = {
    val (lv, oper: c.Expr[(U, U) => U]) = c.nonFunctionToLocal[(U, U) => U](op)
    val (zv, zg: c.Expr[U]) = c.nonFunctionToLocal[U](z)
    val init = c.universe.reify { a: U => oper.splice.apply(zg.splice, a) }
    invokeAggregateKernel[T, U](c)(lv, zv)(zg)(oper)(aggregateN[T, U](c)(init, oper))(ctx)
  }

  def sum[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(num: c.Expr[Numeric[U]], ctx: c.Expr[Scheduler]): c.Expr[U] = {
    import c.universe._

    val (numv, numg) = c.nonFunctionToLocal[Numeric[U]](num)
    val (zerov, zerog) = c.nonFunctionToLocal[U](reify {
      numg.splice.zero
    })
    val calleeExpression = c.Expr[Ranges.Ops](c.applyPrefix)
    val op = reify {
      (x: U, y: U) => numg.splice.plus(x, y)
    }
    val (lv, oper: c.Expr[(U, U) => U]) = c.nonFunctionToLocal[(U, U) => U](op)
    val init = c.universe.reify { a: U => a }
    invokeAggregateKernel[T, U](c)(lv, numv, zerov)(zerog)(oper)(aggregateN[T, U](c)(init, oper))(ctx)
  }


  def foreach[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(action: c.Expr[U=>Unit])(ctx: c.Expr[Scheduler]): c.Expr[Unit] = {
    import c.universe._

    val (actionv, actiong) = c.nonFunctionToLocal[U => Unit](action)
    val init = c.universe.reify { a: U => actiong.splice.apply(a) }
    val seqoper = reify{(x:Unit, a:U)=> actiong.splice.apply(a)}
    val zero = reify{()}
    val comboop = reify{(x:Unit, y:Unit) => ()}
    invokeAggregateKernel[T, Unit](c)(actionv)(zero)(comboop)(aggregateN[T,Unit](c)(init, seqoper))(ctx)
  }


  def product[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(num: c.Expr[Numeric[U]], ctx: c.Expr[Scheduler]): c.Expr[U] = {
    import c.universe._

    val (numv, numg) = c.nonFunctionToLocal[Numeric[U]](num)
    val (zerov, zerog) = c.nonFunctionToLocal[U](reify {
      numg.splice.one
    })
    val op = reify {
      (x: U, y: U) => numg.splice.times(x, y)
    }
    val (lv, oper: c.Expr[(U, U) => U]) = c.nonFunctionToLocal[(U, U) => U](op)
    val calleeExpression = c.Expr[Ranges.Ops](c.applyPrefix)
    val init = c.universe.reify { a: U => a }
    invokeAggregateKernel[T, U](c)(lv, numv, zerov)(zerog)(oper)(aggregateN[T, U](c)(init, oper))(ctx)
  }

  def count[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(p: c.Expr[U => Boolean])(ctx: c.Expr[Scheduler]): c.Expr[Int] = {
    import c.universe._

    val (predicv, predic) = c.nonFunctionToLocal[T => Boolean](p)
    val zero = reify { 0 }
    val combop = reify {
      (x: Int, y: Int) => x + y
    }
    val seqop = reify {
      (x: Int, y: T) =>
        if (predic.splice(y)) x + 1 else x
    }
    val (seqlv, seqoper) = c.nonFunctionToLocal[(Int, T) => Int](seqop)
    val (comblv, comboper) = c.nonFunctionToLocal[(Int, Int) => Int](combop)
    val init = c.universe.reify { a: T => if (predic.splice(a)) 1 else 0; }
    invokeAggregateKernel[T, Int](c)(predicv, seqlv, comblv)(zero)(comboper)(aggregateN[T, Int](c)(init, seqoper))(ctx)
  }

  def aggregateN[T: c.WeakTypeTag, R: c.WeakTypeTag](c: BlackboxContext)(init: c.Expr[T => R], oper: c.Expr[(R, T) => R]) = c.universe.reify { (from: Int, to: Int, kernel: Arrays.ArrayKernel[T,R], arr: Array[T]) =>
    {
      if (from > to) kernel.zero
      else {
        var i = from + 1
        var sum: R = init.splice.apply(arr(from))

        while (i < to) {
          sum = oper.splice(sum, arr(i))
          i += 1
        }
        sum
      }
    }
  }

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

  def findIndex[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(calleeExpr: c.Expr[Arrays.Ops[T]])(p: c.Expr[U => Boolean])(ctx: c.Expr[Scheduler]): c.Expr[Option[Int]] = {
    import c.universe._

    val (lv, pred) = c.nonFunctionToLocal[U => Boolean](p)

    val result = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._
      import internal._
      import scala.collection.par
      lv.splice
      val callee = calleeExpr.splice
      val stealer = callee.stealer
      val kernel = new scala.collection.par.workstealing.Arrays.ArrayKernel[T, Option[Int]] {
        def zero = None
        def combine(a: Option[Int], b: Option[Int]) = if (a.isDefined) a else b
        def apply(node: Node[T, Option[Int]], from: Int, to: Int) = {
          var i = from
          
          val arr = callee.array.seq
          while (i < to && !pred.splice(arr(i))) {
            i += 1
          }
          if (i < to) {
            setTerminationCause(ResultFound)
            Some(i)
          }
          else None
        }
      }
      val result = ctx.splice.invokeParallelOperation(stealer, kernel)
      result
    }
    c.inlineAndReset(result)
  }

  def find[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(p: c.Expr[U => Boolean])(ctx: c.Expr[Scheduler]): c.Expr[Option[T]] = {
    import c.universe._

    val (calleeExpressionv, calleeExpressiong) = c.nonFunctionToLocal(c.Expr[Arrays.Ops[T]](c.applyPrefix))
    val found = findIndex[T, T](c)(calleeExpressiong)(p)(ctx)
    reify {
      calleeExpressionv.splice
      val mayBeIndex = found.splice
      val result =
        if (mayBeIndex.isDefined) Some(calleeExpressiong.splice.array.seq(mayBeIndex.get))
        else None
      result
    }
  }

  def forall[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(p: c.Expr[U => Boolean])(ctx: c.Expr[Scheduler]): c.Expr[Boolean] = {
    import c.universe._

    val (calleeExpressionv, calleeExpressiong) = c.nonFunctionToLocal(c.Expr[Arrays.Ops[T]](c.applyPrefix))
    val np = reify {
      (x: T) => !p.splice(x)
    }
    val found = findIndex[T, T](c)(calleeExpressiong)(np)(ctx)
    reify {
      calleeExpressionv.splice
      found.splice.isEmpty
    }
  }

  def exists[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(p: c.Expr[U => Boolean])(ctx: c.Expr[Scheduler]): c.Expr[Boolean] = {
    import c.universe._

    val (calleeExpressionv, calleeExpressiong) = c.nonFunctionToLocal(c.Expr[Arrays.Ops[T]](c.applyPrefix))
    val found = findIndex[T, U](c)(calleeExpressiong)(p)(ctx)
    reify {
      calleeExpressionv.splice
      found.splice.nonEmpty
    }
  }
  
  def invokeAggregateKernel[T: c.WeakTypeTag, R: c.WeakTypeTag](c: BlackboxContext)(initializer: c.Expr[Unit]*)(z: c.Expr[R])(combiner: c.Expr[(R, R) => R])(applyerN: c.Expr[(Int, Int, Arrays.ArrayKernel[T,R], Array[T]) => R])(ctx: c.Expr[Scheduler]): c.Expr[R] = {
    import c.universe._

    val calleeExpression = c.Expr[Arrays.Ops[T]](c.applyPrefix)
    val resultWithoutInit = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel =
        new scala.collection.par.workstealing.Arrays.ArrayKernel[T, R] {
          def zero = z.splice
          def combine(a: R, b: R) = combiner.splice.apply(a, b)
          def apply(node: Scheduler.Node[T, R], from: Int, to: Int) = applyerN.splice.apply(from, to, this, callee.array.seq)
        }
      ctx.splice.invokeParallelOperation(stealer, kernel)
    }
    val newChildren = initializer.flatMap { initializer =>
      val origTree = initializer.tree
      if (origTree.isDef) List(origTree) else origTree.children
    }.toList

    val resultTree = resultWithoutInit.tree match {
      case Block((children, expr)) => Block(newChildren ::: children, expr)
      case _ => c.abort(resultWithoutInit.tree.pos, "failed to get kernel as block")
    }

    val result = c.Expr[R](resultTree)

    c.inlineAndReset(result)
  }

  def reduce[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(operator: c.Expr[(U, U) => U])(ctx: c.Expr[Scheduler]): c.Expr[U] = mapReduce[T, U, U](c)(c.universe.reify { x: U => x})(operator)(ctx)

  def mapReduce[T: c.WeakTypeTag, U >: T: c.WeakTypeTag, R: c.WeakTypeTag](c: BlackboxContext)(mapper: c.Expr[U => R])(reducer: c.Expr[(R, R) => R])(ctx: c.Expr[Scheduler]): c.Expr[R] = {
    import c.universe._

    val (lv, op) = c.nonFunctionToLocal[(R, R) => R](reducer)
    val (mv, mop) = c.nonFunctionToLocal[U => R](mapper)
    val calleeExpression = c.Expr[Arrays.Ops[T]](c.applyPrefix)
    val result = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      lv.splice
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel = new scala.collection.par.workstealing.Arrays.ArrayKernel[T, ResultCell[R]] {
        override def beforeWorkOn(tree: Scheduler.Ref[T, ResultCell[R]], node: Scheduler.Node[T, ResultCell[R]]) {
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
        def apply(node: Node[T, ResultCell[R]], from: Int, until: Int) = {
          val array = node.stealer.asInstanceOf[Arrays.ArrayStealer[T]].array
          val rc = node.READ_INTERMEDIATE
          if (from < until) {
            var sum: R = mop.splice.apply(array(from))
            var i = from + 1
            while (i < until) {
              sum = op.splice(sum, mop.splice.apply(array(i)))
              i += 1
            }
            if (rc.isEmpty) rc.result = sum
            else rc.result = op.splice(rc.result, sum)
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

  def copyMapKernel[T: c.WeakTypeTag, S: c.WeakTypeTag](c: BlackboxContext)(f: c.Expr[T => S])(callee: c.Expr[Arrays.Ops[T]], from: c.Expr[Int], until: c.Expr[Int])(getTagForS: c.Expr[ClassTag[S]]): c.Expr[Arrays.CopyMapArrayKernel[T, S]] = {
    import c.universe._

    reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par.Scheduler
      import scala.collection.par.workstealing.ProgressStatus
      val sTag = getTagForS.splice
      val len = until.splice - from.splice
      val sarray = sTag.newArray(len)
      new Arrays.CopyMapArrayKernel[T, S] {
        import scala.collection.par.Scheduler.{ Ref, Node }
        import scala.collection.par.workstealing.Arrays.CopyProgress
        def resultArray = sarray
        def apply(node: Node[T, Unit], from: Int, until: Int) = {
          val srcarr = callee.splice.array.seq
          var srci = from
          var desti = from
          while (srci < until) {
            sarray(desti) = f.splice(srcarr(srci))
            srci += 1
            desti += 1
          }
          ()
        }
      }
    }
  }

  def transformerKernel[T: c.WeakTypeTag, S: c.WeakTypeTag, That: c.WeakTypeTag](c: BlackboxContext)(callee: c.Expr[Arrays.Ops[T]], mergerExpr: c.Expr[Merger[S, That]], applyer: c.Expr[(Merger[S, That], T) => Any]): c.Expr[Arrays.ArrayKernel[T, Merger[S, That]]] = {
    import c.universe._

    reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par.Scheduler
      import scala.collection.par.Scheduler.{ Ref, Node }
      new Arrays.ArrayKernel[T, Merger[S, That]] {
        override def beforeWorkOn(tree: Ref[T, Merger[S, That]], node: Node[T, Merger[S, That]]) {
          node.WRITE_INTERMEDIATE(mergerExpr.splice)
        }
        def zero = null
        def combine(a: Merger[S, That], b: Merger[S, That]) =
          if (a eq null) b
          else if (b eq null) a
          else if (a eq b) a
          else a merge b
        def apply(node: Node[T, Merger[S, That]], from: Int, until: Int) = {
          val merger = node.READ_INTERMEDIATE
          val arr = callee.splice.array.seq
          var i = from
          while (i < until) {
            val elem = arr(i)
            applyer.splice(merger, elem)
            i += 1
          }
          merger
        }
      }
    }
  }

  def accumulate[T: c.WeakTypeTag, S: c.WeakTypeTag](c: BlackboxContext)(merger: c.Expr[Merger[T, S]])(ctx: c.Expr[Scheduler]): c.Expr[S] = {
    import c.universe._

    val (cv, callee) = c.nonFunctionToLocal(c.Expr[Arrays.Ops[T]](c.applyPrefix), "callee")
    val tkernel = transformerKernel[T, T, S](c)(callee, merger, reify {
      (merger: Merger[T, S], elem: T) => merger += elem
    })

    val operation = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._
      import scala.collection.par.workstealing.Arrays
      import scala.collection.par.Scheduler
      import scala.collection.par.Scheduler.{ Ref, Node }
      cv.splice
      val stealer = callee.splice.stealer
      val kernel = tkernel.splice
      val merger = ctx.splice.invokeParallelOperation(stealer, kernel)
      merger.result
    }

    c.inlineAndReset(operation)
  }

  def map[T: c.WeakTypeTag, S: c.WeakTypeTag, That: c.WeakTypeTag](c: BlackboxContext)(func: c.Expr[T => S])(cmf: c.Expr[CanMergeFrom[Par[Array[T]], S, That]], ctx: c.Expr[Scheduler]): c.Expr[That] = {
    import c.universe._

    val (lv, f) = c.nonFunctionToLocal[T => S](func)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[Arrays.Ops[T]](c.applyPrefix), "callee")
    val (cmfv, canmerge) = c.nonFunctionToLocal(cmf, "cmf")
    val mergerExpr = reify { canmerge.splice.apply(callee.splice.array) }
    val (mv, merger) = c.nonFunctionToLocal(mergerExpr, "merger")
    val stagExpr = reify { merger.splice.asInstanceOf[Arrays.ArrayMerger[S]].classTag }
    val lengthExpr = reify { callee.splice.array.seq.length }
    val cmkernel = copyMapKernel(c)(f)(callee, reify { 0 }, lengthExpr)(stagExpr)
    val tkernel = transformerKernel(c)(callee, mergerExpr, reify { (merger: Merger[S, That], elem: T) => merger += f.splice(elem) })

    val operation = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._
      import scala.collection.par.workstealing.Arrays
      import scala.collection.par.Scheduler
      import scala.collection.par.Scheduler.{ Ref, Node }
      import scala.reflect.ClassTag
      lv.splice
      cv.splice
      cmfv.splice
      mv.splice
      val stealer = callee.splice.stealer
      if (Arrays.isArrayMerger(merger.splice)) {
        val kernel = cmkernel.splice
        ctx.splice.invokeParallelOperation(stealer, kernel)
        new Par(kernel.resultArray).asInstanceOf[That]
      } else {
        val kernel = tkernel.splice
        val cmb = ctx.splice.invokeParallelOperation(stealer, kernel)
        cmb.result
      }
    }

    c.inlineAndReset(operation)
  }

  def filter[T: c.WeakTypeTag, That:c.WeakTypeTag](c: BlackboxContext)(pred: c.Expr[T => Boolean])(cmf: c.Expr[CanMergeFrom[Par[Array[T]], T, That]], ctx: c.Expr[Scheduler]): c.Expr[That] = {
    import c.universe._

    val (pv, p) = c.nonFunctionToLocal[T => Boolean](pred)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[Arrays.Ops[T]](c.applyPrefix), "callee")
    val (cmfv, canmerge) = c.nonFunctionToLocal[CanMergeFrom[Par[Array[T]], T, That]](cmf, "cmf")
    val mergerExpr = reify { canmerge.splice.apply(callee.splice.seq) }
    val tkernel = transformerKernel[T, T, That](c)(callee, mergerExpr, reify { (merger: Merger[T, That], elem: T) => if (p.splice(elem)) merger += elem })

    val operation = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._
      import scala.collection.par.workstealing.Arrays
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

  def flatMap[T: c.WeakTypeTag, S: c.WeakTypeTag, That: c.WeakTypeTag](c: BlackboxContext)(func: c.Expr[T => TraversableOnce[S]])(cmf: c.Expr[CanMergeFrom[Par[Array[T]], S, That]], ctx: c.Expr[Scheduler]): c.Expr[That] = {
    import c.universe._

    val (lv, f) = c.nonFunctionToLocal[T => TraversableOnce[S]](func)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[Arrays.Ops[T]](c.applyPrefix), "callee")
    val (cmfv, canmerge) = c.nonFunctionToLocal[CanMergeFrom[Par[Array[T]], S, That]](cmf, "cmf")
    val mergerExpr = reify { canmerge.splice.apply(callee.splice.array) }
    val applyer = reify {
      (merger: Merger[S, That], elem: T) => f.splice(elem).foreach(merger += _)
    }
    val tkernel = transformerKernel(c)(callee, mergerExpr, c.optimise(applyer))

    val operation = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._
      import scala.collection.par.workstealing.Arrays
      import scala.collection.par.Scheduler
      import scala.collection.par.Scheduler.{Ref, Node}
      import scala.reflect.ClassTag
      lv.splice
      cv.splice
      cmfv.splice
      val stealer = callee.splice.stealer
      val kernel = tkernel.splice
      val cmb = ctx.splice.invokeParallelOperation(stealer, kernel)
      cmb.result
    }

    c.inlineAndReset(operation)
  }

}

