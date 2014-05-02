package scala.collection.optimizer.internal

import scala.reflect.macros.blackbox.Context
import scala.collection.optimizer.Lists
import scala.language.experimental.macros
import scala.reflect.macros._
import scala.reflect.ClassTag
import scala.collection.par.workstealing._
import scala.collection.par.Scheduler
import scala.collection.par.Scheduler.Node
import scala.collection.par.generic._
import scala.collection.par.Par
import scala.collection.par.Merger

import scala.collection.par.workstealing.internal.Optimizer.c2opt


object ListMacros {

  def mkAggregate[T: c.WeakTypeTag, S: c.WeakTypeTag](c: Context)(z: c.Expr[S])(combop: c.Expr[(S, S) => S])(seqop: c.Expr[(S, T) => S])(prefix: List[c.Expr[Any]]): c.Expr[S] = {

    import c.universe._

    val (calleeExpressionv, calleeExpressiong) = c.nonFunctionToLocal[Lists.Ops[T]](c.Expr[Lists.Ops[T]](c.applyPrefix))
    val t =
      q"""
      $calleeExpressionv
      $prefix
      var zero = $z
      val seqop = $seqop
      val comboop = $combop
      for(el <- $calleeExpressiong.list.seq) zero = seqop(zero, el)
      zero
      """
    c.Expr[S](c.untypecheck(t))
  }

  def aggregate[T: c.WeakTypeTag, S: c.WeakTypeTag](c: Context)(z: c.Expr[S])(combop: c.Expr[(S, S) => S])(seqop: c.Expr[(S, T) => S]): c.Expr[S] =
  mkAggregate(c)(z)(combop)(seqop)(Nil)

  def fold[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(z: c.Expr[U])(op: c.Expr[(U, U) => U]): c.Expr[U] = {
    aggregate(c)(z)(op)(op)
  }

  def sum[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(num: c.Expr[Numeric[U]], ctx: c.Expr[Scheduler]): c.Expr[U] = {
    import c.universe._
    val (numv, numg) = c.nonFunctionToLocal[Numeric[U]](num)
    val op = c.Expr[(U, U)=>U](q"(x: U, y: U) => $numg.plus(x, y)")
    mkAggregate(c)(c.Expr[U](q"$numg.zero"))(op)(op)(List(numv))
  }


    def foreach[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(action: c.Expr[U=>Unit])(ctx: c.Expr[Scheduler]): c.Expr[Unit] = {
      import c.universe._

      val (actionv, actiong) = c.nonFunctionToLocal[U => Unit](action)
      val init = q"a: U => $actiong.apply(a)"
      val fakeComboop = c.Expr[(Unit, Unit) => Unit](q"(x:Unit, a:Unit)=> x")
      val seqoper = c.Expr[(Unit, U) => Unit](q"(x:Unit, a:U)=> $actiong.apply(a)")
      val zero = c.Expr[Unit](q"()")
      mkAggregate(c)(zero)(fakeComboop)(seqoper)(List(actionv))
    }

  /*
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
    */
}
