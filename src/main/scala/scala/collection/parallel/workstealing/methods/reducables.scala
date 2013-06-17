package scala.collection.parallel.workstealing.methods

import scala.language.experimental.macros
import scala.reflect.macros._
import scala.reflect.ClassTag
import scala.collection.parallel.generic._
import scala.collection.parallel.Par
import scala.collection.parallel.workstealing._
import scala.collection.parallel.Configuration
import scala.collection.parallel.Merger
import scala.reflect.ClassTag
import scala.collection.parallel.workstealing.methods.Optimizer._
import scala.collection.parallel.workstealing.WorkstealingTreeScheduler.Node

object ReducablesMacros {

  def aggregate[T: c.WeakTypeTag, S: c.WeakTypeTag, Repr:c.WeakTypeTag](c: Context)(z: c.Expr[S])(combop: c.Expr[(S, S) => S])(seqop: c.Expr[(S, T) => S])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[S] = {
    import c.universe._

    val (seqlv, seqoper) = c.nonFunctionToLocal[(S, T) => S](seqop)
    val (comblv, comboper) = c.nonFunctionToLocal[(S, S) => S](combop)
    val (zv, zg) = c.nonFunctionToLocal[S](z)
    val init = c.universe.reify { a: T => seqoper.splice.apply(zg.splice, a) }
    invokeAggregateKernel[T, S, Repr](c)(seqlv, comblv, zv)(zg)(comboper)(aggregateN[T, S](c)(init, seqoper))(ctx)
  }

  def fold[T: c.WeakTypeTag, U >: T: c.WeakTypeTag, Repr:c.WeakTypeTag](c: Context)(z: c.Expr[U])(op: c.Expr[(U, U) => U])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
    val (lv, oper: c.Expr[(U, U) => U]) = c.nonFunctionToLocal[(U, U) => U](op)
    val (zv, zg: c.Expr[U]) = c.nonFunctionToLocal[U](z)
    val init = c.universe.reify { a: U => oper.splice.apply(zg.splice, a) }
    invokeAggregateKernel[T, U, Repr](c)(lv, zv)(zg)(oper)(aggregateN[T, U](c)(init, oper))(ctx)
  }

  def sum[T: c.WeakTypeTag, U >: T: c.WeakTypeTag, Repr:c.WeakTypeTag](c: Context)(num: c.Expr[Numeric[U]], ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
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
    invokeAggregateKernel[T, U,Repr](c)(lv, numv, zerov)(zerog)(oper)(aggregateN[T, U](c)(init, oper))(ctx)
  }

  def foreach[T: c.WeakTypeTag, U >: T: c.WeakTypeTag, Repr:c.WeakTypeTag](c: Context)(action: c.Expr[U => Unit])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[Unit] = {
    import c.universe._

    val (actionv, actiong) = c.nonFunctionToLocal[U => Unit](action)
    val init = c.universe.reify { a: U => actiong.splice.apply(a) }
    val seqoper = reify { (x: Unit, a: U) => actiong.splice.apply(a) }
    val zero = reify { () }
    val comboop = reify { (x: Unit, y: Unit) => () }
    invokeAggregateKernel[T, Unit, Repr](c)(actionv)(zero)(comboop)(aggregateN[T, Unit](c)(init, seqoper))(ctx)
  }

  def product[T: c.WeakTypeTag, U >: T: c.WeakTypeTag, Repr:c.WeakTypeTag](c: Context)(num: c.Expr[Numeric[U]], ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
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
    invokeAggregateKernel[T, U, Repr](c)(lv, numv, zerov)(zerog)(oper)(aggregateN[T, U](c)(init, oper))(ctx)
  }

  def count[T: c.WeakTypeTag, U >: T: c.WeakTypeTag, Repr:c.WeakTypeTag](c: Context)(p: c.Expr[U => Boolean])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[Int] = {
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
    invokeAggregateKernel[T, Int,Repr](c)(predicv, seqlv, comblv)(zero)(comboper)(aggregateN[T, Int](c)(init, seqoper))(ctx)
  }

  def aggregateN[T: c.WeakTypeTag, R: c.WeakTypeTag](c: Context)(init: c.Expr[T => R], oper: c.Expr[(R, T) => R]) = c.universe.reify { (node: Node[T, R], chunkSize: Int, kernel: Reducables.ReducableKernel[T, R]) =>
    {
      val stealer = node.stealer
      if (!stealer.hasNext) kernel.zero
      else {
        var sum: R = init.splice.apply(stealer.next)

        while (stealer.hasNext) {
          sum = oper.splice(sum, stealer.next)
        }
        sum
      }
    }
  }

  def invokeAggregateKernel[T: c.WeakTypeTag, R: c.WeakTypeTag, Repr: c.WeakTypeTag](c: Context)(initializer: c.Expr[Unit]*)(z: c.Expr[R])(combiner: c.Expr[(R, R) => R])(applyerN: c.Expr[(Node[T, R], Int, Reducables.ReducableKernel[T, R]) => R])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[R] = {
    import c.universe._

    val calleeExpression = c.Expr[Reducables.OpsLike[T, Repr]](c.applyPrefix)
    val resultWithoutInit = reify {
      import scala._
      import collection.parallel
      import parallel._
      import workstealing._
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel =
        new scala.collection.parallel.workstealing.Reducables.ReducableKernel[T, R] {
          def zero = z.splice
          def combine(a: R, b: R) = combiner.splice.apply(a, b)
          def apply(node: Node[T, R], chunkSize: Int): R = applyerN.splice.apply(node, chunkSize, this)
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

  def reduce[T: c.WeakTypeTag, U >: T: c.WeakTypeTag, Repr: c.WeakTypeTag](c: Context)(op: c.Expr[(U, U) => U])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
    import c.universe._

    mapReduce[T, U, U, Repr](c)(reify { u: U => u })(op)(ctx)
  }

  def mapReduce[T: c.WeakTypeTag, U >: T: c.WeakTypeTag, R: c.WeakTypeTag, Repr: c.WeakTypeTag](c: Context)(mapper: c.Expr[U => R])(reducer: c.Expr[(R, R) => R])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[R] = {
    import c.universe._

    val (lv, op) = c.nonFunctionToLocal[(R, R) => R](reducer)
    val (mv, mop) = c.nonFunctionToLocal[U => R](mapper)
    val calleeExpression = c.Expr[Reducables.OpsLike[T, Repr]](c.applyPrefix)
    val result = reify {
      import scala.collection.parallel.workstealing._
      lv.splice
      mv.splice

      val callee = calleeExpression.splice
      val stealer = callee.stealer

      val kernel = new scala.collection.parallel.workstealing.Reducables.ReducableKernel[T, ResultCell[R]] {
        override def beforeWorkOn(tree: WorkstealingTreeScheduler.Ref[T, ResultCell[R]], node: WorkstealingTreeScheduler.Node[T, ResultCell[R]]) {
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
        def apply(node: Node[T, ResultCell[R]], chunkSize: Int): ResultCell[R] = {
          val stealer = node.stealer
          val intermediate = node.READ_INTERMEDIATE

          if (stealer.hasNext) {
            var current = if (intermediate.isEmpty) {
              val next = stealer.next
              mop.splice.apply(next)
            } else intermediate.result

            while (stealer.hasNext) {
              val next = stealer.next
              current = op.splice(current, mop.splice.apply(next))
            }
            intermediate.result = current
          }
          intermediate
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

  def min[T: c.WeakTypeTag, U >: T: c.WeakTypeTag, Repr: c.WeakTypeTag](c: Context)(ord: c.Expr[Ordering[U]], ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[T] = {
    import c.universe._

    val (ordv, ordg) = c.nonFunctionToLocal[Ordering[U]](ord)
    val op = reify { (x: T, y: T) => if (ordg.splice.compare(x, y) < 0) x else y }
    reify {
      ordv.splice
      reduce[T, T, Repr](c)(op)(ctx).splice
    }
  }

  def max[T: c.WeakTypeTag, U >: T: c.WeakTypeTag, Repr: c.WeakTypeTag](c: Context)(ord: c.Expr[Ordering[U]], ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[T] = {
    import c.universe._

    val (ordv, ordg) = c.nonFunctionToLocal[Ordering[U]](ord)
    val op = reify { (x: T, y: T) => if (ordg.splice.compare(x, y) > 0) x else y }
    reify {
      ordv.splice
      reduce[T, T, Repr](c)(op)(ctx).splice
    }
  }
}
