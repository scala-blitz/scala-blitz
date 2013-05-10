package scala.collection.parallel.workstealing.methods



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.collection.parallel.generic._
import collection.parallel.Par
import collection.parallel.workstealing._



object RangesMacros {

  final val HAND_OPTIMIZATIONS_ENABLED = sys.props.get("scala.collection.parallel.range.manual_optimizations").map(_.toBoolean).getOrElse(true)

  /* macro implementations */

  def fold[U >: Int: c.WeakTypeTag](c: Context)(z: c.Expr[U])(op: c.Expr[(U, U) => U])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
    val (lv, oper: c.Expr[(U, U) => U]) = c.functionExpr2Local[(U, U) => U](op)
    val (zv, zg: c.Expr[U]) = c.functionExpr2Local[U](z)
    val init = c.universe.reify { a: U => oper.splice.apply(zg.splice, a) }
    invokeKernel[U, U, U, U](c)(lv, zv)(zg)(oper)(a0RetrunZero(c), a1Sum[U, U](c)(init, oper), aNSum[U, U](c)(init, oper))(ctx)(true)
  }

  def reduce[U >: Int: c.WeakTypeTag](c: Context)(op: c.Expr[(U, U) => U])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
    import c.universe._
    val zero = reify { Ranges.EMPTY_RESULT }

    val (lv, oper: Expr[(U, U) => U]) = c.functionExpr2Local[(U, U) => U](op)
    val combine = reify { (a: Any, b: Any) =>
      {
        if (a == zero.splice) b
        else if (b == zero.splice) a
        else oper.splice(a.asInstanceOf[U], b.asInstanceOf[U])
      }
    }
    val init = c.universe.reify { a: U => a }
    invokeKernel[U, U, U, Any](c)(lv)(zero)(combine)(a0RetrunZero[U, Any](c), a1Sum[U, Any](c)(init, oper), aNSum[U, Any](c)(init, oper))(ctx)(false)
  }

  def aggregate[S: c.WeakTypeTag](c: Context)(z: c.Expr[S])(combop: c.Expr[(S, S) => S])(seqop: c.Expr[(S, Int) => S])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[S] = {
    import c.universe._

    val (seqlv, seqoper) = c.functionExpr2Local[(S, Int) => S](seqop)
    val (comblv, comboper) = c.functionExpr2Local[(S, S) => S](combop)
    val (zv, zg) = c.functionExpr2Local[S](z)
    val init = c.universe.reify { a: Int => seqoper.splice.apply(zg.splice, a) }
    invokeKernel[Int, S, S, S](c)(seqlv, comblv, zv)(z)(comboper)(a0RetrunZero(c), a1Sum[S, S](c)(init, seqoper), aNSum[S, S](c)(init, seqoper))(ctx)(true)
  }

  def sum[U >: Int: c.WeakTypeTag](c: Context)(num: c.Expr[Numeric[U]], ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
    import c.universe._

    val (numv, numg) = c.functionExpr2Local[Numeric[U]](num)
    val (zerov, zerog) = c.functionExpr2Local[U](reify {
      numg.splice.zero
    })
    val calleeExpression = c.Expr[Ranges.Ops](c.applyPrefix)
    val op = reify {
      (x: U, y: U) => numg.splice.plus(x, y)
    }
    val (lv, oper: c.Expr[(U, U) => U]) = c.functionExpr2Local[(U, U) => U](op)
    val init = c.universe.reify { a: U => a }
    val computator = invokeKernel[U, U, U, U](c)(lv, numv, zerov)(zerog)(oper)(a0RetrunZero(c), a1Sum[U, U](c)(init, oper), aNSum[U, U](c)(init, oper))(ctx)(true)
    reify {
      if (HAND_OPTIMIZATIONS_ENABLED && (num.splice eq scala.math.Numeric.IntIsIntegral)) {
        val range = calleeExpression.splice.r

        if (range.isEmpty) 0 else (range.numRangeElements.toLong * (range.head + range.last) / 2).toInt
      } else { computator.splice }
    }

  }

  def product[U >: Int: c.WeakTypeTag](c: Context)(num: c.Expr[Numeric[U]], ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
    import c.universe._

    val (numv, numg) = c.functionExpr2Local[Numeric[U]](num)
    val (zerov, zerog) = c.functionExpr2Local[U](reify {
      numg.splice.one
    })
    val op = reify {
      (x: U, y: U) => numg.splice.times(x, y)
    }
    val (lv, oper: c.Expr[(U, U) => U]) = c.functionExpr2Local[(U, U) => U](op)
    val calleeExpression = c.Expr[Ranges.Ops](c.applyPrefix)
    val init = c.universe.reify { a: U => a }
    val computator = invokeKernel[U, U, U, U](c)(lv, numv, zerov)(zerog)(oper)(a0RetrunZero(c), a1Sum[U, U](c)(init, oper), aNSum[U, U](c)(init, oper))(ctx)(true)
    reify {
      if (HAND_OPTIMIZATIONS_ENABLED && (num.splice eq scala.math.Numeric.IntIsIntegral) && (calleeExpression.splice.r.containsSlice(1 to 34) || (calleeExpression.splice.r.contains(0)))) {
        0
      } else { computator.splice }
    }
  }

  def count(c: Context)(p: c.Expr[Int => Boolean])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[Int] = {
    import c.universe._

    val (predicv, predic) = c.functionExpr2Local[Int => Boolean](p)
    val zero = reify { 0 }
    val combop = reify {
      (x: Int, y: Int) => x + y
    }
    val seqop = reify {
      (x: Int, y: Int) =>
        if (predic.splice(y)) x + 1 else x
    }
    val (seqlv, seqoper) = c.functionExpr2Local[(Int, Int) => Int](seqop)
    val (comblv, comboper) = c.functionExpr2Local[(Int, Int) => Int](combop)
    val init = c.universe.reify { a: Int => if (predic.splice(a)) 1 else 0; }
    invokeKernel[Int, Int, Int, Int](c)(predicv, seqlv, comblv)(zero)(comboper)(a0RetrunZero(c), a1Sum[Int, Any](c)(init, seqoper), aNSum[Int, Int](c)(init, seqoper))(ctx)(true)
  }

  def min[U >: Int: c.WeakTypeTag](c: Context)(ord: c.Expr[Ordering[U]], ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[Int] = {
    import c.universe._

    val (ordv, ordg) = c.functionExpr2Local[Ordering[U]](ord)
    val op = reify {
      (x: Int, y: Int) => if (ordg.splice.compare(x, y) <= 0) x else y
    }
    val zero = reify { Ranges.EMPTY_RESULT }
    val (lv, oper: Expr[(Int, Int) => Int]) = c.functionExpr2Local[(Int, Int) => Int](op)
    val combine = reify { (a: Any, b: Any) =>
      {
        if (a == zero.splice) b
        else if (b == zero.splice) a
        else oper.splice(a.asInstanceOf[Int], b.asInstanceOf[Int])
      }
    }
    val init = c.universe.reify { a: Int => a }
    val computator = invokeKernel[Int, Int, Int, Any](c)(lv, ordv)(zero)(combine)(a0RetrunZero(c), a1Sum[Int, Any](c)(init, oper), aNSum[Int, Any](c)(init, oper))(ctx)(false)
    val calleeExpression = c.Expr[Ranges.Ops](c.applyPrefix)

    reify {
      if (HAND_OPTIMIZATIONS_ENABLED && (ord.splice eq Ordering.Int)) {
        val range = calleeExpression.splice.r
        if (range.step > 0) range.head
        else range.last
      } else { computator.splice }
    }
  }

  def max[U >: Int: c.WeakTypeTag](c: Context)(ord: c.Expr[Ordering[U]], ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[Int] = {
    import c.universe._

    val (ordv, ordg) = c.functionExpr2Local[Ordering[U]](ord)
    val op = reify {
      (x: Int, y: Int) => if (ordg.splice.compare(x, y) >= 0) x else y
    }
    val zero = reify { Ranges.EMPTY_RESULT }
    val (lv, oper: Expr[(Int, Int) => Int]) = c.functionExpr2Local[(Int, Int) => Int](op)
    val combine = reify { (a: Any, b: Any) =>
      {
        if (a == zero.splice) b
        else if (b == zero.splice) a
        else oper.splice(a.asInstanceOf[Int], b.asInstanceOf[Int])
      }
    }
    val init = c.universe.reify { a: Int => a }
    val computator = invokeKernel[Int, Int, Int, Any](c)(lv, ordv)(zero)(combine)(a0RetrunZero(c), a1Sum[Int, Any](c)(init, oper), aNSum[Int, Any](c)(init, oper))(ctx)(false)
    val calleeExpression = c.Expr[Ranges.Ops](c.applyPrefix)

    reify {
      if (HAND_OPTIMIZATIONS_ENABLED && (ord.splice eq Ordering.Int)) {
        val range = calleeExpression.splice.r
        if (range.step < 0) range.head
        else range.last
      } else { computator.splice }
    }

  }

  def a0RetrunZero[M: c.WeakTypeTag, R >: M: c.WeakTypeTag](c: Context): c.Expr[(Int, R) => R] = c.universe.reify { (at: Int, zero: R) => zero }
  def a1Sum[M: c.WeakTypeTag, R >: M: c.WeakTypeTag](c: Context)(init: c.Expr[Int => M], oper: c.Expr[(M, Int) => M]) = c.universe.reify { (from: Int, to: Int, zero: R) =>
    {
      val fin = if (from > to) from else to
      var i: Int = from + to - fin + 1
      var sum: M = init.splice.apply(from)
      while (i <= fin) {
        sum = oper.splice(sum, i)
        i += 1
      }
      sum
    }
  }
  def aNSum[M: c.WeakTypeTag, R >: M: c.WeakTypeTag](c: Context)(init: c.Expr[Int => M], oper: c.Expr[(M, Int) => M]) = c.universe.reify { (from: Int, to: Int, stride: Int, zero: R) =>
    {
      var i = from + stride
      var sum: M = init.splice.apply(from)
      if (stride > 0) {
        while (i <= to) {
          sum = oper.splice(sum, i)
          i += stride
        }
      } else if (stride < 0) {
        while (i >= to) {
          sum = oper.splice(sum, i)
          i += stride
        }
      } else ???
      sum
    }
  }

  def invokeKernel[U >: Int: c.WeakTypeTag, M: c.WeakTypeTag, RESULT: c.WeakTypeTag, REDUCE >: M: c.WeakTypeTag](c: Context)(initializer: c.Expr[Unit]*)(z: c.Expr[REDUCE])(combiner: c.Expr[(REDUCE, REDUCE) => REDUCE])(applyer0: c.Expr[(Int, REDUCE) => REDUCE], applyer1: c.Expr[(Int, Int, REDUCE) => M], applyerN: c.Expr[(Int, Int, Int, REDUCE) => M])(ctx: c.Expr[WorkstealingTreeScheduler])(allowZeroRezult: Boolean = true): c.Expr[RESULT] = {
    import c.universe._
    val calleeExpression = c.Expr[Ranges.Ops](c.applyPrefix)
    val resultWithoutInit =
      reify {
        import scala._
        import collection.parallel
        import parallel._
        import workstealing._
        val callee = calleeExpression.splice
        val stealer = callee.stealer
        val kernel =
          new scala.collection.parallel.workstealing.Ranges.RangeKernel[REDUCE, M] {
            def zero = z.splice
            def combine(a: REDUCE, b: REDUCE) = combiner.splice.apply(a, b)
            def apply0(at: Int) = applyer0.splice.apply(at, zero)
            def apply1(from: Int, to: Int) = applyer1.splice.apply(from, to, zero)
            def applyN(from: Int, to: Int, stride: Int) = applyerN.splice.apply(from, to, stride, zero)
          }
        val result = ctx.splice.invokeParallelOperation(stealer, kernel)
        (kernel, result)
      }
    val newChildren = initializer.flatMap { initializer =>
      val origTree = initializer.tree
      if (origTree.isDef) List(origTree) else origTree.children
    }.toList

    val resultTree = resultWithoutInit.tree match {
      case Block((children, expr)) => Block(newChildren ::: children, expr)
      case _ => c.abort(resultWithoutInit.tree.pos, "failed to get kernel as block")
    }
    val result = c.Expr[Tuple2[Ranges.RangeKernel[REDUCE, M], M]](resultTree)

    val operation = if (allowZeroRezult) reify { result.splice._2.asInstanceOf[RESULT] }
    else reify {
      val res = result.splice
      if (res._2 == res._1.zero) throw new java.lang.UnsupportedOperationException("result is empty")
      else res._2.asInstanceOf[RESULT]
    }

    c.inlineAndReset(operation)
  }

}
