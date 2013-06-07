package scala.collection.parallel.workstealing.methods



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.collection.parallel.generic._
import collection.parallel.Par
import collection.parallel.workstealing._
import collection.parallel.Configuration
import Optimizer._



object RangesMacros {

  /* macro implementations */

  def fold[U >: Int: c.WeakTypeTag](c: Context)(z: c.Expr[U])(op: c.Expr[(U, U) => U])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
    val (lv, oper: c.Expr[(U, U) => U]) = c.nonFunctionToLocal[(U, U) => U](op)
    val (zv, zg: c.Expr[U]) = c.nonFunctionToLocal[U](z)
    val init = c.universe.reify { a: U => oper.splice.apply(zg.splice, a) }
    invokeAggregateKernel[U, U](c)(lv, zv)(zg)(oper)(aggregateZero(c), aggregate1[U](c)(init, oper), aggregateN[U](c)(init, oper))(ctx)
  }

  def aggregate[S: c.WeakTypeTag](c: Context)(z: c.Expr[S])(combop: c.Expr[(S, S) => S])(seqop: c.Expr[(S, Int) => S])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[S] = {
    import c.universe._

    val (seqlv, seqoper) = c.nonFunctionToLocal[(S, Int) => S](seqop)
    val (comblv, comboper) = c.nonFunctionToLocal[(S, S) => S](combop)
    val (zv, zg) = c.nonFunctionToLocal[S](z)
    val init = c.universe.reify { a: Int => seqoper.splice.apply(zg.splice, a) }
    invokeAggregateKernel[Int, S](c)(seqlv, comblv, zv)(zg)(comboper)(aggregateZero(c), aggregate1[S](c)(init, seqoper), aggregateN[S](c)(init, seqoper))(ctx)
  }

  def sum[U >: Int: c.WeakTypeTag](c: Context)(num: c.Expr[Numeric[U]], ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
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
    val computator = invokeAggregateKernel[U, U](c)(lv, numv, zerov)(zerog)(oper)(aggregateZero(c), aggregate1[U](c)(init, oper), aggregateN[U](c)(init, oper))(ctx)
    reify {
      if (Configuration.manualOptimizations && (num.splice eq scala.math.Numeric.IntIsIntegral)) {
        val range = calleeExpression.splice.r

        if (range.isEmpty) 0 else (range.numRangeElements.toLong * (range.head + range.last) / 2).toInt
      } else { computator.splice }
    }

  }

  def product[U >: Int: c.WeakTypeTag](c: Context)(num: c.Expr[Numeric[U]], ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
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
    val computator = invokeAggregateKernel[U, U](c)(lv, numv, zerov)(zerog)(oper)(aggregateZero(c), aggregate1[U](c)(init, oper), aggregateN[U](c)(init, oper))(ctx)
    reify {
      if (Configuration.manualOptimizations && (num.splice eq scala.math.Numeric.IntIsIntegral) && (calleeExpression.splice.r.containsSlice(1 to 34) || (calleeExpression.splice.r.contains(0)))) {
        0
      } else { computator.splice }
    }
  }

  def count(c: Context)(p: c.Expr[Int => Boolean])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[Int] = {
    import c.universe._

    val (predicv, predic) = c.nonFunctionToLocal[Int => Boolean](p)
    val zero = reify { 0 }
    val combop = reify {
      (x: Int, y: Int) => x + y
    }
    val seqop = reify {
      (x: Int, y: Int) =>
        if (predic.splice(y)) x + 1 else x
    }
    val (seqlv, seqoper) = c.nonFunctionToLocal[(Int, Int) => Int](seqop)
    val (comblv, comboper) = c.nonFunctionToLocal[(Int, Int) => Int](combop)
    val init = c.universe.reify { a: Int => if (predic.splice(a)) 1 else 0; }
    invokeAggregateKernel[Int, Int](c)(predicv, seqlv, comblv)(zero)(comboper)(aggregateZero(c), aggregate1[Int](c)(init, seqoper), aggregateN[Int](c)(init, seqoper))(ctx)
  }

  def aggregateZero[R: c.WeakTypeTag](c: Context): c.Expr[(Int, R) => R] = c.universe.reify { (at: Int, zero: R) => zero }

  def aggregate1[R: c.WeakTypeTag](c: Context)(init: c.Expr[Int => R], oper: c.Expr[(R, Int) => R]) = c.universe.reify { (from: Int, to: Int, zero: R) =>
    {
      val fin = if (from > to) from else to
      var i: Int = from + to - fin + 1
      var sum: R = init.splice.apply(from)
      while (i <= fin) {
        sum = oper.splice(sum, i)
        i += 1
      }
      sum
    }
  }

  def aggregateN[R: c.WeakTypeTag](c: Context)(init: c.Expr[Int => R], oper: c.Expr[(R, Int) => R]) = c.universe.reify { (from: Int, to: Int, stride: Int, zero: R) =>
    {
      var i = from + stride
      var sum: R = init.splice.apply(from)
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

  def invokeAggregateKernel[U >: Int: c.WeakTypeTag, R: c.WeakTypeTag](c: Context)(initializer: c.Expr[Unit]*)(z: c.Expr[R])(combiner: c.Expr[(R, R) => R])(applyer0: c.Expr[(Int, R) => R], applyer1: c.Expr[(Int, Int, R) => R], applyerN: c.Expr[(Int, Int, Int, R) => R])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[R] = {
    import c.universe._

    val calleeExpression = c.Expr[Ranges.Ops](c.applyPrefix)
    val resultWithoutInit = reify {
      import scala._
      import collection.parallel
      import parallel._
      import workstealing._
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel =
        new scala.collection.parallel.workstealing.Ranges.RangeKernel[R] {
          def zero = z.splice
          def combine(a: R, b: R) = combiner.splice.apply(a, b)
          def apply0(node: WorkstealingTreeScheduler.Node[Int, R], at: Int) = applyer0.splice.apply(at, zero)
          def apply1(node: WorkstealingTreeScheduler.Node[Int, R], from: Int, to: Int) = applyer1.splice.apply(from, to, zero)
          def applyN(node: WorkstealingTreeScheduler.Node[Int, R], from: Int, to: Int, stride: Int) = applyerN.splice.apply(from, to, stride, zero)
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

  def reduce[U >: Int: c.WeakTypeTag](c: Context)(operator: c.Expr[(U, U) => U])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
    import c.universe._

    val (lv, op) = c.nonFunctionToLocal[(U, U) => U](operator)
    val calleeExpression = c.Expr[Ranges.Ops](c.applyPrefix)
    val result = reify {
      import scala.collection.parallel.workstealing._
      lv.splice
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel = new scala.collection.parallel.workstealing.Ranges.RangeKernel[ResultCell[U]] {
        override def beforeWorkOn(tree: WorkstealingTreeScheduler.Ref[Int, ResultCell[U]], node: WorkstealingTreeScheduler.Node[Int, ResultCell[U]]) {
          node.WRITE_INTERMEDIATE(new ResultCell[U])
        }
        def zero = new ResultCell[U]
        def combine(a: ResultCell[U], b: ResultCell[U]) = {
          if (a eq b) a
          else if (a.isEmpty) b
          else if (b.isEmpty) a
          else {
            val r = new ResultCell[U]
            r.result = op.splice(a.result, b.result)
            r
          }
        }
        def apply0(node: WorkstealingTreeScheduler.Node[Int, ResultCell[U]], at: Int) = node.READ_INTERMEDIATE
        def apply1(node: WorkstealingTreeScheduler.Node[Int, ResultCell[U]], from: Int, to: Int) = {
          val cell = node.READ_INTERMEDIATE
          var i = from + 1
          var sum: U = if (cell.isEmpty) from else op.splice(cell.result, from)
          while (i <= to) {
            sum = op.splice(sum, i)
            i += 1
          }
          cell.result = sum
          cell
        }
        def applyN(node: WorkstealingTreeScheduler.Node[Int, ResultCell[U]], from: Int, to: Int, stride: Int) = {
          val cell = node.READ_INTERMEDIATE
          var i = from + stride
          var sum: U = if (cell.isEmpty) from else op.splice(cell.result, from)
          if (stride > 0) {
            while (i <= to) {
              sum = op.splice(sum, i)
              i += stride
            }
          } else {
            while (i >= to) {
              sum = op.splice(sum, i)
              i += stride
            }
          }
          cell.result = sum
          cell
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

  def min[U >: Int: c.WeakTypeTag](c: Context)(ord: c.Expr[Ordering[U]], ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[Int] = {
    import c.universe._

    val calleeExpression = c.Expr[Ranges.Ops](c.applyPrefix)
    val (ordv, ordg) = c.nonFunctionToLocal[Ordering[U]](ord)
    val op = reify { (x: Int, y: Int) => if (ordg.splice.compare(x, y) < 0) x else y }
    val reduceResult = reduce[Int](c)(op)(ctx)

    reify {
      ordv.splice
      if (Configuration.manualOptimizations && (ordg.splice eq Ordering.Int)) {
        val range = calleeExpression.splice.r
        if (range.step > 0) range.head else range.last
      } else reduceResult.splice
    }
  }

  def max[U >: Int: c.WeakTypeTag](c: Context)(ord: c.Expr[Ordering[U]], ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[Int] = {
    import c.universe._

    val calleeExpression = c.Expr[Ranges.Ops](c.applyPrefix)
    val (ordv, ordg) = c.nonFunctionToLocal[Ordering[U]](ord)
    val op = reify { (x: Int, y: Int) => if (ordg.splice.compare(x, y) < 0) y else x }
    val reduceResult = reduce[Int](c)(op)(ctx)

    reify {
      ordv.splice
      if (Configuration.manualOptimizations && (ordg.splice eq Ordering.Int)) {
        val range = calleeExpression.splice.r
        if (range.step < 0) range.head else range.last
      } else reduceResult.splice
    }
  }

  def find(c: Context)(p: c.Expr[Int => Boolean])(ctx:c.Expr[WorkstealingTreeScheduler]): c.Expr[Option[Int]] = {
    import c.universe._

    val (lv, pred) = c.nonFunctionToLocal[Int => Boolean](p)

    val calleeExpression = c.Expr[Ranges.Ops](c.applyPrefix)
    val result = reify {
      import scala.collection.parallel.workstealing._
      import methods._
      lv.splice
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel = new scala.collection.parallel.workstealing.Ranges.RangeKernel[Option[Int]] {
        def zero = None
        def combine(a: Option[Int], b: Option[Int]) = if (a.isDefined) a else b
        def apply0(node: WorkstealingTreeScheduler.Node[Int, Option[Int]], at: Int) = {
          if (pred.splice(at)) {
            terminationCause = ResultFound
            Some(at)
          }
          else None
        }
        def apply1(node: WorkstealingTreeScheduler.Node[Int, Option[Int]], from: Int, to: Int) = {
          var i = from
          var result: Option[Int] = None
          while (i <= to && result.isEmpty) {
            if(pred.splice(i)) result = Some(i)
            i += 1
          }
          if(result.isDefined) terminationCause = ResultFound
          result
        }
        def applyN(node: WorkstealingTreeScheduler.Node[Int, Option[Int]], from: Int, to: Int, stride: Int) = {
          var i = from
          var result: Option[Int] = None
          if (stride > 0) {
            while (i <= to&&result.isEmpty) {
              if(pred.splice(i)) result = Some(i)
              i += stride
            }
          } else {
            while (i >= to&&result.isEmpty) {
              if(pred.splice(i)) result = Some(i)
              i += stride
            }
          }
          if(result.isDefined) terminationCause = ResultFound
          result
        }
      }
      val result = ctx.splice.invokeParallelOperation(stealer, kernel)
      result
    }

    c.inlineAndReset(result)
  }

  def forall(c: Context)(p: c.Expr[Int => Boolean])( ctx:c.Expr[WorkstealingTreeScheduler]): c.Expr[Boolean] = {
    import c.universe._

    val np = reify {
      (x: Int) => !p.splice(x)
    }
    val found = find(c)(np)(ctx)
    reify {
      found.splice.isEmpty
    }
  }

  def exists(c: Context)(p: c.Expr[Int => Boolean])(ctx:c.Expr[WorkstealingTreeScheduler]): c.Expr[Boolean] = {
    import c.universe._

    val found = find(c)(p)(ctx)
    reify {
      found.splice.nonEmpty
    }
  }

  def copyToArray1[U >: Int: c.WeakTypeTag](c: Context)(start: c.Expr[Int], len: c.Expr[Int], rhead: c.Expr[Int]) = c.universe.reify { (from: Int, to: Int, arr: Array[U]) => {
      val fin = if (from > to) from else to
      var i: Int = from + to - fin
      var dest: Int = start.splice + i - rhead.splice
      val dto = if(1 + to-from < len.splice - dest - 1) to else from + (len.splice - dest - 1)

      while (i <= dto) {
        arr(dest) = i
        i += 1
        dest += 1
      }
      arr
    }
  }

  def copyToArrayN[U >: Int: c.WeakTypeTag](c: Context)(start: c.Expr[Int], len: c.Expr[Int], rhead: c.Expr[Int]) = c.universe.reify { (from: Int, to: Int, stride: Int, arr: Array[U]) => {
      var i = from
      var dest = start.splice + (i - rhead.splice) / stride
      val dto = if(1 + (to-from)/stride < len.splice - dest - 1) to else from + (len.splice - dest - 1) * stride

      if(dest < len.splice) {
      if (stride > 0) {
        while (i <= dto) {
          arr(dest) = i
          i += stride
          dest += 1
        }
      } else if (stride < 0) {
        while (i >= dto) {
          arr(dest) = i
          i += stride
          dest += 1
        }
      } else ???}
      arr
    }
  }

  def invokeCopyToArrayKernel[U >: Int: c.WeakTypeTag](c: Context)(initializer: c.Expr[Unit]*)(arr: c.Expr[Array[U]], start: c.Expr[Int], len: c.Expr[Int])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[Unit] = {
    import c.universe._
    val (startv, startg) = c.nonFunctionToLocal[Int](start)
    val (lenv, lengg) = c.nonFunctionToLocal[Int](len)
    val (arrv, arrg) = c.nonFunctionToLocal[Array[U]](arr)
    val comboper: c.Expr[(Array[U], Array[U]) => Array[U]] = reify { (a: Array[U], b: Array[U]) => a }
    val calleeExpression = c.Expr[Ranges.Ops](c.applyPrefix)
    val rhead = reify { calleeExpression.splice.r.head }
    val result = invokeAggregateKernel[Int, Array[U]](c)(initializer ++ Seq(startv, lenv, arrv): _*)(arrg)(comboper)(
      aggregateZero(c),
      copyToArray1[U](c)(startg, lengg, rhead),
      copyToArrayN[U](c)(startg, lengg, rhead)
    )(ctx)

    reify {
      result.splice
      ()
    }
  }

  def copyToArray[U >: Int: c.WeakTypeTag](c: Context)(arr: c.Expr[Array[U]], start: c.Expr[Int], len: c.Expr[Int])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[Unit] = {
    invokeCopyToArrayKernel[U](c)()(arr, start, len)(ctx)
  }

  def copyToArray3[U >: Int: c.WeakTypeTag](c: Context)(arr: c.Expr[Array[U]])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[Unit] = {
    val (arrv,arrg) = c.nonFunctionToLocal[Array[U]](arr)
    val len = c.universe.reify{ arrg.splice.length }
    val start = c.universe.reify{ 0 }
    invokeCopyToArrayKernel[U](c)(arrv)(arr, start, len)(ctx)
  }

  def copyToArray2[U >: Int: c.WeakTypeTag](c: Context)(arr: c.Expr[Array[U]], start: c.Expr[Int])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[Unit] = {
    val (arrv,arrg) = c.nonFunctionToLocal[Array[U]](arr)
    val len = c.universe.reify{ arrg.splice.length }
    invokeCopyToArrayKernel[U](c)(arrv)(arr, start, len)(ctx)
  }


}

