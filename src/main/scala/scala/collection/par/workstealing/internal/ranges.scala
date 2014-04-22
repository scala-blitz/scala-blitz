package scala.collection.par.workstealing.internal



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.reflect.ClassTag
import scala.collection.par.generic._
import scala.collection.par.Par
import scala.collection.par.Scheduler
import scala.collection.par.workstealing._
import scala.collection.par.Configuration
import scala.collection.par.Merger
import Optimizer._
import scala.reflect.ClassTag
import scala.reflect.macros.blackbox.{Context => BlackboxContext}


object RangesMacros {

  /* macro implementations */

  def fold[U >: Int: c.WeakTypeTag](c: BlackboxContext)(z: c.Expr[U])(op: c.Expr[(U, U) => U])(ctx: c.Expr[Scheduler]): c.Expr[U] = {
    val (lv, oper: c.Expr[(U, U) => U]) = c.nonFunctionToLocal[(U, U) => U](op)
    val (zv, zg: c.Expr[U]) = c.nonFunctionToLocal[U](z)
    val init = c.universe.reify { a: U => oper.splice.apply(zg.splice, a) }
    invokeAggregateKernel[U, U](c)(lv, zv)(zg)(oper)(aggregateZero(c), aggregate1[U](c)(init, oper), aggregateN[U](c)(init, oper))(ctx)
  }

  def aggregate[S: c.WeakTypeTag](c: BlackboxContext)(z: c.Expr[S])(combop: c.Expr[(S, S) => S])(seqop: c.Expr[(S, Int) => S])(ctx: c.Expr[Scheduler]): c.Expr[S] = {
    import c.universe._

    val (seqlv, seqoper) = c.nonFunctionToLocal[(S, Int) => S](seqop)
    val (comblv, comboper) = c.nonFunctionToLocal[(S, S) => S](combop)
    val (zv, zg) = c.nonFunctionToLocal[S](z)
    val init = c.universe.reify { a: Int => seqoper.splice.apply(zg.splice, a) }
    invokeAggregateKernel[Int, S](c)(seqlv, comblv, zv)(zg)(comboper)(aggregateZero(c), aggregate1[S](c)(init, seqoper), aggregateN[S](c)(init, seqoper))(ctx)
  }

  def foreach[U >: Int: c.WeakTypeTag](c: BlackboxContext)(action: c.Expr[U => Unit])(ctx: c.Expr[Scheduler]): c.Expr[Unit] = {
    import c.universe._

    val (actionv, actiong) = c.nonFunctionToLocal[Int => Unit](action)
    val init = c.universe.reify { a: Int => actiong.splice.apply(a) }
    val seqoper = reify { (x: Unit, a: Int) => actiong.splice.apply(a) }
    val zero = reify { () }
    val comboop = reify { (x: Unit, y: Unit) => () }
    invokeAggregateKernel[Int, Unit](c)(actionv)(zero)(comboop)(aggregateZero(c), aggregate1[Unit](c)(init, seqoper), aggregateN[Unit](c)(init, seqoper))(ctx)
  }

  def sum[U >: Int: c.WeakTypeTag](c: BlackboxContext)(num: c.Expr[Numeric[U]], ctx: c.Expr[Scheduler]): c.Expr[U] = {
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

        if (range.isEmpty) 0 else (range.length.toLong * (range.head + range.last) / 2).toInt
      } else { computator.splice }
    }

  }

  def product[U >: Int: c.WeakTypeTag](c: BlackboxContext)(num: c.Expr[Numeric[U]], ctx: c.Expr[Scheduler]): c.Expr[U] = {
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

  def count[U>:Int: c.WeakTypeTag](c: BlackboxContext)(p: c.Expr[U => Boolean])(ctx: c.Expr[Scheduler]): c.Expr[Int] = {
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

  def aggregateZero[R: c.WeakTypeTag](c: BlackboxContext): c.Expr[(Scheduler.Node[Int, R], Int, Ranges.RangeKernel[R]) => R] = c.universe.reify { (node: Scheduler.Node[Int, R], at: Int, kernel: Ranges.RangeKernel[R]) => node.READ_INTERMEDIATE }

  def aggregate1[R: c.WeakTypeTag](c: BlackboxContext)(init: c.Expr[Int => R], oper: c.Expr[(R, Int) => R]) = c.universe.reify { (node:Scheduler.Node[Int, R], from: Int, to: Int, kernel: Ranges.RangeKernel[R]) =>
    {
      val fin = if (from > to) from else to
      var i: Int = from + to - fin
      var sum: R = node.READ_INTERMEDIATE
      while (i <= fin) {
        sum = oper.splice(sum, i)
        i += 1
      }
      sum
    }
  }

  def aggregateN[R: c.WeakTypeTag](c: BlackboxContext)(init: c.Expr[Int => R], oper: c.Expr[(R, Int) => R]) = c.universe.reify { (node: Scheduler.Node[Int, R], from: Int, to: Int, stride: Int, kernel: Ranges.RangeKernel[R]) =>
    {
      var i = from
      var sum: R = node.READ_INTERMEDIATE
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

  def invokeAggregateKernel[U >: Int: c.WeakTypeTag, R: c.WeakTypeTag](c: BlackboxContext)(initializer: c.Expr[Unit]*)(z: c.Expr[R])(combiner: c.Expr[(R, R) => R])(applyer0: c.Expr[(Scheduler.Node[Int, R], Int, Ranges.RangeKernel[R]) => R], applyer1: c.Expr[(Scheduler.Node[Int, R], Int, Int, Ranges.RangeKernel[R]) => R], applyerN: c.Expr[(Scheduler.Node[Int, R], Int, Int, Int, Ranges.RangeKernel[R]) => R])(ctx: c.Expr[Scheduler]): c.Expr[R] = {
    import c.universe._

    val calleeExpression = c.Expr[Ranges.Ops](c.applyPrefix)
    val resultWithoutInit = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel =
        new scala.collection.par.workstealing.Ranges.RangeKernel[R] {
          def zero = z.splice
          def combine(a: R, b: R) = combiner.splice.apply(a, b)
          def apply0(node: Scheduler.Node[Int, R], at: Int) = applyer0.splice.apply(node, at, this)
          def apply1(node: Scheduler.Node[Int, R], from: Int, to: Int) = applyer1.splice.apply(node, from, to, this)
          def applyN(node: Scheduler.Node[Int, R], from: Int, to: Int, stride: Int) = applyerN.splice.apply(node, from, to, stride, this)
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

  def reduce[U >: Int: c.WeakTypeTag](c: BlackboxContext)(operator: c.Expr[(U, U) => U])(ctx: c.Expr[Scheduler]): c.Expr[U] = mapReduce[U, U](c)(c.universe.reify { x: U => x })(operator)(ctx)

  def mapReduce[U >: Int: c.WeakTypeTag, R: c.WeakTypeTag](c: BlackboxContext)(mapper: c.Expr[U => R])(reducer: c.Expr[(R, R) => R])(ctx: c.Expr[Scheduler]): c.Expr[R] = {
    import c.universe._

    val (lv, op) = c.nonFunctionToLocal[(R, R) => R](reducer)
    val (mv, mop) = c.nonFunctionToLocal[U => R](mapper)
    val calleeExpression = c.Expr[Ranges.Ops](c.applyPrefix)
    val result = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      lv.splice
      mv.splice
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel = new scala.collection.par.workstealing.Ranges.RangeKernel[ResultCell[R]] {
        override def beforeWorkOn(tree: Scheduler.Ref[Int, ResultCell[R]], node: Scheduler.Node[Int, ResultCell[R]]) {
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
        def apply0(node: Scheduler.Node[Int, ResultCell[R]], at: Int) = node.READ_INTERMEDIATE
        def apply1(node: Scheduler.Node[Int, ResultCell[R]], from: Int, to: Int) = {
          val cell = node.READ_INTERMEDIATE
          var i = from + 1
          var sum: R = if (cell.isEmpty) mop.splice.apply(from) else op.splice(cell.result, mop.splice.apply(from))
          while (i <= to) {
            sum = op.splice(sum, mop.splice.apply(i))
            i += 1
          }
          cell.result = sum
          cell
        }
        def applyN(node: Scheduler.Node[Int, ResultCell[R]], from: Int, to: Int, stride: Int) = {
          val cell = node.READ_INTERMEDIATE
          var i = from + stride
          var sum: R = if (cell.isEmpty) mop.splice.apply(from) else op.splice(cell.result, mop.splice.apply(from))
          if (stride > 0) {
            while (i <= to) {
              sum = op.splice(sum, mop.splice.apply(i))
              i += stride
            }
          } else {
            while (i >= to) {
              sum = op.splice(sum, mop.splice.apply(i))
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

  def min[U >: Int: c.WeakTypeTag](c: BlackboxContext)(ord: c.Expr[Ordering[U]], ctx: c.Expr[Scheduler]): c.Expr[Int] = {
    import c.universe._

    val calleeExpression = c.Expr[Ranges.Ops](c.applyPrefix)
    val (ordv, ordg) = c.nonFunctionToLocal[Ordering[U]](ord)
    val op = reify { (x: Int, y: Int) => if (ordg.splice.compare(x, y) < 0) x else y }
    val reduceResult = reduce[Int](c)(op)(ctx)

    reify {
      ordv.splice
      if (Configuration.manualOptimizations && (ordg.splice eq scala.math.Ordering.Int)) {
        val range = calleeExpression.splice.r
        if (range.step > 0) range.head else range.last
      } else reduceResult.splice
    }
  }

  def max[U >: Int: c.WeakTypeTag](c: BlackboxContext)(ord: c.Expr[Ordering[U]], ctx: c.Expr[Scheduler]): c.Expr[Int] = {
    import c.universe._

    val calleeExpression = c.Expr[Ranges.Ops](c.applyPrefix)
    val (ordv, ordg) = c.nonFunctionToLocal[Ordering[U]](ord)
    val op = reify { (x: Int, y: Int) => if (ordg.splice.compare(x, y) < 0) y else x }
    val reduceResult = reduce[Int](c)(op)(ctx)

    reify {
      ordv.splice
      if (Configuration.manualOptimizations && (ordg.splice eq scala.math.Ordering.Int)) {
        val range = calleeExpression.splice.r
        if (range.step < 0) range.head else range.last
      } else reduceResult.splice
    }
  }

  def find[U >: Int : c.WeakTypeTag](c: BlackboxContext)(p: c.Expr[U => Boolean])(ctx: c.Expr[Scheduler]): c.Expr[Option[Int]] = {
    import c.universe._

    val (lv, pred) = c.nonFunctionToLocal[U => Boolean](p)

    val calleeExpression = c.Expr[Ranges.Ops](c.applyPrefix)
    val result = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import internal._
      lv.splice
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel = new scala.collection.par.workstealing.Ranges.RangeKernel[Option[Int]] {
        def zero = None
        def combine(a: Option[Int], b: Option[Int]) = if (a.isDefined) a else b
        def apply0(node: Scheduler.Node[Int, Option[Int]], at: Int) = {
          if (pred.splice(at)) {
            setTerminationCause(ResultFound)
            Some(at)
          } else None
        }
        def apply1(node: Scheduler.Node[Int, Option[Int]], from: Int, to: Int) = {
          var i = from
          var result: Option[Int] = None
          while (i <= to && result.isEmpty) {
            if (pred.splice(i)) result = Some(i)
            i += 1
          }
          if (result.isDefined) setTerminationCause(ResultFound)
          result
        }
        def applyN(node: Scheduler.Node[Int, Option[Int]], from: Int, to: Int, stride: Int) = {
          var i = from
          var result: Option[Int] = None
          if (stride > 0) {
            while (i <= to && result.isEmpty) {
              if (pred.splice(i)) result = Some(i)
              i += stride
            }
          } else {
            while (i >= to && result.isEmpty) {
              if (pred.splice(i)) result = Some(i)
              i += stride
            }
          }
          if (result.isDefined) setTerminationCause(ResultFound)
          result
        }
      }
      val result = ctx.splice.invokeParallelOperation(stealer, kernel)
      result
    }

    c.inlineAndReset(result)
  }

  def forall[U >: Int : c.WeakTypeTag](c: BlackboxContext)(p: c.Expr[U => Boolean])(ctx: c.Expr[Scheduler]): c.Expr[Boolean] = {
    import c.universe._

    val np = reify {
      (x: Int) => !p.splice(x)
    }
    val found = find(c)(np)(ctx)
    reify {
      found.splice.isEmpty
    }
  }

  def exists[U >: Int : c.WeakTypeTag](c: BlackboxContext)(p: c.Expr[U => Boolean])(ctx: c.Expr[Scheduler]): c.Expr[Boolean] = {
    import c.universe._

    val found = find(c)(p)(ctx)
    reify {
      found.splice.nonEmpty
    }
  }

  def copyToArray1[U >: Int: c.WeakTypeTag](c: BlackboxContext)(start: c.Expr[Int], len: c.Expr[Int], rhead: c.Expr[Int]) = c.universe.reify { (node: Scheduler.Node[Int, Array[U]], from: Int, to: Int, kernel: Ranges.RangeKernel[Array[U]]) =>
    {
      val arr = kernel.zero
      val fin = if (from > to) from else to
      var i: Int = from + to - fin
      var dest: Int = start.splice + i - rhead.splice
      val dto = if (1 + to - from < len.splice - dest - 1) to else from + (len.splice - dest - 1)

      while (i <= dto) {
        arr(dest) = i
        i += 1
        dest += 1
      }
      arr
    }
  }

  def copyToArrayN[U >: Int: c.WeakTypeTag](c: BlackboxContext)(start: c.Expr[Int], len: c.Expr[Int], rhead: c.Expr[Int]) = c.universe.reify { (node: Scheduler.Node[Int, Array[U]], from: Int, to: Int, stride: Int, kernel: Ranges.RangeKernel[Array[U]]) =>
    {
      val arr = kernel.zero
      var i = from
      var dest = start.splice + (i - rhead.splice) / stride
      val dto = if (1 + (to - from) / stride < len.splice - dest - 1) to else from + (len.splice - dest - 1) * stride

      if (dest < len.splice) {
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
        } else ???
      }
      arr
    }
  }

  def invokeCopyToArrayKernel[U >: Int: c.WeakTypeTag](c: BlackboxContext)(initializer: c.Expr[Unit]*)(arr: c.Expr[Array[U]], start: c.Expr[Int], len: c.Expr[Int])(ctx: c.Expr[Scheduler]): c.Expr[Unit] = {
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
      copyToArrayN[U](c)(startg, lengg, rhead))(ctx)

    reify {
      result.splice
      ()
    }
  }

  def copyToArray[U >: Int: c.WeakTypeTag](c: BlackboxContext)(arr: c.Expr[Array[U]], start: c.Expr[Int], len: c.Expr[Int])(ctx: c.Expr[Scheduler]): c.Expr[Unit] = {
    invokeCopyToArrayKernel[U](c)()(arr, start, len)(ctx)
  }

  def copyToArray3[U >: Int: c.WeakTypeTag](c: BlackboxContext)(arr: c.Expr[Array[U]])(ctx: c.Expr[Scheduler]): c.Expr[Unit] = {
    val (arrv, arrg) = c.nonFunctionToLocal[Array[U]](arr)
    val len = c.universe.reify { arrg.splice.length }
    val start = c.universe.reify { 0 }
    invokeCopyToArrayKernel[U](c)(arrv)(arr, start, len)(ctx)
  }

  def copyToArray2[U >: Int: c.WeakTypeTag](c: BlackboxContext)(arr: c.Expr[Array[U]], start: c.Expr[Int])(ctx: c.Expr[Scheduler]): c.Expr[Unit] = {
    val (arrv, arrg) = c.nonFunctionToLocal[Array[U]](arr)
    val len = c.universe.reify { arrg.splice.length }
    invokeCopyToArrayKernel[U](c)(arrv)(arr, start, len)(ctx)
  }

  def copyMapKernel[T >: Int: c.WeakTypeTag, S: c.WeakTypeTag](c: BlackboxContext)(f: c.Expr[T => S])(callee: c.Expr[Ranges.Ops])(getTagForS: c.Expr[ClassTag[S]]): c.Expr[Ranges.CopyMapRangeKernel[S]] = {
    import c.universe._

    reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par.Scheduler
      import scala.collection.par.workstealing.ProgressStatus
      val sTag = getTagForS.splice
      val range = callee.splice.r
      val len = range.size

      val sarray = sTag.newArray(len)
      new Ranges.CopyMapRangeKernel[S] {
        import scala.collection.par.Scheduler.{ Ref, Node }
        import scala.collection.par.workstealing.Arrays.CopyProgress

        def resultArray = sarray
        def applyN(node: Node[T, Unit], from: Int, to: Int, stride: Int) = {
          var i = from
          val rhead = range.head
          var dest = (i - rhead) / stride
          val dto = if (1 + (to - from) / stride < len - dest - 1) to else from + (len - dest - 1) * stride

          if (dest < len) {
            if (stride > 0) {
              while (i <= dto) {
                sarray(dest) = f.splice(i)
                i += stride
                dest += 1
              }
            } else if (stride < 0) {
              while (i >= dto) {
                sarray(dest) = f.splice(i)
                i += stride
                dest += 1
              }
            } else ???
          }
        }
      }
    }
  }

  def transformerKernel[T >: Int: c.WeakTypeTag, S: c.WeakTypeTag, That: c.WeakTypeTag](c: BlackboxContext)(callee: c.Expr[Ranges.Ops], mergerExpr: c.Expr[Merger[S, That]], applyer: c.Expr[(Merger[S, That], Int) => Any]): c.Expr[Ranges.RangeKernel[Merger[S, That]]] = {
    import c.universe._

    reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par.Scheduler
      import scala.collection.par.Scheduler.{ Ref, Node }
      new Ranges.RangeKernel[Merger[S, That]] {
        override def beforeWorkOn(tree: Ref[Int, Merger[S, That]], node: Node[Int, Merger[S, That]]) {
          node.WRITE_INTERMEDIATE(mergerExpr.splice)
        }
        def zero = null
        def combine(a: Merger[Int, That], b: Merger[Int, That]) =
          if (a eq null) b
          else if (b eq null) a
          else if (a eq b) a
          else a merge b
        def applyN(node: Node[Int, Merger[S, That]], from: Int, to: Int, stride: Int) = {
          val merger = node.READ_INTERMEDIATE
          var i = from
          if (stride > 0) {
            while (i <= to) {
              applyer.splice(merger, i)
              i += stride
            }
          } else if (stride < 0) {
            while (i >= to) {
              applyer.splice(merger, i)
              i += stride
            }
          } else ???
          merger
        }
        def apply1(node: Node[Int, Merger[S, That]], from: Int, to: Int) = applyN(node, from, to, 1)
        def apply0(node: Node[Int, Merger[S, That]], from: Int) = applyN(node, from, from, 1)
      }
    }
  }

  def map[T >: Int: c.WeakTypeTag, S: c.WeakTypeTag, That: c.WeakTypeTag](c: BlackboxContext)(func: c.Expr[T => S])(cmf: c.Expr[CanMergeFrom[Par[Range], S, That]], ctx: c.Expr[Scheduler]): c.Expr[That] = {
    import c.universe._

    val (lv, f) = c.nonFunctionToLocal[T => S](func)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[Ranges.Ops](c.applyPrefix), "callee")
    val (cmfv, canmerge) = c.nonFunctionToLocal(cmf, "cmf")
    val mergerExpr = reify { canmerge.splice.apply(callee.splice.range) }
    val (mv, merger) = c.nonFunctionToLocal(mergerExpr, "merger")
    val stagExpr = reify { merger.splice.asInstanceOf[Arrays.ArrayMerger[S]].classTag }
    val lengthExpr = reify { callee.splice.r.length }
    val cmkernel = copyMapKernel(c)(f)(callee)(stagExpr)
    val tkernel = transformerKernel(c)(callee, mergerExpr, reify { (merger: Merger[S, That], elem: T) => merger += f.splice(elem) })

    val operation = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par.workstealing.Ranges
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

  def flatMap[T >: Int: c.WeakTypeTag, S: c.WeakTypeTag, That: c.WeakTypeTag](c: BlackboxContext)(func: c.Expr[T => TraversableOnce[S]])(cmf: c.Expr[CanMergeFrom[Par[Range], S, That]], ctx: c.Expr[Scheduler]): c.Expr[That] = {
    import c.universe._

    val (lv, f) = c.nonFunctionToLocal[T => TraversableOnce[S]](func)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[Ranges.Ops](c.applyPrefix), "callee")
    val (cmfv, canmerge) = c.nonFunctionToLocal[CanMergeFrom[Par[Range], S, That]](cmf, "cmf")
    val mergerExpr = reify { canmerge.splice.apply(callee.splice.range) }
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
      import scala.collection.par.workstealing.Ranges
      import scala.collection.par.Scheduler
      import scala.collection.par.Scheduler.{ Ref, Node }
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

  def filter[That:c.WeakTypeTag](c: BlackboxContext)(pred: c.Expr[Int => Boolean])(cmf: c.Expr[CanMergeFrom[Par[Range], Int, That]], ctx: c.Expr[Scheduler]): c.Expr[That] = {
    import c.universe._

    val (pv, p) = c.nonFunctionToLocal[Int => Boolean](pred)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[Ranges.Ops](c.applyPrefix), "callee")
    val (cmfv, canmerge) = c.nonFunctionToLocal[CanMergeFrom[Par[Range], Int, That]](cmf, "cmf")
    val mergerExpr = reify { canmerge.splice.apply(callee.splice.seq) }
    val tkernel = transformerKernel[Int, Int, That](c)(callee, mergerExpr, reify { (merger: Merger[Int, That], elem: Int) => if (p.splice(elem)) merger += elem })

    val operation = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par.workstealing.{ Arrays, Ranges }
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

}

