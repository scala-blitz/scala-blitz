package scala.collection.parallel.workstealing.methods



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.collection.parallel.workstealing._
import scala.collection.parallel.workstealing.WorkstealingTreeScheduler.Node



object ArraysMacros {

  /* macro implementations */

  def aggregate[T:c.WeakTypeTag, S: c.WeakTypeTag](c: Context)(z: c.Expr[S])(combop: c.Expr[(S, S) => S])(seqop: c.Expr[(S, T) => S])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[S] = {
    import c.universe._

    val (seqlv, seqoper) = c.functionExpr2Local[(S, T) => S](seqop)
    val (comblv, comboper) = c.functionExpr2Local[(S, S) => S](combop)
    val (zv, zg) = c.functionExpr2Local[S](z)
    val init = c.universe.reify { a: T => seqoper.splice.apply(zg.splice, a) }
    invokeAggregateKernel[T, S](c)(seqlv, comblv, zv)(zg)(comboper)(aggregateN[T,S](c)(init, seqoper))(ctx)
  }

  def fold[T:c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(z: c.Expr[U])(op: c.Expr[(U, U) => U])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
    val (lv, oper: c.Expr[(U, U) => U]) = c.functionExpr2Local[(U, U) => U](op)
    val (zv, zg: c.Expr[U]) = c.functionExpr2Local[U](z)
    val init = c.universe.reify { a: U => oper.splice.apply(zg.splice, a) }
    invokeAggregateKernel[T, U](c)(lv, zv)(zg)(oper)(aggregateN[T,U](c)(init, oper))(ctx)
  }

  def sum[T:c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(num: c.Expr[Numeric[U]], ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
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
    invokeAggregateKernel[T, U](c)(lv, numv, zerov)(zerog)(oper)(aggregateN[T,U](c)(init, oper))(ctx)
    
  }

  def product[T:c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(num: c.Expr[Numeric[U]], ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
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
    invokeAggregateKernel[T, U](c)(lv, numv, zerov)(zerog)(oper)(aggregateN[T,U](c)(init, oper))(ctx)

  }

  def count[T:c.WeakTypeTag](c: Context)(p: c.Expr[T => Boolean])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[Int] = {
    import c.universe._

    val (predicv, predic) = c.functionExpr2Local[T => Boolean](p)
    val zero = reify { 0 }
    val combop = reify {
      (x: Int, y: Int) => x + y
    }
    val seqop = reify {
      (x: Int, y: T) =>
        if (predic.splice(y)) x + 1 else x
    }
    val (seqlv, seqoper) = c.functionExpr2Local[(Int, T) => Int](seqop)
    val (comblv, comboper) = c.functionExpr2Local[(Int, Int) => Int](combop)
    val init = c.universe.reify { a: T => if (predic.splice(a)) 1 else 0; }
    invokeAggregateKernel[T, Int](c)(predicv, seqlv, comblv)(zero)(comboper)(aggregateN[T,Int](c)(init, seqoper))(ctx)
  }
 
  def aggregateN[T:c.WeakTypeTag, R: c.WeakTypeTag](c: Context)(init: c.Expr[T => R], oper: c.Expr[(R, T) => R]) = c.universe.reify { (from: Int, to: Int, zero: R, arr:Array[T]) =>
    {
      if(from>to) zero
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

  def invokeAggregateKernel[T:c.WeakTypeTag,  R: c.WeakTypeTag](c: Context)(initializer: c.Expr[Unit]*)(z: c.Expr[R])(combiner: c.Expr[(R, R) => R])(applyerN: c.Expr[(Int, Int, R, Array[T]) => R])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[R] = {
    import c.universe._

    val calleeExpression = c.Expr[Arrays.Ops[T]](c.applyPrefix)
    val resultWithoutInit = reify {
      import scala._
      import collection.parallel
      import parallel._
      import workstealing._
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel =
        new scala.collection.parallel.workstealing.Arrays.ArrayKernel[T,R] {
          def zero = z.splice
          def combine(a: R, b: R) = combiner.splice.apply(a, b)
          def apply(node: WorkstealingTreeScheduler.Node[Int, R], from: Int, to: Int) = applyerN.splice.apply(from, to, zero, callee.array)
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


  def reduce[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(operator: c.Expr[(U, U) => U])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
    import c.universe._

    val (lv, op) = c.functionExpr2Local[(U, U) => U](operator)
    val calleeExpression = c.Expr[Arrays.Ops[T]](c.applyPrefix)
    val result = reify {
      import scala.collection.parallel.workstealing._
      lv.splice
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel = new scala.collection.parallel.workstealing.Arrays.ArrayKernel[T, ResultCell[U]] {
        override def beforeWorkOn(tree: WorkstealingTreeScheduler.Ref[T, ResultCell[U]], node: WorkstealingTreeScheduler.Node[T, ResultCell[U]]) {
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
        def apply(node: Node[T, ResultCell[U]], from: Int, until: Int) = {
          val array = node.stealer.asInstanceOf[Arrays.ArrayStealer[T]].array
          val rc = node.READ_INTERMEDIATE
          if (from < until) {
            var sum: U = array(from)
            var i = from + 1
            while (i < until) {
              sum = op.splice(sum, array(i))
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



}

