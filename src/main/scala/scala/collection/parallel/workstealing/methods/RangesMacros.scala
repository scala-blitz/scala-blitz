package scala.collection.parallel.workstealing.methods



import scala.reflect.macros._
import scala.collection.parallel.generic._
import collection.parallel.Par
import collection.parallel.workstealing._



object RangesMacros {

  /* macro implementations */

  def fold[U >: Int: c.WeakTypeTag](c: Context)(z: c.Expr[U])(op: c.Expr[(U, U) => U])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
    import c.universe._

    val (lv, oper) = c.functionExpr2Local[(U, U) => U](op)
    val calleeExpression = c.Expr[Ranges.Ops](c.applyPrefix)
    val operation = reify {
      import collection.parallel.workstealing.Ranges
      lv.splice
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel = new Ranges.RangeKernel[U] {
        def zero = z.splice
        def combine(a: U, b: U) = oper.splice(a, b)
        def apply0(at: Int) = zero
        def apply1(from: Int, to: Int): U = {
          var i = from
          var sum: U = zero
          while (i <= to) {
            sum = oper.splice(sum, i)
            i += 1
          }
          sum
        }
        def applyN(from: Int, to: Int, stride: Int): U = {
          var i = from
          var sum: U = zero
          while (i <= to) {
            sum = oper.splice(sum, i)
            i += stride
          }
          sum
        }
      }
      ctx.splice.invokeParallelOperation(stealer, kernel)
    }
    c.inlineAndReset(operation)
  }

  def reduce[U >: Int: c.WeakTypeTag](c: Context)(op: c.Expr[(U, U) => U])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
    import c.universe._

    val (lv, oper) = c.functionExpr2Local[(U, U) => U](op)
    val calleeExpression = c.Expr[Ranges.Ops](c.applyPrefix)
    val operation = reify {
      import collection.parallel.workstealing.Ranges
      lv.splice
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel = new Ranges.RangeKernel[Any] {
        val zero = Ranges.EMPTY_RESULT
        def combine(a: Any, b: Any) = {
          if (a == zero) b
          else if (b == zero) a
          else oper.splice(a.asInstanceOf[U], b.asInstanceOf[U])
        }
        def apply0(at: Int) = zero
        def apply1(from: Int, to: Int): Any = {
          var i = from + 1
          var sum: U = from
          while (i <= to) {
            sum = oper.splice(sum, i)
            i += 1
          }
          sum
        }
        def applyN(from: Int, to: Int, stride: Int): Any = {
          var i = from + stride
          var sum: U = from
          while (i <= to) {
            sum = oper.splice(sum, i)
            i += stride
          }
          sum
        }
      }
      val res = ctx.splice.invokeParallelOperation(stealer, kernel)
      if (res == kernel.zero) throw new java.lang.UnsupportedOperationException("empty.reduce") else res.asInstanceOf[U]
    }
    c.inlineAndReset(operation)
  }

}


