package scala.collection.parallel.workstealing.methods



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.collection.parallel.workstealing._
import scala.collection.parallel.workstealing.WorkstealingTreeScheduler.Node



object ArraysMacros {

  /* macro implementations */

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

