package scala.collection.parallel
package workstealing



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.collection.parallel.generic._



object Ranges {

  trait Scope {
    implicit def rangeOps(r: Par[collection.immutable.Range]) = new Ranges.Ops(r.xs)

    implicit def canMergeRange[T]: CanMergeFrom[Par[Range], Int, Par[Range]] = ???
  }

  class Ops(val r: collection.immutable.Range) extends AnyVal with Zippable.OpsLike[Int, Par[collection.immutable.Range]] {
    def stealer: Stealer[Int] = ???
    override def reduce[U >: Int](op: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler): U = macro RangesMacros.reduce[U]
  }

  /* stealer implementation */

  import WorkstealingTreeScheduler.{ Kernel, Node }

  abstract class RangeStealer(val range: collection.immutable.Range, @volatile var progress: Int) extends PreciseStealer[Int] {
    var nextProgress: Int = _
    var nextUntil: Int = _
    var padding6: Int = _
    var padding7: Int = _
    var padding8: Int = _
    var padding9: Int = _
    var padding10: Int = _
    var padding11: Int = _
    var padding12: Int = _
    var padding13: Int = _
    var padding14: Int = _
    var padding15: Int = _

    def READ_PROGRESS = unsafe.getIntVolatile(this, PROGRESS_OFFSET)
  }

  abstract class RangeKernel[R] extends Kernel[Int, R] {
    def apply(node: Node[Int, R], chunkSize: Int): R = {
      val stealer = node.stealer.asInstanceOf[RangeStealer]
      val startIndex = stealer.nextProgress
      val endIndex = stealer.nextUntil
      val range = stealer.range
      val from = range.apply(startIndex)
      val to = range.apply(endIndex)
      val step = range.step

      if (endIndex - startIndex == 0) apply0(from)
      else if (step == 1) apply1(from, to)
      else applyN(from, to, step)
    }
    def apply0(at: Int): R
    def apply1(from: Int, to: Int): R
    def applyN(from: Int, to: Int, step: Int): R
  }

  val PROGRESS_OFFSET = unsafe.objectFieldOffset(classOf[RangeStealer].getDeclaredField("progress"))
  val EMPTY_RESULT = new AnyRef

}


object RangesMacros {

  /* macro implementations */

  def reduce[U >: Int: c.WeakTypeTag](c: Context)(op: c.Expr[(U, U) => U])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
    import c.universe._

    val (lv, oper) = c.functionExpr2Local[(U, U) => U](op)
    val calleeExpression = c.Expr[Ranges.Ops](c.applyPrefix)
    val operation = reify {
      import scala.collection.parallel.workstealing.WorkstealingTreeScheduler
      lv.splice
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel = new Par.WorkstealingRanges.RangeKernel[Any] {
        val zero = Par.WorkstealingRanges.EMPTY_RESULT
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


