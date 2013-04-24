package scala.collection.parallel
package workstealing



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.collection.parallel.generic._



object Range {

  trait Scope {
    implicit def rangeOps(r: Par[collection.immutable.Range]) = new Range.Ops(r.xs)

    implicit def canMergeRange[T]: CanMergeFrom[Par[Range], Int, Par[Range]] = ???
  }

  class Ops(val r: collection.immutable.Range) extends AnyVal with Zippable.OpsLike[Int, Par[collection.immutable.Range]] {
    def stealer: Stealer[Int] = ???
    override def reduce[U >: Int](op: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler): U = macro Range.reduce[U]
  }

  /* stealer implementation */

  abstract class RangeStealer(val range: collection.immutable.Range, @volatile var progress: Int) extends PreciseStealer[Int] {
    var padding4: Int = _
    var padding5: Int = _
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

  val PROGRESS_OFFSET = unsafe.objectFieldOffset(classOf[RangeStealer].getDeclaredField("progress"))
  val EMPTY_RESULT = new AnyRef

  /* macro implementations */

  import WorkstealingTreeScheduler.{ Kernel, Node }

  def reduce[U >: Int: c.WeakTypeTag](c: Context)(op: c.Expr[(U, U) => U])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
    import c.universe._

    val (lv, oper) = c.functionExpr2Local[(U, U) => U](op)
    val calleeExpression = c.Expr[Par[collection.immutable.Range]](c.applyPrefix)
    println(c.applyPrefix)
    val kernel = reify {
      lv.splice
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val res = ctx.splice.invokeParallelOperation(stealer, new Kernel[Int, Any] {
        val zero = EMPTY_RESULT
        def combine(a: Any, b: Any) = {
          if (a == zero) b
          else if (b == zero) a
          else oper.splice(a.asInstanceOf[U], b.asInstanceOf[U])
        }
        def apply(node: Node[Int, Any], chunkSize: Int): Any = {
          val stealer = node.stealer.asInstanceOf[RangeStealer]
          val from = stealer.READ_PROGRESS
          val until = stealer.range.last
          val step = stealer.range.step

          if (step == 1) apply1(from, until)
          else applyN(from, until, step)
        }
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
      })
      val r: U = if (res == EMPTY_RESULT) throw new java.lang.UnsupportedOperationException("empty.reduce") else res.asInstanceOf[U]
      r
    }
    c.inlineAndReset(kernel)
  }

}




