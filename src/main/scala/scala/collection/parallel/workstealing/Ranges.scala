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
    def stealer: Stealer[Int] = new RangeStealer(r, 0, r.length)
    override def reduce[U >: Int](op: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler): U = macro methods.RangesMacros.reduce[U]
    override def fold[U >: Int](z: => U)(op: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler): U = macro methods.RangesMacros.fold[U]
    def aggregate[S](z: S)(combop: (S, S) => S)(seqop: (S, Int) => S)(implicit ctx: WorkstealingTreeScheduler): S = macro methods.RangesMacros.aggregate[S]
    def sum[U >: Int](implicit num: Numeric[U], ctx: WorkstealingTreeScheduler): U = macro methods.RangesMacros.sum[U]
    def product[U >: Int](implicit num: Numeric[U], ctx: WorkstealingTreeScheduler): U = macro methods.RangesMacros.product[U]
    def min[U >: Int](implicit ord: Ordering[U], ctx: WorkstealingTreeScheduler): Int = macro methods.RangesMacros.min[U]
    def max[U >: Int](implicit ord: Ordering[U], ctx: WorkstealingTreeScheduler): Int = macro methods.RangesMacros.max[U]

  }

  /* stealer implementation */

  import WorkstealingTreeScheduler.{ Kernel, Node }

  class RangeStealer(val range: collection.immutable.Range, start: Int, end: Int) extends IndexedStealer[Int](start, end) {
    type StealerType = RangeStealer

    var padding8: Int = _
    var padding9: Int = _
    var padding10: Int = _
    var padding11: Int = _
    var padding12: Int = _
    var padding13: Int = _
    var padding14: Int = _
    var padding15: Int = _

    def next(): Int = {
      val idx = nextProgress
      nextProgress += 1
      range.apply(idx)
    }

    def newStealer(s: Int, u: Int) = new RangeStealer(range, s, u)
  }

  abstract class RangeKernel[@specialized R] extends IndexedStealer.IndexedKernel[Int, R] {
    def apply(node: Node[Int, R], chunkSize: Int): R = {
      val stealer = node.stealer.asInstanceOf[RangeStealer]
      val nextProgress = stealer.nextProgress
      val nextUntil = stealer.nextUntil
      val range = stealer.range
      val from = range.apply(nextProgress)

      if (nextProgress == nextUntil) apply0(from)
      else {
        val to = range.apply(nextUntil - 1)
        val step = range.step

        if (step == 1) apply1(from, to)
        else applyN(from, to, step)
      }
    }
    def apply0(at: Int): R
    def apply1(from: Int, to: Int): R
    def applyN(from: Int, to: Int, stride: Int): R
  }

  val EMPTY_RESULT = new AnyRef

}

object RangeKernel {

  def A0_RETURN_ZERO[R: c.WeakTypeTag](c: Context): c.Expr[(Int, R) => R] = c.universe.reify { (at: Int, zero: R) => zero }
  def A1_SUM[R: c.WeakTypeTag](c: Context)(oper: c.Expr[(R, Int) => R]): c.Expr[(Int, Int, R) => R] = c.universe.reify { (from: Int, to: Int, zero: R) =>
    {
      var i: Int = from
      var sum: R = zero
      while (i <= to) {
        sum = oper.splice(sum, i)
        i += 1
      }
      sum
    }
  }
  def AN_SUM[R: c.WeakTypeTag](c: Context)(oper: c.Expr[(R, Int) => R]) = c.universe.reify { (from: Int, to: Int, stride: Int, zero: R) =>
    {
      var i = from
      var sum: R = zero
      while (i <= to) {
        sum = oper.splice(sum, i)
        i += stride
      }
      sum
    }
  }

  def makeKernel_Impl[U >: Int: c.WeakTypeTag, R: c.WeakTypeTag, RR: c.WeakTypeTag](c: Context)(initilizers: c.Expr[Unit]*)(z: c.Expr[R])(combiner: c.Expr[(R, R) => R])(applyer0: c.Expr[(Int, R) => R], applyer1: c.Expr[(Int, Int, R) => R], applyerN: c.Expr[(Int, Int, Int, R) => R])(ctx: c.Expr[WorkstealingTreeScheduler])(allowZeroRezult: Boolean = true): c.Expr[RR] = {
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
          new scala.collection.parallel.workstealing.Ranges.RangeKernel[R] {

            def zero = z.splice
            def combine(a: R, b: R) = combiner.splice.apply(a, b)
            def apply0(at: Int) = applyer0.splice.apply(at, zero)
            def apply1(from: Int, to: Int) = applyer1.splice.apply(from, to, zero)
            def applyN(from: Int, to: Int, stride: Int) = applyerN.splice.apply(from, to, stride, zero)
          }
        val result = ctx.splice.invokeParallelOperation(stealer, kernel)
        (kernel, result)
      }
    val newChildren = initilizers.flatMap { initilizer =>
      val origTree = initilizer.tree
      if (origTree.isDef) List(origTree) else origTree.children
    }.toList

    val resultTree = resultWithoutInit.tree match {
      case Block((children, expr)) => Block(newChildren ::: children, expr)
      case _ => c.abort(resultWithoutInit.tree.pos, "failed to get kernel as block " + resultWithoutInit.isInstanceOf[Block].toString)
    }
    val result = c.Expr[Tuple2[Ranges.RangeKernel[R], R]](resultTree)

    val operation = if (allowZeroRezult) reify { result.splice._2.asInstanceOf[RR] }
    else reify {
      val res = result.splice
      if (res._2 == res._1.zero) throw new java.lang.UnsupportedOperationException("result is empty")
      else res._2.asInstanceOf[RR]
    }

    c.inlineAndReset(operation)
  }

}
