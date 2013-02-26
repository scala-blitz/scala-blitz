package scala.collection.workstealing



import sun.misc.Unsafe
import annotation.tailrec
import collection._
import scala.language.experimental.macros
import scala.reflect.macros._



class ParRange(val range: Range, val config: Workstealing.Config)
extends IndexedWorkstealing[Int]
with ParIterableOperations[Int] {

  import IndexedWorkstealing._
  import Workstealing.initialStep

  def size = range.size

  type N[R] = RangeNode[R]

  type K[R] = RangeKernel[R]

  final class RangeNode[R](l: Ptr[Int, R], r: Ptr[Int, R])(val rng: Range, s: Int, e: Int, rn: Long, st: Int)
  extends IndexNode[Int, R](l, r)(s, e, rn, st) {
    var lindex = start

    def next(): Int = {
      val i = lindex
      lindex = i + 1
      rng.apply(i)
      //rng.start + i * rng.step // does not bring considerable performance gain
    }

    def newExpanded(parent: Ptr[Int, R]): RangeNode[R] = {
      val r = /*READ*/range
      val p = positiveProgress(r)
      val u = until(r)
      val remaining = u - p
      val firsthalf = remaining / 2
      val secondhalf = remaining - firsthalf
      val lnode = new RangeNode[R](null, null)(rng, p, p + firsthalf, createRange(p, p + firsthalf), initialStep)
      val rnode = new RangeNode[R](null, null)(rng, p + firsthalf, u, createRange(p + firsthalf, u), initialStep)
      val lptr = new Ptr[Int, R](parent, parent.level + 1)(lnode)
      val rptr = new Ptr[Int, R](parent, parent.level + 1)(rnode)
      val nnode = new RangeNode(lptr, rptr)(rng, start, end, r, step)
      nnode.owner = this.owner
      nnode
    }

  }

  abstract class RangeKernel[R] extends IndexKernel[Int, R] {
    def applyIndex(node: RangeNode[R], p: Int, np: Int) = {
      val rangestart = node.rng.start
      val step = node.rng.step
      val from = rangestart + step * p
      val to = rangestart + step * (np - 1)

      if (step == 1) applyRange1(node, from, to)
      else applyRange(node, from, to, step)
    }
    def applyRange(node: RangeNode[R], p: Int, np: Int, step: Int): R
    def applyRange1(node: RangeNode[R], p: Int, np: Int): R
  }

  def newRoot[R] = {
    val work = new RangeNode[R](null, null)(range, 0, size, createRange(0, size), initialStep)
    val root = new Ptr[Int, R](null, 0)(work)
    root
  }

  override def foreach[U](f: Int => U): Unit = macro ParRange.foreach[U]

  override def fold[U >: Int](z: U)(op: (U, U) => U): U = macro ParRange.fold[U]

  override def reduce[U >: Int](op: (U, U) => U): U = macro ParRange.reduce[U]

  override def aggregate[S](z: =>S)(combop: (S, S) => S)(seqop: (S, Int) => S): S = macro ParRange.aggregate[S]

  override def sum[U >: Int](implicit num: Numeric[U]): U = macro ParRange.sum[U]

  override def product[U >: Int](implicit num: Numeric[U]): U = macro ParRange.product[U]

  override def count(p: Int => Boolean): Int = macro ParRange.count

}


object ParRange {

  def foreach[U: c.WeakTypeTag](c: Context)(f: c.Expr[Int => U]): c.Expr[Unit] = {
    import c.universe._

    val (lv, func) = c.functionExpr2Local(f)
    val callee = c.Expr[Nothing](c.applyPrefix)
    val kernel = reify {
      lv.splice
      val xs = callee.splice.asInstanceOf[ParRange]
      xs.invokeParallelOperation(new xs.RangeKernel[Unit] {
        def zero = ()
        def combine(a: Unit, b: Unit) = a
        def applyRange(node: xs.RangeNode[Unit], from: Int, to: Int, step: Int) = {
          var i = from
          while (i <= to) {
            func.splice(i)
            i += step
          }
        }
        def applyRange1(node: xs.RangeNode[Unit], from: Int, to: Int) = {
          var i = from
          while (i <= to) {
            func.splice(i)
            i += 1
          }
        }       
      })
    }
    c.inlineAndReset(kernel)
  }

  def fold[U >: Int: c.WeakTypeTag](c: Context)(z: c.Expr[U])(op: c.Expr[(U, U) => U]): c.Expr[U] = {
    import c.universe._

    val (l, oper) = c.functionExpr2Local[(U, U) => U](op)
    val callee = c.Expr[Nothing](c.applyPrefix)
    val kernel = reify {
      l.splice
      val xs = callee.splice.asInstanceOf[ParRange]
      xs.invokeParallelOperation(new xs.RangeKernel[U] {
        val zero = z.splice
        def combine(a: U, b: U) = oper.splice(a, b)
        def applyRange(node: xs.RangeNode[U], from: Int, to: Int, step: Int) = {
          var i = from
          var sum = zero
          while (i <= to) {
            sum = oper.splice(sum, i)
            i += step
          }
          sum
        }
        def applyRange1(node: xs.RangeNode[U], from: Int, to: Int) = {
          var i = from
          var sum = zero
          while (i <= to) {
            sum = oper.splice(sum, i)
            i += 1
          }
          sum
        }
      })
    }
    c.inlineAndReset(kernel)
  }

  def reduce[U >: Int: c.WeakTypeTag](c: Context)(op: c.Expr[(U, U) => U]): c.Expr[U] = {
    import c.universe._

    val (lv, oper) = c.functionExpr2Local[(U, U) => U](op)
    val callee = c.Expr[Nothing](c.applyPrefix)
    val kernel = reify {
      lv.splice
      val xs = callee.splice.asInstanceOf[ParRange]
      val rs = xs.invokeParallelOperation(new xs.RangeKernel[Any] {
        val zero = ParIterableOperations.nil
        def combine(a: Any, b: Any) = {
          if (a == zero) b
          else if (b == zero) a
          else oper.splice(a.asInstanceOf[U], b.asInstanceOf[U])
        }
        def applyRange(node: xs.RangeNode[Any], from: Int, to: Int, step: Int) = {
          var i = from + step
          var sum: U = from
          while (i <= to) {
            sum = oper.splice(sum, i)
            i += step
          }
          sum
        }
        def applyRange1(node: xs.RangeNode[Any], from: Int, to: Int) = {
          var i = from + 1
          var sum: U = from
          while (i <= to) {
            sum = oper.splice(sum, i)
            i += 1
          }
          sum
        }
      })
      if (rs == ParIterableOperations.nil) throw new UnsupportedOperationException
      else rs.asInstanceOf[U]
    }
    c.inlineAndReset(kernel)
  }

  def aggregate[S: c.WeakTypeTag](c: Context)(z: c.Expr[S])(combop: c.Expr[(S, S) => S])(seqop: c.Expr[(S, Int) => S]): c.Expr[S] = {
    import c.universe._

    val (seqlv, seqoper) = c.functionExpr2Local[(S, Int) => S](seqop)
    val (comblv, comboper) = c.functionExpr2Local[(S, S) => S](combop)
    val callee = c.Expr[Nothing](c.applyPrefix)
    val kernel = reify {
      seqlv.splice
      comblv.splice
      val xs = callee.splice.asInstanceOf[ParRange]
      xs.invokeParallelOperation(new xs.RangeKernel[S] {
        def zero = z.splice
        def combine(a: S, b: S) = comboper.splice(a, b)
        def applyRange(node: xs.RangeNode[S], from: Int, to: Int, step: Int) = {
          var i = from
          var sum = zero
          while (i <= to) {
            sum = seqoper.splice(sum, i)
            i += step
          }
          sum
        }
        def applyRange1(node: xs.RangeNode[S], from: Int, to: Int) = {
          var i = from
          var sum = zero
          while (i <= to) {
            sum = seqoper.splice(sum, i)
            i += 1
          }
          sum
        }
      })
    }
    c.inlineAndReset(kernel)
  }

  def sum[U >: Int: c.WeakTypeTag](c: Context)(num: c.Expr[Numeric[U]]): c.Expr[U] = {
    import c.universe._

    val zero = reify {
      num.splice.zero
    }
    val op = reify {
      (x: U, y: U) => num.splice.plus(x, y)
    }
    fold[U](c)(zero)(op)
  }

  def product[U >: Int: c.WeakTypeTag](c: Context)(num: c.Expr[Numeric[U]]): c.Expr[U] = {
    import c.universe._

    val zero = reify {
      num.splice.one
    }
    val op = reify {
      (x: U, y: U) => num.splice.times(x, y)
    }
    fold[U](c)(zero)(op)
  }

  def count(c: Context)(p: c.Expr[Int => Boolean]): c.Expr[Int] = {
    import c.universe._

    val zero = reify { 0 }
    val combop = reify {
      (x: Int, y: Int) => x + y
    }
    val seqop = reify {
      (x: Int, y: Int) =>
      if (p.splice(y)) x + 1 else x
    }
    aggregate[Int](c)(zero)(combop)(seqop)
  }
}










