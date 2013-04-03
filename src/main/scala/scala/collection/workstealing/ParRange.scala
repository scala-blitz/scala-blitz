package scala.collection.workstealing



import sun.misc.Unsafe
import annotation.tailrec
import scala.collection._
import scala.language.experimental.macros
import scala.reflect.macros._



class ParRange(val range: Range, val config: Workstealing.Config) extends ParIterable[Int]
with ParIterableLike[Int, ParIterable[Int]]
with IndexedWorkstealing[Int] {

  import IndexedWorkstealing._

  def size = range.size

  protected[this] def newCombiner = new ParArray.ArrayCombiner[Int]

  type N[R] = RangeNode[R]

  type K[R] = RangeKernel[R]

  final class RangeNode[R](l: Ptr[Int, R], r: Ptr[Int, R])(val rng: Range, s: Int, e: Int, rn: Long, st: Int)
  extends IndexNode[Int, R](l, r)(s, e, rn, st) {
    def next(): Int = {
      val r = rng.apply(nextProgress)
      nextProgress += 1
      r
    }

    def hasNext: Boolean = nextProgress < nextUntil

    def newExpanded(parent: Ptr[Int, R], worker: Workstealing.Worker, kernel: Kernel[Int, R]): RangeNode[R] = {
      val r = /*READ*/range
      val p = positiveProgress(r)
      val u = until(r)
      val remaining = u - p
      val firsthalf = remaining / 2
      val secondhalf = remaining - firsthalf
      val lnode = new RangeNode[R](null, null)(rng, p, p + firsthalf, createRange(p, p + firsthalf), config.initialStep)
      val rnode = new RangeNode[R](null, null)(rng, p + firsthalf, u, createRange(p + firsthalf, u), config.initialStep)
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
    val work = new RangeNode[R](null, null)(range, 0, size, createRange(0, size), config.initialStep)
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

  // TODO fix when macros with generated bridges are fixed
  def min2[U >: Int](implicit ord: Ordering[U]): Int = macro ParRange.min[U]

  // TODO fix when macros with generated bridges are fixed
  def max2[U >: Int](implicit ord: Ordering[U]): Int = macro ParRange.max[U]

  override def find(p: Int => Boolean): Option[Int] = macro ParRange.find

  override def forall(p: Int => Boolean): Boolean = macro ParRange.forall

  override def exists(p: Int => Boolean): Boolean = macro ParRange.exists

  override def copyToArray[U >: Int](arr: Array[U], start: Int, len: Int): Unit = macro ParRange.copyToArray[U]

  override def copyToArray[U >: Int](arr: Array[U], start: Int): Unit = macro ParRange.copyToArray2[U]

  override def copyToArray[U >: Int](arr: Array[U]): Unit = macro ParRange.copyToArray3[U]

  // TODO fix when macros with generated bridges are fixed
  def filter2(p: Int => Boolean): ParIterable[Int] = macro ParRange.filter

  // TODO fix when macros with generated bridges are fixed
  def filterNot2(p: Int => Boolean): ParIterable[Int] = macro ParRange.filterNot

}


object ParRange {

  def foreach[U: c.WeakTypeTag](c: Context)(f: c.Expr[Int => U]): c.Expr[Unit] = {
    import c.universe._

    val (lv, func) = c.functionExpr2Local(f)
    val callee = c.Expr[ParRange](c.applyPrefix)
    val kernel = reify {
      lv.splice
      val xs = callee.splice
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
    val callee = c.Expr[ParRange](c.applyPrefix)
    val kernel = reify {
      l.splice
      val xs = callee.splice
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
    val callee = c.Expr[ParRange](c.applyPrefix)
    val kernel = reify {
      lv.splice
      val xs = callee.splice
      val rs = xs.invokeParallelOperation(new xs.RangeKernel[Any] {
        val zero = ParIterableLike.nil
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
      if (rs == ParIterableLike.nil) throw new java.lang.UnsupportedOperationException
      else rs.asInstanceOf[U]
    }
    c.inlineAndReset(kernel)
  }

  def aggregate[S: c.WeakTypeTag](c: Context)(z: c.Expr[S])(combop: c.Expr[(S, S) => S])(seqop: c.Expr[(S, Int) => S]): c.Expr[S] = {
    import c.universe._

    val (seqlv, seqoper) = c.functionExpr2Local[(S, Int) => S](seqop)
    val (comblv, comboper) = c.functionExpr2Local[(S, S) => S](combop)
    val callee = c.Expr[ParRange](c.applyPrefix)
    val kernel = reify {
      seqlv.splice
      comblv.splice
      val xs = callee.splice
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

  def min[U >: Int: c.WeakTypeTag](c: Context)(ord: c.Expr[Ordering[U]]): c.Expr[Int] = {
    import c.universe._

    val op = reify {
     (x: Int, y: Int) => if (ord.splice.compare(x, y) <= 0) x else y
    }
    reduce[Int](c)(op)
  }

  def max[U >: Int: c.WeakTypeTag](c: Context)(ord: c.Expr[Ordering[U]]): c.Expr[Int] = {
    import c.universe._

    val op = reify {
      (x: Int, y: Int) => if (ord.splice.compare(x, y) >= 0) x else y
    }
    reduce[Int](c)(op)
  }

  def find(c: Context)(p: c.Expr[Int => Boolean]): c.Expr[Option[Int]] = {
    import c.universe._

    val (lv, pred) = c.functionExpr2Local[Int => Boolean](p)
    val callee = c.Expr[ParRange](c.applyPrefix)
    val kernel = reify {
      lv.splice
      val xs = callee.splice
      xs.invokeParallelOperation(new xs.RangeKernel[Option[Int]] {
        def zero = None
        def combine(a: Option[Int], b: Option[Int]) = if (a.nonEmpty) a else b
        def applyRange(node: xs.RangeNode[Option[Int]], from: Int, to: Int, step: Int) = {
          var found: Option[Int] = None
          var i = from
          while (i <= to) {
            if (pred.splice(i)) {
              found = Some(i)
              notTermFlag = false
              i = to + 1
            } else i += step
          }
          found
        }
        def applyRange1(node: xs.RangeNode[Option[Int]], from: Int, to: Int) = {
          var found: Option[Int] = None
          var i = from
          while (i <= to) {
            if (pred.splice(i)) {
              found = Some(i)
              notTermFlag = false
              i = to + 1
            } else i += 1
          }
          found
        }
      })
    }
    c.inlineAndReset(kernel)
  }

  def forall(c: Context)(p: c.Expr[Int => Boolean]): c.Expr[Boolean] = {
    import c.universe._

    val np = reify {
      (x: Int) => !p.splice(x)
    }
    val found = find(c)(np)
    reify {
      found.splice.isEmpty
    }
  }

  def exists(c: Context)(p: c.Expr[Int => Boolean]): c.Expr[Boolean] = {
    import c.universe._

    val found = find(c)(p)
    reify {
      found.splice.nonEmpty
    }
  }

  def copyToArray[U >: Int: c.WeakTypeTag](c: Context)(arr: c.Expr[Array[U]], start: c.Expr[Int], len: c.Expr[Int]): c.Expr[Unit] = {
    import c.universe._

    val callee = c.Expr[ParRange](c.applyPrefix)
    val kernel = reify {
      val xs = callee.splice
      xs.invokeParallelOperation(new xs.RangeKernel[ParIterableLike.CopyToArrayStatus] {
        type Status = ParIterableLike.CopyToArrayStatus
        private def mathmin(a: Int, b: Int) = if (a < b) a else b
        override def beforeWorkOn(tree: xs.Ptr[Int, Status], node: xs.Node[Int, Status]) {
        }
        override def afterCreateRoot(root: xs.Ptr[Int, Status]) {
          root.child.lresult = new Status(start.splice, start.splice)
        }
        override def afterExpand(old: xs.Node[Int, Status], node: xs.Node[Int, Status]) {
          val completed = node.elementsCompleted
          val arrstart = old.lresult.arrayStart + completed
          val leftarrstart = arrstart
          val rightarrstart = arrstart + node.left.child.elementsRemaining

          node.left.child.lresult = new Status(leftarrstart, leftarrstart)
          node.right.child.lresult = new Status(rightarrstart, rightarrstart)
        }
        def zero = null
        def combine(a: Status, b: Status) = null
        def applyRange(node: xs.RangeNode[Status], from: Int, to: Int, step: Int) = {
          var i = node.lresult.arrayProgress
          var limit = mathmin(i + (to - from) + 1, mathmin(arr.splice.length, start.splice + len.splice))
          var j = from
          while (i < limit) {
            arr.splice(i) = j
            i += 1
            j += step
          }
          node.lresult.arrayProgress = i
          null
        }
        def applyRange1(node: xs.RangeNode[Status], from: Int, to: Int) = {
          var i = node.lresult.arrayProgress
          var limit = mathmin(i + (to - from) + 1, mathmin(arr.splice.length, start.splice + len.splice))
          var j = from
          while (i < limit) {
            arr.splice(i) = j
            i += 1
            j += 1
          }
          node.lresult.arrayProgress = i
          null
        }
      })
      ()
    }
    c.inlineAndReset(kernel)
  }

  def copyToArray2[U >: Int: c.WeakTypeTag](c: Context)(arr: c.Expr[Array[U]], start: c.Expr[Int]): c.Expr[Unit] = {
    import c.universe._

    val len = reify {
      arr.splice.length
    }
    copyToArray[U](c)(arr, start, len)
  }

  def copyToArray3[U >: Int: c.WeakTypeTag](c: Context)(arr: c.Expr[Array[U]]): c.Expr[Unit] = {
    import c.universe._

    val start = reify {
      0
    }
    val len = reify {
      arr.splice.length
    }
    copyToArray[U](c)(arr, start, len)
  }

  def filter(c: Context)(p: c.Expr[Int => Boolean]): c.Expr[ParIterable[Int]] = {
    import c.universe._

    val (lv, oper) = c.functionExpr2Local[Int => Boolean](p)
    val callee = c.Expr[ParRange](c.applyPrefix)
    val kernel = reify {
      lv.splice
      val xs = callee.splice
      val cmb = xs.invokeParallelOperation(new xs.RangeKernel[Combiner[Int, ParIterable[Int]]] {
        type Result = Combiner[Int, ParIterable[Int]]
        override def beforeWorkOn(tree: xs.Ptr[Int, Result], node: xs.Node[Int, Result]) {
          node.lresult = xs.createCombiner
        }
        def zero = null
        def combine(a: Result, b: Result) =
          if (a eq null) b
          else if (b eq null) a
          else if (a eq b) a
          else a combine b
        def applyRange(node: xs.RangeNode[Result], from: Int, to: Int, step: Int) = {
          val cmb = node.lresult
          var i = from
          while (i <= to) {
            if (p.splice(i)) cmb += i
            i += step
          }
          cmb
        }
        def applyRange1(node: xs.RangeNode[Result], from: Int, to: Int) = {
          val cmb = node.lresult
          var i = from
          while (i <= to) {
            if (p.splice(i)) cmb += i
            i += 1
          }
          cmb
        }
      })
      cmb.result
    }
    c.inlineAndReset(kernel)
  }

  def filterNot(c: Context)(p: c.Expr[Int => Boolean]): c.Expr[ParIterable[Int]] = {
    import c.universe._

    val np = reify {
      (x: Int) => !p.splice(x)
    }
    filter(c)(np)
  }

}













