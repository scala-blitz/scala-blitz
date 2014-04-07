package scala.collection.par.workstealing.internal



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.collection.par.generic._
import collection.par.Par
import collection.par.Scheduler
import collection.par.workstealing._
import scala.collection.par.Scheduler
import collection.par.Configuration
import scala.collection.par.Conc
import scala.math
import Optimizer._
import scala.reflect.macros.blackbox.{Context => BlackboxContext}


object ConcsMacros {

  /* macro implementations */

  def reduce[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(operator: c.Expr[(U, U) => U])(ctx: c.Expr[Scheduler]): c.Expr[U] = {
    import c.universe._

    val (lv, op) = c.nonFunctionToLocal[(U, U) => U](operator)
    val calleeExpression = c.Expr[Concs.Ops[T]](c.applyPrefix)
    val result = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._
      lv.splice
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel = new scala.collection.par.workstealing.Concs.ConcKernel[T, ResultCell[U]] {
        override def beforeWorkOn(tree: Scheduler.Ref[T, ResultCell[U]], node: Scheduler.Node[T, ResultCell[U]]) {
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
        final def applyTree(t: Conc[T], remaining: Int, cell: ResultCell[U]) = {
          def apply(t: Conc[T], remaining: Int): U = t match {
            case _: Conc.<>[T] | _: Conc.Append[T] =>
              val l = apply(t.left, remaining)
              val r = apply(t.right, remaining - t.left.size)
              op.splice(l, r)
            case c: Conc.Single[T] =>
              c.elem
            case c: Conc.Chunk[T] =>
              applyChunk(c, 0, remaining)
            case _ =>
              ???
          }

          val sum = apply(t, remaining)
          cell.result = if (cell.isEmpty) sum else op.splice(cell.result, sum)
          cell
        }
        final def applyChunk(c: Conc.Chunk[T], from: Int, remaining: Int, cell: ResultCell[U]) = {
          if (remaining > 0 && c.size > from) {
            val sum = applyChunk(c, from, remaining)
            cell.result = if (cell.isEmpty) sum else op.splice(cell.result, sum)
          }
          cell
        }
        private def min(a: Int, b: Int) = if (a < b) a else b
        private def applyChunk(c: Conc.Chunk[T], from: Int, remaining: Int): U = {
          var i = from + 1
          val until = min(from + remaining, c.size)
          var a = c.elems
          var sum: U = a(from)
          while (i < until) {
            sum = op.splice(sum, a(i))
            i += 1
          }
          sum
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

  def copyToArray[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: BlackboxContext)(arr: c.Expr[Array[U]], start: c.Expr[Int], len: c.Expr[Int])(ctx: c.Expr[Scheduler]): c.Expr[Unit] = {
    import c.universe._

    val calleeExpression = c.Expr[Concs.Ops[T]](c.applyPrefix)
    reify {
      import scala._
      import collection.par
      import par._
      import workstealing._
      import scala.collection.par.Scheduler.{Ref, Node}
      val callee = calleeExpression.splice
      val array = arr.splice
      val startIndex = start.splice
      val length = math.min(len.splice, math.min(callee.c.length, array.length - startIndex))
      val stealer = callee.stealer
      val kernel = new scala.collection.par.workstealing.Concs.ConcKernel[T, ProgressStatus] {
        override def beforeWorkOn(tree: Ref[T, ProgressStatus], node: Node[T, ProgressStatus]) {
        }
        override def afterCreateRoot(root: Ref[T, ProgressStatus]) {
          root.child.WRITE_INTERMEDIATE(new ProgressStatus(startIndex, startIndex))
        }
        override def afterExpand(oldnode: Node[T, ProgressStatus], newnode: Node[T, ProgressStatus]) {
          val stealer = newnode.stealer.asPrecise
          val completed = stealer.elementsCompleted
          val arrstart = oldnode.READ_INTERMEDIATE.start + completed
          val leftarrstart = arrstart
          val rightarrstart = arrstart + newnode.left.child.stealer.asPrecise.elementsRemaining

          newnode.left.child.WRITE_INTERMEDIATE(new ProgressStatus(leftarrstart, leftarrstart))
          newnode.right.child.WRITE_INTERMEDIATE(new ProgressStatus(rightarrstart, rightarrstart))
        }
        def zero = null
        def combine(a: ProgressStatus, b: ProgressStatus) = null
        final def applyTree(t: Conc[T], remaining: Int, status: ProgressStatus): ProgressStatus = {
          def apply(t: Conc[T], remaining: Int, idx: Int): Unit = t match {
            case _: Conc.<>[T] | _: Conc.Append[T] =>
              apply(t.left, remaining, idx)
              apply(t.right, remaining - t.left.size, idx + t.left.size)
            case c: Conc.Single[T] =>
              array(idx) = c.elem
            case c: Conc.Chunk[T] =>
              applyChunk(c, 0, idx, min(remaining, c.size))
            case _ =>
              ???
          }

          apply(t, remaining, status.progress)
          status.progress += min(t.size, remaining)
          status
        }
        final def applyChunk(c: Conc.Chunk[T], from: Int, remaining: Int, status: ProgressStatus): ProgressStatus = {
          val len = min(remaining, c.size - from)
          applyChunk(c, from, status.progress, len)
          status.progress += len
          status
        }
        private def min(a: Int, b: Int) = if (a < b) a else b
        private def applyChunk(c: Conc.Chunk[T], from: Int, idx: Int, length: Int) {
          Array.copy(c.elems, from, array, idx, length)
        }
      }
      ctx.splice.invokeParallelOperation(stealer, kernel)
      ()
    }
  }

}


object ConcsMethods {
  import scala.collection.par.workstealing._
  import scala.collection.par.Scheduler.{Ref, Node}

  class CopyToArrayKernel[T, U >: T](array: Array[U], startIndex: Int, length: Int) extends scala.collection.par.workstealing.Concs.ConcKernel[T, ProgressStatus] {
    override def beforeWorkOn(tree: Ref[T, ProgressStatus], node: Node[T, ProgressStatus]) {
    }
    override def afterCreateRoot(root: Ref[T, ProgressStatus]) {
      root.child.WRITE_INTERMEDIATE(new ProgressStatus(startIndex, startIndex))
    }
    override def afterExpand(oldnode: Node[T, ProgressStatus], newnode: Node[T, ProgressStatus]) {
      val stealer = newnode.stealer.asPrecise
      val completed = stealer.elementsCompleted
      val arrstart = oldnode.READ_INTERMEDIATE.start + completed
      val leftarrstart = arrstart
      val rightarrstart = arrstart + newnode.left.child.stealer.asPrecise.elementsRemaining
      newnode.left.child.WRITE_INTERMEDIATE(new ProgressStatus(leftarrstart, leftarrstart))
      newnode.right.child.WRITE_INTERMEDIATE(new ProgressStatus(rightarrstart, rightarrstart))
    }
    def zero = null
    def combine(a: ProgressStatus, b: ProgressStatus) = null
    def array_assign(idx: Int, a: Array[U], c: Conc.Single[T]) = array(idx) = c.elem
    final def applyTree(t: Conc[T], remaining: Int, status: ProgressStatus): ProgressStatus = {
      def apply(t: Conc[T], remaining: Int, idx: Int): Unit = t match {
        case _: Conc.<>[T] | _: Conc.Append[T] =>
          apply(t.left, remaining, idx)
          apply(t.right, remaining - t.left.size, idx + t.left.size)
        case c: Conc.Single[T] =>
          array_assign(idx, array, c)
        case c: Conc.Chunk[T] =>
          applyChunk(c, 0, idx, min(remaining, c.size))
        case _ =>
          ???
      }
      apply(t, remaining, status.progress)
      status.progress += min(t.size, remaining)
      status
    }
    final def applyChunk(c: Conc.Chunk[T], from: Int, remaining: Int, status: ProgressStatus): ProgressStatus = {
      val len = min(remaining, c.size - from)
      applyChunk(c, from, status.progress, len)
      status.progress += len
      status
    }
    private def min(a: Int, b: Int) = if (a < b) a else b
    private def applyChunk(c: Conc.Chunk[T], from: Int, idx: Int, length: Int) {
      Array.copy(c.elems, from, array, idx, length)
    }
  }

  def copyToArray[T, U >: T](c: Conc[T], array: Array[U], startIndex: Int, len: Int)(ctx: Scheduler): Unit = {
    val length = math.min(len, math.min(c.length, array.length - startIndex))
    val stealer = new Concs.Ops(c).stealer

    val kernel = new CopyToArrayKernel[T, U](array, startIndex, length)
    ctx.invokeParallelOperation(stealer, kernel)
    ()
  }

}












