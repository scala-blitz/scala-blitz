package scala.collection.par.workstealing.internal



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.reflect.ClassTag
import scala.collection.par.generic._
import scala.collection.par.Par
import scala.collection.par.Reducable
import scala.collection.par.Scheduler
import scala.collection.par.workstealing._
import scala.collection.par.Configuration
import scala.collection.par.Merger
import scala.collection.par.PreciseStealer
import scala.reflect.ClassTag
import scala.collection.par.workstealing.internal.Optimizer._
import scala.collection.par.Scheduler.Node
import scala.reflect.macros.blackbox.{Context => BlackboxContext}


object ReducablesMacros {

  def aggregate[T: c.WeakTypeTag, S: c.WeakTypeTag, Repr: c.WeakTypeTag](c: BlackboxContext)(z: c.Expr[S])(combop: c.Expr[(S, S) => S])(seqop: c.Expr[(S, T) => S])(ctx: c.Expr[Scheduler]): c.Expr[S] = {
    import c.universe._

    val (seqlv, seqoper) = c.nonFunctionToLocal[(S, T) => S](seqop)
    val (comblv, comboper) = c.nonFunctionToLocal[(S, S) => S](combop)
    val (zv, zg) = c.nonFunctionToLocal[S](z)
    val init = c.universe.reify { a: T => seqoper.splice.apply(zg.splice, a) }
    invokeAggregateKernel[T, S, Repr](c)(seqlv, comblv, zv)(zg)(comboper)(aggregateN[T, S](c)(init, seqoper))(ctx)
  }

  def fold[T: c.WeakTypeTag, U >: T: c.WeakTypeTag, Repr: c.WeakTypeTag](c: BlackboxContext)(z: c.Expr[U])(op: c.Expr[(U, U) => U])(ctx: c.Expr[Scheduler]): c.Expr[U] = {
    val (lv, oper: c.Expr[(U, U) => U]) = c.nonFunctionToLocal[(U, U) => U](op)
    val (zv, zg: c.Expr[U]) = c.nonFunctionToLocal[U](z)
    val init = c.universe.reify { a: U => oper.splice.apply(zg.splice, a) }
    invokeAggregateKernel[T, U, Repr](c)(lv, zv)(zg)(oper)(aggregateN[T, U](c)(init, oper))(ctx)
  }

  def sum[T: c.WeakTypeTag, U >: T: c.WeakTypeTag, Repr: c.WeakTypeTag](c: BlackboxContext)(num: c.Expr[Numeric[U]], ctx: c.Expr[Scheduler]): c.Expr[U] = {
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
    invokeAggregateKernel[T, U, Repr](c)(lv, numv, zerov)(zerog)(oper)(aggregateN[T, U](c)(init, oper))(ctx)
  }

  def foreach[T: c.WeakTypeTag, U >: T: c.WeakTypeTag, Repr: c.WeakTypeTag](c: BlackboxContext)(action: c.Expr[U => Unit])(ctx: c.Expr[Scheduler]): c.Expr[Unit] = {
    import c.universe._

    val (actionv, actiong) = c.nonFunctionToLocal[U => Unit](action)
    val init = c.universe.reify { a: U => actiong.splice.apply(a) }
    val seqoper = reify { (x: Unit, a: U) => actiong.splice.apply(a) }
    val zero = reify { () }
    val comboop = reify { (x: Unit, y: Unit) => () }
    invokeAggregateKernel[T, Unit, Repr](c)(actionv)(zero)(comboop)(aggregateN[T, Unit](c)(init, seqoper))(ctx)
  }

  def product[T: c.WeakTypeTag, U >: T: c.WeakTypeTag, Repr: c.WeakTypeTag](c: BlackboxContext)(num: c.Expr[Numeric[U]], ctx: c.Expr[Scheduler]): c.Expr[U] = {
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
    invokeAggregateKernel[T, U, Repr](c)(lv, numv, zerov)(zerog)(oper)(aggregateN[T, U](c)(init, oper))(ctx)
  }

  def count[T: c.WeakTypeTag, U >: T: c.WeakTypeTag, Repr: c.WeakTypeTag](c: BlackboxContext)(p: c.Expr[U => Boolean])(ctx: c.Expr[Scheduler]): c.Expr[Int] = {
    import c.universe._

    val (predicv, predic) = c.nonFunctionToLocal[T => Boolean](p)
    val zero = reify { 0 }
    val combop = reify {
      (x: Int, y: Int) => x + y
    }
    val seqop = reify {
      (x: Int, y: T) =>
        if (predic.splice(y)) x + 1 else x
    }
    val (seqlv, seqoper) = c.nonFunctionToLocal[(Int, T) => Int](seqop)
    val (comblv, comboper) = c.nonFunctionToLocal[(Int, Int) => Int](combop)
    val init = c.universe.reify { a: T => if (predic.splice(a)) 1 else 0; }
    invokeAggregateKernel[T, Int, Repr](c)(predicv, seqlv, comblv)(zero)(comboper)(aggregateN[T, Int](c)(init, seqoper))(ctx)
  }

  def aggregateN[T: c.WeakTypeTag, R: c.WeakTypeTag](c: BlackboxContext)(init: c.Expr[T => R], oper: c.Expr[(R, T) => R]) = c.universe.reify { (node: Node[T, R], chunkSize: Int, kernel: Reducables.ReducableKernel[T, R]) =>
    {
      val stealer = node.stealer
      if (!stealer.hasNext) kernel.zero
      else {
        var sum: R = init.splice.apply(stealer.next)

        while (stealer.hasNext) {
          sum = oper.splice(sum, stealer.next)
        }
        sum
      }
    }
  }

  def invokeAggregateKernel[T: c.WeakTypeTag, R: c.WeakTypeTag, Repr: c.WeakTypeTag](c: BlackboxContext)(initializer: c.Expr[Unit]*)(z: c.Expr[R])(combiner: c.Expr[(R, R) => R])(applyerN: c.Expr[(Node[T, R], Int, Reducables.ReducableKernel[T, R]) => R])(ctx: c.Expr[Scheduler]): c.Expr[R] = {
    import c.universe._

    val calleeExpression = c.Expr[Reducables.OpsLike[T, Repr]](c.applyPrefix)
    val resultWithoutInit = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel =
        new scala.collection.par.workstealing.Reducables.ReducableKernel[T, R] {
          def zero = z.splice
          def combine(a: R, b: R) = combiner.splice.apply(a, b)
          def apply(node: Node[T, R], chunkSize: Int): R = applyerN.splice.apply(node, chunkSize, this)
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

  def find[T: c.WeakTypeTag, U >: T: c.WeakTypeTag, Repr: c.WeakTypeTag](c: BlackboxContext)(p: c.Expr[U => Boolean])(ctx: c.Expr[Scheduler]): c.Expr[Option[T]] = {
    import c.universe._

    val (lv, pred) = c.nonFunctionToLocal[U => Boolean](p)

    val calleeExpression = c.Expr[Reducables.OpsLike[T, Repr]](c.applyPrefix)
    val result = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import internal._
      lv.splice
      val callee = calleeExpression.splice
      val stealer = callee.stealer
      val kernel = new scala.collection.par.workstealing.Reducables.ReducableKernel[T, Option[T]] {
        def zero = None
        def combine(a: Option[T], b: Option[T]) = if (a.isDefined) a else b
        def apply(node: Node[T, Option[T]], chunkSize: Int) = {
          var result: Option[T] = None
          val stealer = node.stealer
          while (stealer.hasNext && result.isEmpty) {
            val next = stealer.next()
            if (pred.splice(next)) result = Some(next)
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

  def forall[T: c.WeakTypeTag, U >: T: c.WeakTypeTag, Repr: c.WeakTypeTag](c: BlackboxContext)(p: c.Expr[U => Boolean])(ctx: c.Expr[Scheduler]): c.Expr[Boolean] = {
    import c.universe._

    val np = reify {
      (x: T) => !p.splice(x)
    }
    val found = find[T, T, Repr](c)(np)(ctx)
    reify {
      found.splice.isEmpty
    }
  }

  def exists[T: c.WeakTypeTag, U >: T: c.WeakTypeTag, Repr: c.WeakTypeTag](c: BlackboxContext)(p: c.Expr[U => Boolean])(ctx: c.Expr[Scheduler]): c.Expr[Boolean] = {
    import c.universe._

    val found = find[T, U, Repr](c)(p)(ctx)
    reify {
      found.splice.nonEmpty
    }
  }

  def reduce[T: c.WeakTypeTag, U >: T: c.WeakTypeTag, Repr: c.WeakTypeTag](c: BlackboxContext)(op: c.Expr[(U, U) => U])(ctx: c.Expr[Scheduler]): c.Expr[U] = {
    import c.universe._

    mapReduce[T, U, U, Repr](c)(reify { u: U => u })(op)(ctx)
  }

  def mapReduce[T: c.WeakTypeTag, U >: T: c.WeakTypeTag, R: c.WeakTypeTag, Repr: c.WeakTypeTag](c: BlackboxContext)(mapper: c.Expr[U => R])(reducer: c.Expr[(R, R) => R])(ctx: c.Expr[Scheduler]): c.Expr[R] = {
    import c.universe._

    val (lv, op) = c.nonFunctionToLocal[(R, R) => R](reducer)
    val (mv, mop) = c.nonFunctionToLocal[U => R](mapper)
    val calleeExpression = c.Expr[Reducables.OpsLike[T, Repr]](c.applyPrefix)
    val result = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      lv.splice
      mv.splice

      val callee = calleeExpression.splice
      val stealer = callee.stealer

      val kernel = new scala.collection.par.workstealing.Reducables.ReducableKernel[T, ResultCell[R]] {
        override def beforeWorkOn(tree: Scheduler.Ref[T, ResultCell[R]], node: Scheduler.Node[T, ResultCell[R]]) {
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
        def apply(node: Node[T, ResultCell[R]], chunkSize: Int): ResultCell[R] = {
          val stealer = node.stealer
          val intermediate = node.READ_INTERMEDIATE

          if (stealer.hasNext) {
            var current = if (intermediate.isEmpty) {
              val next = stealer.next
              mop.splice.apply(next)
            } else intermediate.result

            while (stealer.hasNext) {
              val next = stealer.next
              current = op.splice(current, mop.splice.apply(next))
            }
            intermediate.result = current
          }
          intermediate
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

  def min[T: c.WeakTypeTag, U >: T: c.WeakTypeTag, Repr: c.WeakTypeTag](c: BlackboxContext)(ord: c.Expr[Ordering[U]], ctx: c.Expr[Scheduler]): c.Expr[T] = {
    import c.universe._

    val (ordv, ordg) = c.nonFunctionToLocal[Ordering[U]](ord)
    val op = reify { (x: T, y: T) => if (ordg.splice.compare(x, y) < 0) x else y }
    reify {
      ordv.splice
      reduce[T, T, Repr](c)(op)(ctx).splice
    }
  }

  def max[T: c.WeakTypeTag, U >: T: c.WeakTypeTag, Repr: c.WeakTypeTag](c: BlackboxContext)(ord: c.Expr[Ordering[U]], ctx: c.Expr[Scheduler]): c.Expr[T] = {
    import c.universe._

    val (ordv, ordg) = c.nonFunctionToLocal[Ordering[U]](ord)
    val op = reify { (x: T, y: T) => if (ordg.splice.compare(x, y) > 0) x else y }
    reify {
      ordv.splice
      reduce[T, T, Repr](c)(op)(ctx).splice
    }
  }

  def invokeCopyMapKernel[T: c.WeakTypeTag, S: c.WeakTypeTag, Repr: c.WeakTypeTag](c: BlackboxContext)(f: c.Expr[T => S])(callee: c.Expr[Reducables.OpsLike[T, Repr]], ctx: c.Expr[Scheduler])(getTagForS: c.Expr[ClassTag[S]]) = {
    import c.universe._

    reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par.Scheduler
      import scala.collection.par.workstealing.ProgressStatus
      val sTag = getTagForS.splice
      val rootStealer = callee.splice.stealer.asInstanceOf[PreciseStealer[T]]
      val len = rootStealer.totalElements
      val sarray = sTag.newArray(len)
      val kernel = new Reducables.CopyMapReducableKernel[T, S] {
        import scala.collection.par.Scheduler.{ Ref, Node }
        import scala.collection.par.workstealing.Arrays.CopyProgress
        def resultArray = sarray
        def apply(node: Node[T, Unit], elementsToGet: Int) = {
          val stealer = node.stealer.asInstanceOf[PreciseStealer[T]]
          var desti = stealer.nextOffset
          while (stealer.hasNext) {
            sarray(desti) = f.splice(stealer.next)
            desti += 1
          }
          ()
        }
      }
      ctx.splice.invokeParallelOperation(rootStealer, kernel)
      new Par(kernel.resultArray)
    }
  }

  def transformerKernel[T: c.WeakTypeTag, S: c.WeakTypeTag, That: c.WeakTypeTag, Repr: c.WeakTypeTag](c: BlackboxContext)(callee: c.Expr[Reducables.OpsLike[T, Repr]], mergerExpr: c.Expr[Merger[S, That]], applyer: c.Expr[(Merger[S, That], T) => Any]): c.Expr[Reducables.ReducableKernel[T, Merger[S, That]]] = {
    import c.universe._

    reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par.Scheduler
      import scala.collection.par.Scheduler.{ Ref, Node }
      new Reducables.ReducableKernel[T, Merger[S, That]] {
        override def beforeWorkOn(tree: Ref[T, Merger[S, That]], node: Node[T, Merger[S, That]]) {
          node.WRITE_INTERMEDIATE(mergerExpr.splice)
        }
        def zero = null
        def combine(a: Merger[S, That], b: Merger[S, That]) =
          if (a eq null) b
          else if (b eq null) a
          else if (a eq b) a
          else a merge b
        def apply(node: Node[T, Merger[S, That]], elementsCount: Int) = {
          val merger = node.READ_INTERMEDIATE
          val stealer = node.stealer
          while (stealer.hasNext) {
            val elem = stealer.next
            applyer.splice(merger, elem)
          }
          merger
        }
      }
    }
  }

  def groupMapAggregate[T: c.WeakTypeTag, K: c.WeakTypeTag, M: c.WeakTypeTag, Repr: c.WeakTypeTag](c: BlackboxContext)(gr: c.Expr[T => K])(mp: c.Expr[T => M])(aggr: c.Expr[(M, M) => M])(kClassTag: c.Expr[scala.reflect.ClassTag[K]], mClassTag: c.Expr[scala.reflect.ClassTag[M]], ctx: c.Expr[Scheduler]) = {
    import c.universe._

    val (grv, grg) = c.nonFunctionToLocal[T => K](gr)
    val (mpv, mpg) = c.nonFunctionToLocal[T => M](mp)
    val (aggrv, aggrg) = c.nonFunctionToLocal[(M, M) => M](aggr)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[Reducables.OpsLike[T, Repr]](c.applyPrefix), "callee")
    val mergerExpr = reify { HashTables.newHashMapCombiningMerger[K, M](kClassTag.splice, mClassTag.splice, ctx.splice, aggrg.splice) }

    reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par.Scheduler
      import scala.collection.par.Scheduler.{ Ref, Node }
      import scala.collection.par.workstealing.HashTables._
      grv.splice
      mpv.splice
      aggrv.splice
      cv.splice
      
      val stealer = callee.splice.stealer
      val kernel = new Reducables.ReducableKernel[T, HashMapCombiningMerger[K, M]] {
        override def beforeWorkOn(tree: Ref[T, HashMapCombiningMerger[K, M]], node: Node[T, HashMapCombiningMerger[K, M]]) {
          node.WRITE_INTERMEDIATE(mergerExpr.splice)
        }
        def zero = null
        def combine(a: HashMapCombiningMerger[K, M], b: HashMapCombiningMerger[K, M]) =
          if (a eq null) b
          else if (b eq null) a
          else if (a eq b) a
          else a merge b
        def apply(node: Node[T, HashMapCombiningMerger[K, M]], elementsCount: Int) = {
          val merger = node.READ_INTERMEDIATE
          val stealer = node.stealer
          while (stealer.hasNext) {
            val elem = stealer.next
            val mappedElem = mpg.splice.apply(elem)
            val elemKey = grg.splice.apply(elem)
            merger += (elemKey, mappedElem)         
          }
          merger
        }
      }
      val resultMerger = ctx.splice.invokeParallelOperation(stealer, kernel)
      resultMerger.result
    }

  }

  def groupBy[T: c.WeakTypeTag, K: c.WeakTypeTag, Repr: c.WeakTypeTag, That <:AnyRef : c.WeakTypeTag ](c: BlackboxContext)(gr: c.Expr[T => K])(kClassTag: c.Expr[scala.reflect.ClassTag[K]], tClassTag: c.Expr[scala.reflect.ClassTag[T]], ctx: c.Expr[Scheduler], cmf:c.Expr[CanMergeFrom[Repr, T, That]]) = {
    import c.universe._

    val (grv, grg) = c.nonFunctionToLocal[T => K](gr)
    val (cmfv, cmfg) = c.nonFunctionToLocal[CanMergeFrom[Repr, T, That]](cmf)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[Reducables.OpsLike[T, Repr]](c.applyPrefix), "callee")
    val mergerExpr = reify { HashTables.newHashMapCollectingMerger[K, T, That, Repr](kClassTag.splice, tClassTag.splice, ctx.splice, cmfg.splice) }

    reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par.Scheduler
      import scala.collection.par.Scheduler.{ Ref, Node }
      import scala.collection.par.workstealing.HashTables._
      grv.splice
      cv.splice
      cmfv.splice
      
      val stealer = callee.splice.stealer
      val kernel = new Reducables.ReducableKernel[T, HashMapCollectingMerger[K, T, That, Repr]] {
        override def beforeWorkOn(tree: Ref[T, HashMapCollectingMerger[K, T, That, Repr]], node: Node[T, HashMapCollectingMerger[K, T, That, Repr]]) {
          node.WRITE_INTERMEDIATE(mergerExpr.splice)
        }
        def zero = null
        def combine(a: HashMapCollectingMerger[K, T, That, Repr], b: HashMapCollectingMerger[K, T, That, Repr]) =
          if (a eq null) b
          else if (b eq null) a
          else if (a eq b) a
          else a merge b
        def apply(node: Node[T, HashMapCollectingMerger[K, T, That, Repr]], elementsCount: Int) = {
          val merger = node.READ_INTERMEDIATE
          val stealer = node.stealer
          while (stealer.hasNext) {
            val elem = stealer.next
            val elemKey = grg.splice.apply(elem)
            merger += (elemKey, elem)         
          }
          merger
        }
      }
      val resultMerger = ctx.splice.invokeParallelOperation(stealer, kernel)
      resultMerger.result
    }

  }


  def map[T: c.WeakTypeTag, S: c.WeakTypeTag, That: c.WeakTypeTag, Repr: c.WeakTypeTag](c: BlackboxContext)(func: c.Expr[T => S])(cmf: c.Expr[CanMergeFrom[Repr, S, That]], ctx: c.Expr[Scheduler]): c.Expr[That] = {
    import c.universe._

    val (lv, f) = c.nonFunctionToLocal[T => S](func)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[Reducables.OpsLike[T, Repr]](c.applyPrefix), "callee")
    val (cmfv, canmerge) = c.nonFunctionToLocal(cmf, "cmf")
    val mergerExpr = reify { canmerge.splice.apply(callee.splice.seq) }
    val (mv, merger) = c.nonFunctionToLocal(mergerExpr, "merger")
    val stagExpr = reify { merger.splice.asInstanceOf[Arrays.ArrayMerger[S]].classTag }
    val cmkernel = invokeCopyMapKernel(c)(f)(callee, ctx)(stagExpr)
    val tkernel = transformerKernel(c)(callee, mergerExpr, reify { (merger: Merger[S, That], elem: T) => merger += f.splice(elem) })

    val operation = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par._
      import scala.collection.par.workstealing.Arrays
      import scala.collection.par.workstealing.Reducables
      import scala.collection.par.Scheduler
      import scala.collection.par.Scheduler.{ Ref, Node }
      import scala.reflect.ClassTag
      lv.splice
      cv.splice
      cmfv.splice
      mv.splice

      if (callee.splice.stealer.isInstanceOf[PreciseStealer[T]] && Arrays.isArrayMerger(merger.splice)) {
        cmkernel.splice.asInstanceOf[That]
      } else {
        val stealer = callee.splice.stealer
        val kernel = tkernel.splice
        val cmb = ctx.splice.invokeParallelOperation(stealer, kernel)
        cmb.result
      }
    }

    c.inlineAndReset(operation)
  }

  def flatMap[T: c.WeakTypeTag, S: c.WeakTypeTag, That: c.WeakTypeTag, Repr: c.WeakTypeTag](c: BlackboxContext)(func: c.Expr[T => TraversableOnce[S]])(cmf: c.Expr[CanMergeFrom[Repr, S, That]], ctx: c.Expr[Scheduler]): c.Expr[That] = {
    import c.universe._

    val (lv, f) = c.nonFunctionToLocal[T => TraversableOnce[S]](func)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[Reducables.OpsLike[T, Repr]](c.applyPrefix), "callee")
    val (cmfv, canmerge) = c.nonFunctionToLocal[CanMergeFrom[Repr, S, That]](cmf, "cmf")
    val mergerExpr = reify { canmerge.splice.apply(callee.splice.seq) }
    val applyer = reify {
      (merger: Merger[S, That], elem: T) => f.splice(elem).foreach(merger += _)
    }
    val tkernel = transformerKernel(c)(callee, mergerExpr, c.optimise(applyer))

    val operation = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par._
      import scala.collection.par.workstealing.Reducables
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

  def filter[T: c.WeakTypeTag, That: c.WeakTypeTag, Repr: c.WeakTypeTag](c: BlackboxContext)(pred: c.Expr[T => Boolean])(cmf: c.Expr[CanMergeFrom[Repr, T, That]], ctx: c.Expr[Scheduler]): c.Expr[That] = {
    import c.universe._

    val (pv, p) = c.nonFunctionToLocal[T => Boolean](pred)
    val (cv, callee) = c.nonFunctionToLocal(c.Expr[Reducables.OpsLike[T, Repr]](c.applyPrefix), "callee")
    val (cmfv, canmerge) = c.nonFunctionToLocal[CanMergeFrom[Repr, T, That]](cmf, "cmf")
    val mergerExpr = reify { canmerge.splice.apply(callee.splice.seq) }
    val tkernel = transformerKernel(c)(callee, mergerExpr, reify { (merger: Merger[T, That], elem: T) => if (p.splice(elem)) merger += elem })

    val operation = reify {
      import scala._
      import collection.par
      import par._
      import workstealing._

      import scala.collection.par._
      import scala.collection.par.workstealing.{ Arrays, Reducables }
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
