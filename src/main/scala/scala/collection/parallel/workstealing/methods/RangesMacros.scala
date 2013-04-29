package scala.collection.parallel.workstealing.methods



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.collection.parallel.generic._
import collection.parallel.Par
import collection.parallel.workstealing._



object RangesMacros {
  import RangeKernel._
  
  /* macro implementations */

  def fold[U >: Int: c.WeakTypeTag](c: Context)(z: c.Expr[U])(op: c.Expr[(U, U) => U])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
    val (lv, oper: c.Expr[(U, U) => U]) = c.functionExpr2Local[(U, U) => U](op)

    makeKernel_Impl[U, U, U](c)(lv)(z)(oper)(A0_RETURN_ZERO(c), A1_SUM[U](c)(oper), AN_SUM[U](c)(oper))(ctx)(true)
  }

  def reduce[U >: Int: c.WeakTypeTag](c: Context)(op: c.Expr[(U, U) => U])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
    import c.universe._
    val zero = reify { Ranges.EMPTY_RESULT }

    val (lv, oper: Expr[(U, U) => U]) = c.functionExpr2Local[(U, U) => U](op)
    val combine = reify { (a: Any, b: Any) =>
      {
        if (a == zero.splice) b
        else if (b == zero.splice) a
        else oper.splice(a.asInstanceOf[U], b.asInstanceOf[U])
      }
    }

    makeKernel_Impl[U, Any, U](c)(lv)(zero)(combine)(A0_RETURN_ZERO(c), A1_SUM[Any](c)(combine), AN_SUM[Any](c)(combine))(ctx)(false)
  }

  def aggregate[S: c.WeakTypeTag](c: Context)(z: c.Expr[S])(combop: c.Expr[(S, S) => S])(seqop: c.Expr[(S, Int) => S])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[S] = {
    import c.universe._

    val (seqlv, seqoper) = c.functionExpr2Local[(S, Int) => S](seqop)
    val (comblv, comboper) = c.functionExpr2Local[(S, S) => S](combop)
    
    makeKernel_Impl[Int, S, S](c)(seqlv,comblv)(z)(comboper)(A0_RETURN_ZERO(c), A1_SUM[S](c)(seqoper), AN_SUM[S](c)(seqoper))(ctx)(true)
  }

  def sum[U >: Int: c.WeakTypeTag](c: Context)(num: c.Expr[Numeric[U]],ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
    import c.universe._
    
    val (numv, numg) = c.functionExpr2Local[Numeric[U]](num)
    val zero = reify {
      numg.splice.zero
    }
    val op = reify {
      (x: U, y: U) => numg.splice.plus(x, y)
    }
    val (lv, oper: c.Expr[(U, U) => U]) = c.functionExpr2Local[(U, U) => U](op)


    makeKernel_Impl[U, U, U](c)(lv,numv)(zero)(oper)(A0_RETURN_ZERO(c), A1_SUM[U](c)(oper), AN_SUM[U](c)(oper))(ctx)(true)
  }

  def product[U >: Int: c.WeakTypeTag](c: Context)(num: c.Expr[Numeric[U]],ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
    import c.universe._

    val (numv, numg) = c.functionExpr2Local[Numeric[U]](num)
    val zero = reify {
      numg.splice.one
    }
    val op = reify {
      (x: U, y: U) => numg.splice.times(x, y)
    }
    val (lv, oper: c.Expr[(U, U) => U]) = c.functionExpr2Local[(U, U) => U](op)


    makeKernel_Impl[U, U, U](c)(lv,numv)(zero)(oper)(A0_RETURN_ZERO(c), A1_SUM[U](c)(oper), AN_SUM[U](c)(oper))(ctx)(true)
  }

  def count(c: Context)(p: c.Expr[Int => Boolean])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[Int] = {
    import c.universe._

    val (predicv, predic) = c.functionExpr2Local[Int => Boolean](p)
    val zero = reify { 0 }
    val combop = reify {
      (x: Int, y: Int) => x + y
    }
    val seqop = reify {
      (x: Int, y: Int) =>
        if (predic.splice(y)) x + 1 else x
    }
    val (seqlv, seqoper) = c.functionExpr2Local[(Int, Int) => Int](seqop)
    val (comblv, comboper) = c.functionExpr2Local[(Int, Int) => Int](combop)


    makeKernel_Impl[Int, Int, Int](c)(predicv, seqlv, comblv)(zero)(comboper)(A0_RETURN_ZERO(c), A1_SUM[Int](c)(seqoper), AN_SUM[Int](c)(seqoper))(ctx)(true)
  }

  def min[U >: Int: c.WeakTypeTag](c: Context)(ord: c.Expr[Ordering[U]],ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[Int] = {
    import c.universe._

    val (ordv, ordg) = c.functionExpr2Local[Ordering[U]](ord)
    val op = reify {
      (x: Int, y: Int) => if (ordg.splice.compare(x, y) <= 0) x else y
    }
    val zero = reify { Ranges.EMPTY_RESULT }
    val (lv, oper: Expr[(Int, Int) => Int]) = c.functionExpr2Local[(Int, Int) => Int](op)
    val combine = reify { (a: Any, b: Any) =>
      {
        if (a == zero.splice) b
        else if (b == zero.splice) a
        else oper.splice(a.asInstanceOf[Int], b.asInstanceOf[Int])
      }
    }

    makeKernel_Impl[Int, Any, Int](c)(lv,ordv)(zero)(combine)(A0_RETURN_ZERO(c), A1_SUM[Any](c)(combine), AN_SUM[Any](c)(combine))(ctx)(false)
  }

  def max[U >: Int: c.WeakTypeTag](c: Context)(ord: c.Expr[Ordering[U]],ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[Int] = {
    import c.universe._

    val nOrd = reify { ord.splice.reverse }
    min[U](c)(nOrd,ctx)
  }

}
