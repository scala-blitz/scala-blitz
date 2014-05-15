package scala.collection.optimizer.internal

import scala.reflect.macros.blackbox.Context
import scala.collection.optimizer.Lists
import scala.language.experimental.macros
import scala.reflect.macros._
import scala.reflect.ClassTag
import scala.collection.par.workstealing._
import scala.collection.par.Scheduler
import scala.collection.par.Scheduler.Node
import scala.collection.par.generic._
import scala.collection.par.Par
import scala.collection.par.Merger
import scala.collection.optimizer.zero

import scala.collection.par.workstealing.internal.Optimizer.c2opt


object ListMacros {

  def mkAggregate[T: c.WeakTypeTag, S: c.WeakTypeTag](c: Context)(z: c.Expr[S])(combop: c.Expr[(S, S) => S])(seqop: c.Expr[(S, T) => S]): c.Tree = {

    import c.universe._

    val (calleeExpressionv, calleeExpressiong) = c.nonFunctionToLocal[Lists.Ops[T]](c.Expr[Lists.Ops[T]](c.applyPrefix))
    val t = // val comboop = $combop
      q"""
      $calleeExpressionv
      var zero = $z
      var list = $calleeExpressiong.list.seq
      while(list ne Nil) {
        zero = $seqop(zero, list.head)
        list = list.tail
      }
      zero
      """
    t
  }

  def reduce[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(combop: c.Expr[(U, U) => U]): c.Expr[T] = {

    import c.universe._

    val (calleeExpressionv, calleeExpressiong) = c.nonFunctionToLocal[Lists.Ops[T]](c.Expr[Lists.Ops[T]](c.applyPrefix))
    val (comboopv, comboopg) = c.nonFunctionToLocal[(U, U) => U](combop)
    val t =
      q"""
      $calleeExpressionv
      $comboopv
      if($calleeExpressiong.list.seq.isEmpty) throw new java.lang.UnsupportedOperationException("empty.reduce")
      var zero = $calleeExpressiong.list.seq.head
      var list = $calleeExpressiong.list.seq.tail
      while(list ne Nil) {
        zero = $comboopg(zero, list.head)
        list = list.tail
      }
      zero
      """
    c.Expr[T](c.untypecheck(t))
  }

  def aggregate[T: c.WeakTypeTag, S: c.WeakTypeTag](c: Context)(z: c.Expr[S])(combop: c.Expr[(S, S) => S])(seqop: c.Expr[(S, T) => S]): c.Expr[S] =
    c.Expr[S](c.untypecheck(mkAggregate(c)(z)(combop)(seqop)))

  def fold[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(z: c.Expr[U])(op: c.Expr[(U, U) => U]): c.Expr[U] = {
    aggregate(c)(z)(op)(op)
  }

  def sum[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(num: c.Expr[Numeric[U]]): c.Expr[U] = {
    import c.universe._
    val (numv, numg) = c.nonFunctionToLocal[Numeric[U]](num)
    val tu = implicitly[c.WeakTypeTag[U]]
    val op = c.Expr[(U, U)=>U](q"(x: $tu, y: $tu) => $numg.plus(x, y)")
    val zero = c.Expr[U](q"$numg.zero")
    val t = q"""
      $numv
      ${mkAggregate(c)(zero)(op)(op)}
      """
    c.Expr[U](c.untypecheck(t))
  }

  def product[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(num: c.Expr[Numeric[U]]): c.Expr[U] = {
    import c.universe._
    val (numv, numg) = c.nonFunctionToLocal[Numeric[U]](num)
    val tu = implicitly[c.WeakTypeTag[U]]
    val op = c.Expr[(U, U)=>U](q"(x: $tu, y: $tu) => $numg.times(x, y)")
    val zero = c.Expr[U](q"$numg.one")
    val t = q"""
      $numv
      ${mkAggregate(c)(zero)(op)(op)}
      """
    c.Expr[U](c.untypecheck(t))
  }


  def foreach[T: c.WeakTypeTag, U >: T : c.WeakTypeTag](c: Context)(action: c.Expr[U => Unit]): c.Expr[Unit] = {
    import c.universe._

    val (actionv, actiong) = c.nonFunctionToLocal[U => Unit](action)
    val fakeComboop = c.Expr[(Unit, Unit) => Unit](q"(x: Unit, a: Unit)=> x")
    val seqoper = c.Expr[(Unit, U) => Unit](q"(x: Unit, a: U)=> $actiong.apply(a)")
    val zero = c.Expr[Unit](q"()")
    val t = q"""
      $actionv
      ${mkAggregate(c)(zero)(fakeComboop)(seqoper)}
      """
    c.Expr[Unit](c.untypecheck(t))
  }

  def count[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(p: c.Expr[U => Boolean]): c.Expr[Int] = {
    import c.universe._

    val comboop = c.Expr[(Int, Int) => Int](q"(x: Int, a: Int)=> x + a")
    val (opv, opg) = c.nonFunctionToLocal[U => Boolean](p)
    val seqoper = c.Expr[(Int, U) => Int](q"(x: Int, a: U)=> if($opg(a)) x + 1 else x")
    val zero = c.Expr[Int](q"0")
    val t = q"""
      $opv
      ${mkAggregate(c)(zero)(comboop)(seqoper)}
      """
    c.Expr[Int](c.untypecheck(t))
  }

  def min[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(ord: c.Expr[Ordering[U]]): c.Expr[T] = {
    import c.universe._

    val (ordv, ordg) = c.nonFunctionToLocal[Ordering[U]](ord)
    val tu = implicitly[c.WeakTypeTag[U]]
    val op = c.Expr[(T, T) => T](q"(x: $tu, y: $tu) => if ($ordg.compare(x, y) < 0) x else y ")
    val red = reduce[T, T](c)(op)
    c.Expr[T](q"""
      $ordv
      $red
      """
    )
  }

  def max[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(ord: c.Expr[Ordering[U]]): c.Expr[T] = {
    import c.universe._
    val tu = implicitly[c.WeakTypeTag[U]]
    val (ordv, ordg) = c.nonFunctionToLocal[Ordering[U]](ord)
    val op = c.Expr[(T, T) => T](q"(x: $tu, y: $tu) => if ($ordg.compare(x, y) > 0) x else y ")
    val red = reduce[T, T](c)(op)
    c.Expr[T](q"""
      $ordv
      $red
      """
    )
  }
  def find[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(p: c.Expr[U => Boolean]): c.Expr[Option[T]] = {
    import c.universe._
    val tt = implicitly[c.WeakTypeTag[T]]
    val z = zero(c)(tt.tpe)
    val (pv, pg) = c.nonFunctionToLocal[U => Boolean](p)
    val op = c.Expr[(Boolean, T) => Boolean](q"(x: Boolean, y: $tt) => x || {if ($pg(y)) {fndVal = y; true} else false}")
    val fakeComboop = c.Expr[(Boolean, Boolean) => Boolean](q"(x: Boolean, y: Boolean)=> x || y")
    val search = mkAggregate[T, Boolean](c)(c.Expr[Boolean](q"false"))(fakeComboop)(op)
    c.Expr[Option[T]](q"""
      $pv
      var fndVal = $z
      val found = $search
      if(found) Some(fndVal) else None
      """
    )
  }

  def forall[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(p: c.Expr[U => Boolean]): c.Expr[Boolean] = {
    import c.universe._
    val (pv, pg) = c.nonFunctionToLocal[U => Boolean](p)
    val tt = implicitly[c.WeakTypeTag[T]]
    val op = c.Expr[(Boolean, T) => Boolean](q"(x: Boolean, y: $tt) => x || {if (!$pg(y)) {true} else false}")
    val fakeComboop = c.Expr[(Boolean, Boolean) => Boolean](q"(x: Boolean, y: Boolean)=> x || y")
    val search = mkAggregate[T, Boolean](c)(c.Expr[Boolean](q"false"))(fakeComboop)(op)
    c.Expr[Boolean](q"""
      $pv
      $search
      """
    )
  }

  def exists[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(p: c.Expr[U => Boolean]): c.Expr[Boolean] = {
    import c.universe._
    val (pv, pg) = c.nonFunctionToLocal[U => Boolean](p)
    val tt = implicitly[c.WeakTypeTag[T]]
    val op = c.Expr[(Boolean, T) => Boolean](q"(x: Boolean, y: $tt) => x || {if ($pg(y)) {true} else false}")
    val fakeComboop = c.Expr[(Boolean, Boolean) => Boolean](q"(x: Boolean, y: Boolean)=> x || y")
    val search = mkAggregate[T, Boolean](c)(c.Expr[Boolean](q"false"))(fakeComboop)(op)
    c.Expr[Boolean](q"""
      $pv
      $search
      """
    )
  }

  def map[T: c.WeakTypeTag, U >: T: c.WeakTypeTag, T2: c.WeakTypeTag](c: Context)(p: c.Expr[U => T2]): c.Expr[List[T2]] = {
    import c.universe._
    val tt = implicitly[c.WeakTypeTag[T2]]
    val tt2 = implicitly[c.WeakTypeTag[T2]]
    val z = c.Expr[scala.collection.mutable.ListBuffer[T2]](q"new scala.collection.mutable.ListBuffer[$tt2]()")
    val (pv, pg) = c.nonFunctionToLocal[U => T2](p)
    val op = c.Expr[(scala.collection.mutable.ListBuffer[T2], T) => scala.collection.mutable.ListBuffer[T2]](
      q"(x: scala.collection.mutable.ListBuffer[$tt2], y: $tt) => {x += $pg(y); x}"
    )
    val fakeComboop = c.Expr[(scala.collection.mutable.ListBuffer[T2], scala.collection.mutable.ListBuffer[T2]) => scala.collection.mutable.ListBuffer[T2]](
      q"(x: scala.collection.mutable.ListBuffer[$tt2], y:scala.collection.mutable.ListBuffer[$tt2])=> ???"
    )
    val mapper = mkAggregate[T, scala.collection.mutable.ListBuffer[T2]](c)(z)(fakeComboop)(op)
    c.Expr[List[T2]](q"""
      $pv
      $mapper.toList
      """
    )
  }

  def flatMap[T: c.WeakTypeTag, U >: T: c.WeakTypeTag, T2: c.WeakTypeTag](c: Context)(p: c.Expr[U => TraversableOnce[T2]]): c.Expr[List[T2]] = {
    import c.universe._
    val tt = implicitly[c.WeakTypeTag[T2]]
    val tt2 = implicitly[c.WeakTypeTag[T2]]
    val z = c.Expr[scala.collection.mutable.ListBuffer[T2]](q"new scala.collection.mutable.ListBuffer[$tt2]()")
    val (pv, pg) = c.nonFunctionToLocal[U => TraversableOnce[T2]](p)
    val op = c.Expr[(scala.collection.mutable.ListBuffer[T2], T) => scala.collection.mutable.ListBuffer[T2]](
      q"(x: scala.collection.mutable.ListBuffer[$tt2], y: $tt) => {x ++= $pg(y); x}"
    )
    val fakeComboop = c.Expr[(scala.collection.mutable.ListBuffer[T2], scala.collection.mutable.ListBuffer[T2]) => scala.collection.mutable.ListBuffer[T2]](
      q"(x: scala.collection.mutable.ListBuffer[$tt2], y:scala.collection.mutable.ListBuffer[$tt2])=> x"
    )
    val mapper = mkAggregate[T, scala.collection.mutable.ListBuffer[T2]](c)(z)(fakeComboop)(op)
    c.Expr[List[T2]](q"""
      $pv
      $mapper.toList
      """
    )
  }
}
