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

import scala.collection.par.workstealing.internal.Optimizer


object ListMacros {

  class Optimizer[C <: Context](val c: C) {

    import c.universe._

    /** Returns the selection prefix of the current macro application.
      */
    def applyPrefix = c.macroApplication match {
      case Apply(TypeApply(Select(prefix, name), targs), args) =>
        prefix
      case Apply(Apply(TypeApply(Select(prefix, name), targs), args1), args2) =>
        prefix
      case Apply(Apply(Apply(TypeApply(Select(prefix, name), targs), args1), args2), args3) =>
        prefix
      case Apply(Select(prefix, name), args) =>
        prefix
      case _ =>
        c.prefix.tree
    }

    /** Used to generate a local val for a function expression,
      * so that the function value is created only once.
      *
      * If `f` is a function literal, returns the literal.
      * Otherwise, stores the function literal to a local value
      * and returns a pair of the local value definition and an ident tree.
      */
    def nonFunctionToLocal[F](f: c.Expr[F], prefix: String = "local"): (c.Expr[Unit], c.Expr[F]) = f.tree match {
      case Function(_, _) =>
        (c.Expr[Unit](EmptyTree), c.Expr[F](f.tree))
      case _ =>
        val localname = TermName(c.freshName(prefix))
        (c.Expr[Unit](ValDef(Modifiers(), localname, TypeTree(), f.tree)), c.Expr[F](Ident(localname)))
    }
  }


  implicit def c2opt(c: Context) = new Optimizer[c.type](c)


  def nonFunctionToLocal[F](c: Context)(f: c.Expr[F], prefix: String = "local"): (c.Expr[Unit], c.Expr[F]) = {

    import c.universe._

    f.tree match {
      case Function(_, _) =>
        (c.Expr[Unit](EmptyTree), c.Expr[F](f.tree))
      case _ =>
        val localname = TermName(c.freshName(prefix))
        (c.Expr[Unit](ValDef(Modifiers(), localname, TypeTree(), f.tree)), c.Expr[F](Ident(localname)))
    }
  }


  def aggregate[T: c.WeakTypeTag, S: c.WeakTypeTag](c: Context)(z: c.Expr[S])(combop: c.Expr[(S, S) => S])(seqop: c.Expr[(S, T) => S]): c.Expr[S] = {

    import c.universe._

    val (calleeExpressionv, calleeExpressiong) = c.nonFunctionToLocal[Lists.Ops[T]](c.Expr[Lists.Ops[T]](c.applyPrefix))
    val t =
      q"""$calleeExpressionv
      var zero$$ = $z
      val seqop$$ = $seqop
      val comboop$$ = $combop
      for(el <- $calleeExpressiong.list.seq) zero$$ = seqop$$(zero$$, el)
      zero$$
      """
    println(t)
    c.Expr[S](c.untypecheck(t))
  }

}
