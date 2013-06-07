package scala.collection.parallel
package workstealing



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.collection.parallel.generic._



object Reducables {

  trait Scope {
    implicit def reducableOps[T](r: Reducable[T]) = new collection.parallel.workstealing.Reducables.Ops[T](r)

    implicit def canMergeReducable[T]: CanMergeFrom[Reducable[_], Int, Reducable[T]] = ???
  }

  trait OpsLike[+T, +Repr] extends Any /*with ReducableOps[T, Repr, WorkstealingTreeScheduler]*/ {
    def reduce[U >: T](op: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler): U = macro Reducables.reduce[T, U]
    def fold[U >: T](z: =>U)(op: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler): U = macro Reducables.fold[T, U]
    def map[S, That](f: T => S)(implicit cmf: CanMergeFrom[Repr, S, That], ctx: WorkstealingTreeScheduler): That = ???
  }

  class Ops[T](val r: Reducable[T]) extends AnyVal with OpsLike[T, Reducable[T]]

  def reduce[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(op: c.Expr[(U, U) => U])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
    import c.universe._

    reify {
      ???
    }
  }

  def fold[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(z: c.Expr[U])(op: c.Expr[(U, U) => U])(ctx: c.Expr[WorkstealingTreeScheduler]): c.Expr[U] = {
    import c.universe._

    reify {
      ???
    }
  }

}


