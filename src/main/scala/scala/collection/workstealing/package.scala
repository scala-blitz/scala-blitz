package scala.collection



import sun.misc.Unsafe
import scala.language.experimental.macros
import scala.reflect.macros.Context



package object workstealing {

  implicit def Util(context: Context) = new Util[context.type](context)

  class Util[C <: Context](val c: C) {
    import c.universe._

    def inlineAndReset[T](expr: c.Expr[T]): c.Expr[T] =
      c.Expr[T](c resetAllAttrs inlineApplyRecursive(expr.tree))

    def inlineApplyRecursive(tree: Tree): Tree = {
      val ApplyName = newTermName("apply")

      object inliner extends Transformer {
        override def transform(tree: Tree): Tree = {
          tree match {
            case ap @ Apply(Select(prefix, ApplyName), args) =>
              def function2block(params: List[ValDef], body: Tree): Block = {
                if (params.length != args.length)
                  c.abort(c.enclosingPosition, "incorrect arity: " + (params.length, args.length))
                val paramVals = params.zip(args).map {
                  case (ValDef(_, paramname, _, _), a) =>
                    ValDef(Modifiers(), newTermName("" + paramname + "$0"), TypeTree(), a)
                }
                val paramVals2 = params.zip(args).map {
                  case (ValDef(_, paramname, _, _), a) =>
                    ValDef(Modifiers(), paramname, TypeTree(), Ident(newTermName("" + paramname + "$0")))
                }
                Block(paramVals, Block(paramVals2, body))
              }

              def nestedFunction2Block(t: Tree): Tree = t match {
                case Function(params, body) =>
                  function2block(params, body)
                case Block(stats, t2) =>
                  Block(stats, nestedFunction2Block(t2))
                case x =>
                  ap
              }
              nestedFunction2Block(prefix)
            case _ =>
              super.transform(tree)
          }
        }
      }

      inliner.transform(tree)
    }

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
    }

    /** Used to generate a local val for a function expression,
     *  so that the function value is created only once.
     * 
     *  If `f` is a function literal, returns the literal.
     *  Otherwise, stores the function literal to a local value
     *  and returns a pair of the local value definition and an ident tree.
     */
    def functionExpr2Local[F](f: c.Expr[F]) = f.tree match {
      case Function(_, _) =>
        (c.Expr[Unit](EmptyTree), c.Expr[F](f.tree))
      case _ =>
        val localname = newTermName("localf$0")
        (c.Expr[Unit](ValDef(Modifiers(), localname, TypeTree(), f.tree)), c.Expr[F](Ident(localname)))
    }

  }

}


package workstealing {

  object Utils {

    val unsafe = getUnsafe()
  
    def getUnsafe(): Unsafe = {
      if (this.getClass.getClassLoader == null) Unsafe.getUnsafe()
      try {
        val fld = classOf[Unsafe].getDeclaredField("theUnsafe")
        fld.setAccessible(true)
        return fld.get(this.getClass).asInstanceOf[Unsafe]
      } catch {
        case e: Throwable => throw new RuntimeException("Could not obtain access to sun.misc.Unsafe", e)
      }
    }

  }

}

