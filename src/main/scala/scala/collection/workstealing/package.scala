package scala.collection



import sun.misc.Unsafe
import scala.language.experimental.macros
import scala.reflect.macros._



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

              prefix match {
                case Function(params, body) =>
                  function2block(params, body)
                case Block(stats, Function(params, body)) =>
                  // TODO generalize for arbitrary number of nested blocks
                  function2block(params, body)
                case x =>
                  ap
              }
            case _ =>
              super.transform(tree)
          }
        }
      }

      inliner.transform(tree)
    }

    def applicationPrefix = c.macroApplication match {
      case Apply(TypeApply(Select(prefix, name), targs), args) =>
        prefix
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

