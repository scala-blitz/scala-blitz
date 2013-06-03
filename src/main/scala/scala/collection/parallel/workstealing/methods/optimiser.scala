package scala.collection.parallel.workstealing.methods



import scala.reflect.macros._
import scala.collection._



class Optimiser[C <: Context](val c: C) {
  import c.universe._

  val MapName = newTermName("map")
  val ForeachName = newTermName("foreach")
  val uncheckedTpe = typeOf[scala.unchecked]

  object collections {
    def TypeSymbolSet(s: Type*) = s.map(_.typeSymbol).toSet
    val pureMap = TypeSymbolSet(
      typeOf[List[_]], typeOf[Array[_]], typeOf[Vector[_]], typeOf[mutable.ArrayBuffer[_]], typeOf[mutable.ListBuffer[_]], typeOf[Stream[_]]
    )
  }

  def inlineAndReset[T](expr: c.Expr[T]): c.Expr[T] =
    c.Expr[T](c resetAllAttrs inlineFunctionApply(expr.tree))

  def inlineFunctionApply(tree: Tree): Tree = {
    val ApplyName = newTermName("apply")

    object inliner extends Transformer {
      override def transform(tree: Tree): Tree = {
        tree match {
          case ap @ Apply(Select(prefix, ApplyName), args) =>
            def inlineToLocals(params: List[ValDef], body: Tree): Block = {
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
            def functionToBlock(t: Tree): Tree = t match {
              case Function(params, body) =>
                inlineToLocals(params, body)
              case Block(stats, t2) =>
                Block(stats, functionToBlock(t2))
              case x =>
                ap
            }
            super.transform(functionToBlock(prefix))
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
    case _ =>
      c.prefix.tree
  }

  /** Used to generate a local val for a function expression,
   *  so that the function value is created only once.
   * 
   *  If `f` is a function literal, returns the literal.
   *  Otherwise, stores the function literal to a local value
   *  and returns a pair of the local value definition and an ident tree.
   */
  def nonFunctionToLocal[F](f: c.Expr[F], prefix: String = "local"): (c.Expr[Unit], c.Expr[F]) = f.tree match {
    case Function(_, _) =>
      (c.Expr[Unit](EmptyTree), c.Expr[F](f.tree))
    case _ =>
      val localname = newTermName(c.fresh(prefix))
      (c.Expr[Unit](ValDef(Modifiers(), localname, TypeTree(), f.tree)), c.Expr[F](Ident(localname)))
  }

  object FusableMapBlock {
    def unapply(t: Tree): Option[Tree] = t match {
      case Apply(Apply(TypeApply(Select(prefix, MapName), _), List(PureFunction())), implicits) if hasPureMapOp(prefix) =>
        Some(t)
      case Apply(Apply(Select(prefix, MapName), List(PureFunction())), implicits) if hasPureMapOp(prefix) =>
        Some(t)
      case Block(stats, result) =>
        unapply(result)
      case _ =>
        None
    }
    def hasPureMapOp(prefix: Tree): Boolean = {
      val widenedType = prefix.tpe.widen
      val sym = widenedType.typeSymbol
      collections.pureMap contains sym
    }
    object PureFunction {
      def isUnchecked(tpe: Type) = tpe match {
        case AnnotatedType(annots, _, _) => annots.exists(_.tpe == uncheckedTpe)
        case _ => false
      }
      def unapply(tt: Tree): Boolean = tt match {
        case Typed(Function(_, _), tt: TypeTree) => isUnchecked(tt.tpe)
        case Function(_, body) => isUnchecked(body.tpe)
      }
    }
  }

  /** Optimizes occurrences of a map followed directly by a foreach into just foreach loops.
   */
  def mapForeachFusion(tree: Tree): Tree = {
    object fusion extends Transformer {
      override def transform(tree: Tree): Tree = {
        tree match {
          case Apply(TypeApply(Select(FusableMapBlock(m), ForeachName), _), List(Function(params, body))) =>
            super.transform(tree)
          case Apply(Select(FusableMapBlock(m), ForeachName), List(Function(params, body))) =>
            println(tree)
            super.transform(tree)
          case _ =>
            super.transform(tree)
        }
      }
    }

    fusion.transform(tree)
  }

  /** Optimizes the following patterns:
   *  - function literal application
   *  - map/foreach fusion
   */
  def optimise[T](expr: c.Expr[T]): c.Expr[T] = {
    val tree = expr.tree
    println(tree)
    val inltree = inlineFunctionApply(tree)
    val fustree = mapForeachFusion(inltree)
    val opttree = fustree
    c.Expr[T](c resetAllAttrs opttree)
  }

}


object Optimiser {
  implicit def c2opt(c: Context) = new Optimiser[c.type](c)
}


