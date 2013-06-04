package scala.collection.parallel.workstealing.methods



import scala.reflect.macros._
import scala.collection._



class Optimizer[C <: Context](val c: C) {
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
      object InlineablePattern {
        def unapply(tree: Tree): Option[(Tree, List[Tree])] = tree match {
          case Apply(Select(prefix, ApplyName), args) =>
            Some((prefix, args))
          case Apply(prefix @ Function(_, _), args) =>
            Some((prefix, args))
          case _ =>
            None
        }
      }

      override def transform(tree: Tree): Tree = {
        tree match {
          case InlineablePattern(prefix, args) =>
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
                tree
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

  def hasPureMapOp(prefix: Tree): Boolean = {
    val widenedType = prefix.tpe.widen
    val sym = widenedType.typeSymbol
    collections.pureMap contains sym
  }

  class Fusion(PrimaryOpName: TermName, SecondaryOpName: TermName, primaryOpPure: Tree => Boolean) extends Transformer {
    object FusablePrimaryBlock {
      def unapply(tree: Tree): Option[(Tree, Function, Tree => Tree)] = {
        def unapply(f: Tree => Tree, t: Tree): Option[(Tree, Function, Tree => Tree)] = t match {
          case Apply(Apply(TypeApply(Select(prefix, PrimaryOpName), _), List(PureFunction(primaryFunc))), implicits) if primaryOpPure(prefix) =>
            Some((prefix, primaryFunc, f))
          case Apply(Apply(Select(prefix, PrimaryOpName), List(PureFunction(primaryFunc))), implicits) if primaryOpPure(prefix) =>
            Some((prefix, primaryFunc, f))
          case Block(stats, result) =>
            unapply(t => f(Block(stats, t)), result)
          case _ =>
            None
        }
        unapply(t => t, tree)
      }
      object PureFunction {
        def isUnchecked(tpe: Type) = tpe match {
          case AnnotatedType(annots, _, _) => annots.exists(_.tpe == uncheckedTpe)
          case _ => false
        }
        def unapply(tt: Tree): Option[Function] = tt match {
          case Typed(f @ Function(_, _), tt: TypeTree) => if (isUnchecked(tt.tpe)) Some(f) else unapply(f)
          case f @ Function(_, body) => if (isUnchecked(body.tpe)) Some(f) else None
          case _ => None
        }
      }
    }
  
    object FusablePattern {
      def unapply(tree: Tree): Option[(Tree, Function, Function, Tree => Tree, (Tree, Function) => Tree)] = tree match {
        case Apply(TypeApply(Select(FusablePrimaryBlock(callee, primFunc, makeBlock), SecondaryOpName), targs), List(secondFunc @ Function(params, body))) =>
          Some((callee, primFunc, secondFunc, makeBlock, (t, f) => Apply(TypeApply(Select(t, SecondaryOpName), targs), List(f))))
        case Apply(Select(FusablePrimaryBlock(callee, primFunc, makeBlock), SecondaryOpName), List(secondFunc @ Function(params, body))) =>
          Some((callee, primFunc, secondFunc, makeBlock, (t, f) => Apply(Select(t, SecondaryOpName), List(f))))
        case _ =>
          None
      }
    }

    override def transform(tree: Tree): Tree = {
      def fuse(callee: Tree, pf: Function, sf: Function, makeBlock: Tree => Tree, makeSecondary: (Tree, Function) => Tree): Tree = {
        val localName = newTermName(c.fresh("primaryfunres$"))
        val fusedFunc = (pf, sf) match {
          case (Function(List(pparam), pbody), Function(List(sparam), sbody)) =>
            Function(List(pparam), Block(
              List(
                ValDef(Modifiers(), localName, TypeTree(), pbody)
              ),
              Apply(sf, List(Ident(localName)))
            ))
        }
        val call = makeSecondary(callee, fusedFunc)
        val block = makeBlock(call)
        block
      }

      tree match {
        case FusablePattern(callee, pf, sf, makeBlock, makeSecondary) =>
          val ntree = fuse(callee, pf, sf, makeBlock, makeSecondary)
          super.transform(ntree)
        case _ =>
          super.transform(tree)
      }
    }
  }

  /** Optimizes occurrences of a map followed directly by a foreach into just foreach loops.
   */
  def mapForeachFusion(tree: Tree): Tree = {
    val fusion = new Fusion(MapName, ForeachName, hasPureMapOp)
    fusion.transform(tree)
  }

  /** Optimizes the following patterns:
   *  - function literal application
   *  - map/foreach fusion
   */
  def optimise[T](expr: c.Expr[T]): c.Expr[T] = {
    val tree = expr.tree
    val inltree = inlineFunctionApply(tree)
    val fustree = mapForeachFusion(inltree)
    val opttree = fustree
    c.Expr[T](c resetAllAttrs opttree)
  }

}


object Optimizer {
  implicit def c2opt(c: Context) = new Optimizer[c.type](c)
}


