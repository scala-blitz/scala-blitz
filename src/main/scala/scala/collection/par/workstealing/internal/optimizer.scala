package scala.collection.par.workstealing.internal



import scala.reflect.macros._
import scala.collection._
import scala.reflect.macros.blackbox.Context



class Optimizer[C <: Context](val c: C) {
  import c.universe._

  /* utilities */

  val ApplyName = TermName("apply")
  val MapName = TermName("map")
  val FlatMapName = TermName("flatMap")
  val ForeachName = TermName("foreach")
  val uncheckedTpe = typeOf[scala.unchecked]

  object collections {
    def TypeSymbolSet(s: Type*) = s.map(_.typeSymbol).toSet
    val stdSeqs = TypeSymbolSet(typeOf[List[_]], typeOf[Array[_]], typeOf[Vector[_]], typeOf[mutable.ArrayBuffer[_]], typeOf[mutable.ListBuffer[_]], typeOf[Stream[_]])
    val stdSets = TypeSymbolSet()
    val stdMaps = TypeSymbolSet()
    val stdTraversables = stdSeqs ++ stdSets ++ stdMaps
    val pure = Map(
      MapName -> stdTraversables,
      FlatMapName -> stdTraversables
    )
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
      val localname = TermName(c.freshName(prefix))
      (c.Expr[Unit](ValDef(Modifiers(), localname, TypeTree(), f.tree)), c.Expr[F](Ident(localname)))
  }

  object FunctionLiteral {
    def unapply(tree: Tree): Option[(List[ValDef], Tree)] = tree match {
      case Function(params, body) =>
        Some(params, body)
      case Typed(Function(params, body), _) =>
        Some(params, body)
      case _ =>
        None
    }
  }

  object PureFunction {
    def isUnchecked(tpe: Type) = tpe match {
      case tpe: AnnotatedType => tpe.annotations.exists(_.tpe == uncheckedTpe)
      case _ => false
    }
    def unapply(tt: Tree): Option[Function] = tt match {
      case Typed(f @ Function(_, _), tt: TypeTree) => if (isUnchecked(tt.tpe)) Some(f) else unapply(f)
      case f @ Function(_, body) => if (isUnchecked(body.tpe)) Some(f) else None
      case _ => None
    }
  }

  /* inlining */

  object Inlining {

    class Optimization extends Transformer {
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
                  ValDef(Modifiers(), TermName("" + paramname + "$0"), TypeTree(), a)
              }
              val paramVals2 = params.zip(args).map {
                case (ValDef(_, paramname, _, _), a) =>
                  ValDef(Modifiers(), paramname, TypeTree(), Ident(TermName("" + paramname + "$0")))
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
    
  }

  def inlineAndReset[T](expr: c.Expr[T]): c.Expr[T] = {
    val inliner = new Inlining.Optimization
    val global = c.universe.asInstanceOf[scala.tools.nsc.Global]
    c.Expr[T](global.brutallyResetAttrs(inliner.transform(expr.tree).asInstanceOf[global.Tree]).asInstanceOf[c.Tree])
  }

  /* fusion */

  object Fusion {
  
    class OpApply(val OpName: TermName)(calleeConstraint: Tree => Boolean = _ => true) {
      def unapply(tree: Tree): Option[(Tree, Function, (Tree, Function) => Tree)] = tree match {
        case Apply(Apply(TypeApply(Select(prefix, OpName), targs), List(FunctionLiteral(params, body))), implicits) if calleeConstraint(prefix) =>
          Some(prefix, Function(params, body), (t, f) => Apply(TypeApply(Select(t, OpName), targs), List(f)))
        case Apply(Apply(Select(prefix, OpName), List(FunctionLiteral(params, body))), implicits) if calleeConstraint(prefix) =>
          Some(prefix, Function(params, body), (t, f) => Apply(Select(t, OpName), List(f)))
        case Apply(TypeApply(Select(prefix, OpName), targs), List(FunctionLiteral(params, body))) if calleeConstraint(prefix) =>
          Some(prefix, Function(params, body), (t, f) => Apply(TypeApply(Select(t, OpName), targs), List(f)))
        case Apply(Select(prefix, OpName), List(FunctionLiteral(params, body))) if calleeConstraint(prefix) =>
          Some(prefix, Function(params, body), (t, f) => Apply(Select(t, OpName), List(f)))
        case _ =>
          None
      }
    }

    object BlockEndingWith {
      def unapply(tree: Tree): Option[(Tree, Tree => Tree)] = {
        def unapply(f: Tree => Tree, t: Tree): Option[(Tree, Tree => Tree)] = t match {
          case Block(stats, result) =>
            unapply(t => f(Block(stats, t)), result)
          case _ =>
            Some(t, f)
        }

        unapply(t => t, tree)
      }
    }

    trait Rule {
      def unapply(tree: Tree): Option[Tree]
    }
  
    object Rule {
      object Empty extends Rule {
        def unapply(tree: Tree) = None
      }
  
      class Composite(val patterns: Rule*) extends Rule {
        def unapply(tree: Tree) = patterns.foldLeft(None: Option[Tree]) {
          (acc, patt) => acc.orElse(patt.unapply(tree))
        }
      }
  
      abstract class SecondWins extends Rule {
        def PrimaryOpName: TermName

        def SecondaryOpName: TermName

        def fuse(callee: Tree, pf: Function, sf: Function): Function

        val PrimaryOpApply = new OpApply(PrimaryOpName)({ callee =>
          val sym = callee.tpe.widen.typeSymbol
          collections.pure(PrimaryOpName) contains sym
        })

        val SecondaryOpApply = new OpApply(SecondaryOpName)()

        def unapply(tree: Tree): Option[Tree] = {
          tree match {
            case SecondaryOpApply(BlockEndingWith(PrimaryOpApply(callee, pf, _), makeBlock), sf, makeOp) =>
              val f = fuse(callee, pf, sf)
              Some(makeBlock(makeOp(callee, f)))
            case _ =>
              None
          }
        }
      }

      object MapForeach extends SecondWins {
        def PrimaryOpName = MapName
        def SecondaryOpName = ForeachName
        def fuse(callee: Tree, pf: Function, sf: Function) = (pf, sf) match {
          case (Function(List(pparam), pbody), Function(List(sparam), sbody)) =>
            val localName = TermName(c.freshName("primaryres$"))
            Function(List(pparam), Block(
              List(ValDef(Modifiers(), localName, TypeTree(), pbody)),
              Apply(sf, List(Ident(localName)))
            ))
        }
      }

      object FlatMapForeach extends SecondWins {
        def PrimaryOpName = FlatMapName
        def SecondaryOpName = ForeachName
        def fuse(callee: Tree, pf: Function, sf: Function) = (pf, sf) match {
          case (Function(List(pparam), pbody), Function(List(sparam), sbody)) =>
            val primaryResName = TermName(c.freshName("primaryres$"))
            val nestedResName = TermName(c.freshName("nestedres$"))
            Function(List(pparam), Apply(
              Select(pbody, ForeachName),
              List(Function(
                List(ValDef(Modifiers(), nestedResName, TypeTree(), EmptyTree)),
                Apply(sf, List(Ident(nestedResName)))
              ))
            ))
        }
      }
    }

    class Optimization(val rules: Fusion.Rule) extends Transformer {
      override def transform(tree: Tree): Tree = {
        tree match {
          case rules(fused) =>
            transform(fused)
          case _ =>
            super.transform(tree)
        }
      }
    }
  
  }

  /* optimizations */

  /** Optimizes the following patterns:
   *  - function literal application
   *  - map/foreach fusion
   */
  def optimise[T](expr: c.Expr[T]): c.Expr[T] = {
    def inlineFunctionApply(tree: Tree): Tree = {
      val inliner = new Inlining.Optimization
      inliner.transform(tree)
    }
    def flatMapFusion(tree: Tree): Tree = {
      val fusion = new Fusion.Optimization(new Fusion.Rule.Composite(
        Fusion.Rule.MapForeach,
        Fusion.Rule.FlatMapForeach
      ))
      fusion.transform(tree)
    }

    val tree = expr.tree
    val inltree = inlineFunctionApply(tree)
    val fustree = flatMapFusion(inltree)
    val opttree = fustree
    val global = c.universe.asInstanceOf[scala.tools.nsc.Global]
    c.Expr[T](global.brutallyResetAttrs(opttree.asInstanceOf[global.Tree]).asInstanceOf[c.Tree])
  }

}


object Optimizer {
  implicit def c2opt(c: Context) = new Optimizer[c.type](c)
}


