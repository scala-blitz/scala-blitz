package scala.collection



import scala.language.experimental.macros
import scala.reflect.macros._




package object optimizer {
  final val debugOutput = false
  final def debug(s: String) = if (debugOutput) println(s)
  final val echoSplicedCode = false

  def optimize[T](exp: T): Any = macro optimize_impl[T]

  /** Eliminates consecutive Box&Unbox pairs */
  def optimize_postprocess[T](exp: T) = macro optimize_postprocess_impl[T]

  def optimize_postprocess_impl[T: c.WeakTypeTag](c: Context)(exp: c.Expr[T]) = {
    import c.universe._
    import Flag._

    object BoxUnboxEliminating extends Transformer {
      override def transform(tree: Tree): Tree = {
        val res = tree match {
          case q"$xs.seq.toPar" => q"$xs"
          case q"$xs.toPar.seq" => q"$xs"
          case q"scala.this.Predef.intArrayOps($arr).repr" => q"$arr"
          case q"scala.this.Predef.longArrayOps($arr).repr" => q"$arr"
          case q"scala.this.Predef.refArrayOps($arr).repr" => q"$arr"
          case q"scala.this.Predef.shortArrayOps($arr).repr" => q"$arr"
          case q"scala.this.Predef.unitArrayOps($arr).repr" => q"$arr"
          case q"scala.this.Predef.booleanArrayOps($arr).repr" => q"$arr"
          case q"scala.this.Predef.byteArrayOps($arr).repr" => q"$arr"
          case q"scala.this.Predef.charArrayOps($arr).repr" => q"$arr"
          case q"scala.this.Predef.doubleArrayOps($arr).repr" => q"$arr"
          case q"scala.this.Predef.floatArrayOps($arr).repr" => q"$arr"
          case q"scala.this.Predef.genericArrayOps[$tags]($arr).repr" => q"$arr"

          case q"scala.Predef.intArrayOps($arr).repr" => q"$arr"
          case q"scala.Predef.longArrayOps($arr).repr" => q"$arr"
          case q"scala.Predef.refArrayOps($arr).repr" => q"$arr"
          case q"scala.Predef.shortArrayOps($arr).repr" => q"$arr"
          case q"scala.Predef.unitArrayOps($arr).repr" => q"$arr"
          case q"scala.Predef.booleanArrayOps($arr).repr" => q"$arr"
          case q"scala.Predef.byteArrayOps($arr).repr" => q"$arr"
          case q"scala.Predef.charArrayOps($arr).repr" => q"$arr"
          case q"scala.Predef.doubleArrayOps($arr).repr" => q"$arr"
          case q"scala.Predef.floatArrayOps($arr).repr" => q"$arr"
          case q"scala.Predef.genericArrayOps[$tags]($arr).repr" => q"$arr"

          case _ => tree
        }

        super.transform(res)
      }
    }

    val t = BoxUnboxEliminating.transform(exp.tree)
    if (echoSplicedCode) println(t)
    c.Expr(c.resetAllAttrs(t))
  }
  

  def optimize_impl[T: c.WeakTypeTag](c: Context)(exp: c.Expr[T]) = {
    import c.universe._
    import Flag._

    def addImports(tree: Tree) = {
      q"""
         import scala.collection.par._
         import scala.reflect.ClassTag
         import scala.math.Ordering
         implicit val dummy$$0 = Scheduler.Implicits.sequential

         $tree"""
    }
    var allTypesFound = true // if we haven't found type somewhere, it means that we've spliced informative changes
    var progress = false

    object CastingTransformer extends Transformer {
      override def transform(tree: Tree): Tree = {

        /** gives seq from Par[Seq], consecutive .toPar.seq if created will be removed during post-processing */
        def unPar(tree: Tree) = {
          import scala.collection.par.Par
          val result = {
            if ((tree.tpe ne null) && (tree.tpe.typeSymbol == typeOf[Par[_]].typeSymbol))  q"$tree.seq"
            else tree
          }
          result 
        }

        /** gives from WrappedArray or ArrayOps an Array */
        def unWrapArray(tree: Tree) = {
          val resultWrapped = {
            if ((tree.tpe ne null) && tree.tpe.baseClasses.contains(typeOf[collection.mutable.WrappedArray[_]].typeSymbol)) {
              debug(s"*********************unwrapping to array!\n$tree")
              q"$tree.array"
              } else {
              if ((tree.tpe ne null) && tree.tpe.baseClasses.contains(typeOf[collection.mutable.ArrayOps[_]].typeSymbol)) {
                debug(s"*********************unwrapping to array!\n$tree")
                q"$tree.repr"
              } else tree
            }
          }

          // check for wrap&unwrap
          val result = resultWrapped match {
            // for some reason those aren't equal mathes, and comiler adds with'this' 
            case q"scala.this.Predef.intArrayOps($arr).repr" => q"$arr"
            case q"scala.this.Predef.longArrayOps($arr).repr" => q"$arr"
            case q"scala.this.Predef.refArrayOps($arr).repr" => q"$arr"
            case q"scala.this.Predef.shortArrayOps($arr).repr" => q"$arr"
            case q"scala.this.Predef.unitArrayOps($arr).repr" => q"$arr"
            case q"scala.this.Predef.booleanArrayOps($arr).repr" => q"$arr"
            case q"scala.this.Predef.byteArrayOps($arr).repr" => q"$arr"
            case q"scala.this.Predef.charArrayOps($arr).repr" => q"$arr"
            case q"scala.this.Predef.doubleArrayOps($arr).repr" => q"$arr"
            case q"scala.this.Predef.floatArrayOps($arr).repr" => q"$arr"
            case q"scala.this.Predef.genericArrayOps[$tags]($arr).repr" => q"$arr"

            case q"scala.Predef.intArrayOps($arr).repr" => q"$arr"
            case q"scala.Predef.longArrayOps($arr).repr" => q"$arr"
            case q"scala.Predef.refArrayOps($arr).repr" => q"$arr"
            case q"scala.Predef.shortArrayOps($arr).repr" => q"$arr"
            case q"scala.Predef.unitArrayOps($arr).repr" => q"$arr"
            case q"scala.Predef.booleanArrayOps($arr).repr" => q"$arr"
            case q"scala.Predef.byteArrayOps($arr).repr" => q"$arr"
            case q"scala.Predef.charArrayOps($arr).repr" => q"$arr"
            case q"scala.Predef.doubleArrayOps($arr).repr" => q"$arr"
            case q"scala.Predef.floatArrayOps($arr).repr" => q"$arr"
            case q"scala.Predef.genericArrayOps[$tags]($arr).repr" => q"$arr"
            case _ => resultWrapped
          }
          result 
        }
       
        val typesWhiteList = Set(typeOf[List[_]].typeSymbol,
          typeOf[Range].typeSymbol,
          typeOf[scala.collection.immutable.Range.Inclusive].typeSymbol,
          typeOf[scala.collection.immutable.HashMap[_, _]].typeSymbol,
          typeOf[scala.collection.immutable.HashSet[_]].typeSymbol,
          typeOf[scala.collection.mutable.HashMap[_, _]].typeSymbol,
          typeOf[scala.collection.mutable.HashSet[_]].typeSymbol,
          typeOf[scala.collection.mutable.WrappedArray[_]].typeSymbol,
          typeOf[scala.collection.mutable.ArrayOps[_]].typeSymbol,
          typeOf[Array[_]].typeSymbol
        )

        // TODO: fix broken newTermName("groupBy") 
        val methodWhiteList: Set[Name] = Set(newTermName("foreach"), newTermName("map"), newTermName("reduce"), newTermName("exists"), newTermName("filter"), newTermName("find"), newTermName("flatMap"), newTermName("forall"), newTermName("count"), newTermName("fold"), newTermName("sum"), newTermName("product"), newTermName("min"), newTermName("max"))
        // methods that result in Par[collection] and thus require .seq invocation
        val collectionMethods: Set[Name] = Set(newTermName("map"), newTermName("filter"), newTermName("flatMap"))
        // methods for which second argument list should be maintained
        val maintain2ArgumentList: Set[Name] = Set(newTermName("fold"))
        // methods that already have an implicit argument list. we reuse it and add scheduler to it
        val append2ArgumentList: Set[Name] = Set(newTermName("sum"), newTermName("product"), newTermName("min"), newTermName("max"))

        tree match {
          case q"$seqOriginal.aggregate[$tag]($zeroOriginal)($seqopOriginal, $comboopOriginal)" =>
            val seqTransformed = transform(seqOriginal)
            val whiteListed = (seqTransformed.tpe ne null) && typesWhiteList.contains(seqTransformed.tpe.typeSymbol)

            if (seqTransformed.tpe eq null) {
              allTypesFound = false
              debug(s"\n\ntype not found for seqTransformed \nnext op:aggregate")
            }
            val rewrite: Tree =
              if (whiteListed) {
                val seq = unWrapArray(seqTransformed)
                debug(s"\n\nAGGREGATEtransforming ${tree}\n seq type is ${seq.tpe}\n type is in whiteList: $whiteListed")
                val seqop = transform(seqopOriginal)
                val comboop = transform(comboopOriginal)
                val zero = transform(zeroOriginal)
                progress = true
                q"$seq.toPar.aggregate($zero)($comboop)($seqop)"
              } else super.transform(tree)
            if (whiteListed) debug("quasiquote formed " + rewrite + "\n")
            rewrite
          case q"$seqOriginal.$method[..$targs]($argsOriginal)($cbf)" => //map, fold
            
            val seqTansformed = transform(seqOriginal)
            val whiteListed = (seqTansformed.tpe ne null) && typesWhiteList.contains(seqTansformed.tpe.typeSymbol)
            val methodListed = methodWhiteList.contains(method)
            if (methodListed && (seqTansformed.tpe eq null)) {
              allTypesFound = false
              debug(s"\n\ntype not found for $seqTansformed \nnext op:$method")
            }
            val rewrite: Tree = 
              if (whiteListed && methodListed) {
                val seq = unWrapArray(seqTansformed)
                debug(s"\n\nMAPtransforming ${tree}\n seq type is ${seq.tpe}\n type is in whiteList: $whiteListed\nmethod is ${method}\nmethod is in whiteList: $methodListed")
                val args = transform(argsOriginal)
                progress = true
                if (maintain2ArgumentList.contains(method))
                  q"$seq.toPar.$method($args)($cbf).seq"
                else q"$seq.toPar.$method($args).seq"
              }
              else if (methodListed && ! maintain2ArgumentList.contains(method))  //clean cbf, but not for fold
                {
                  val transformedSeq = transform(q"$seqOriginal")
                  val transformedArgs = transform(q"$argsOriginal")
                  if ((seqOriginal.toString != transformedSeq.toString) ||(transformedArgs.toString != argsOriginal.toString)) {
                    debug("**************************************Removed cbf")
                    debug("seq is $seqOriginal")
                    debug("transformedSeq is $transformedSeq")
                    q"$transformedSeq.$method($transformedArgs)"
                  }
                  else tree
                }
              else super.transform(tree)
            if (whiteListed && methodListed) debug(s"quasiquote formed ${rewrite}")
            rewrite
          case q"$seqOriginal.$method[..$targs]($argsOriginal)" => //foreach, reduce, groupBy(broken?), exists, filter
            val seqTransformed = transform(seqOriginal)
            val whiteListed = (seqTransformed.tpe ne null) && typesWhiteList.contains(seqTransformed.tpe.typeSymbol)
            val methodListed = methodWhiteList.contains(method)
            if (methodListed && (seqTransformed.tpe eq null)) {
              allTypesFound = false
              debug(s"\n\ntype not found for seqOriginal\n next op:$method")
            }
            val rewrite: Tree = if (whiteListed && methodListed) {
              val seq = unWrapArray(seqTransformed)
              debug(s"\n\nFOREACHtransforming ${tree}\n seq type is ${seq.tpe}\n type is in whiteList: $whiteListed\nmethod is ${method}\nmethod is in whiteList: $methodListed")
              val args = transform(argsOriginal)
              progress = true
              if (append2ArgumentList.contains(method)) //max, sum
                q"$seq.toPar.$method($args, dummy$$0)"
              else if (collectionMethods.contains(method)) // filter
                q"$seq.toPar.$method($args).seq"
              else q"$seq.toPar.$method($args)" // q"$seq.foreach[..$targs]($args)"
            } else super.transform(tree)
            if (whiteListed && methodListed) debug(s"quasiquote formed ${rewrite}")
            rewrite
          case _ =>
//            debug(s"not transforming $tree") 
            super.transform(tree)
        }
      }
    }

    val t = CastingTransformer.transform(exp.tree)

    val resultWithImports =     
      if (allTypesFound) q"optimize_postprocess{${addImports(t)}}"
      else addImports(q"optimize{$t}")
     debug(s"\n\n\n***********************************************************************************\nresult: $resultWithImports")

    c.Expr((c.resetAllAttrs(resultWithImports)))
  }
}
