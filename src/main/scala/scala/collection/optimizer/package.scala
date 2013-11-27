package scala.collection



import scala.language.experimental.macros
import scala.reflect.macros._



package object optimizer {
  final val debug = false

  def optimize[T](exp: T) = macro optimize_impl[T]

  def optimize_impl[T: c.WeakTypeTag](c: WhiteboxContext)(exp: c.Expr[T]) = {
    import c.universe._
    import Flag._

    def addImports(tree: Tree) = {
      q"""
         import scala.collection.par._
         import scala.reflect.ClassTag
         implicit val dummy$$0 = Scheduler.Implicits.sequential

         $tree"""
    }

    object CastingTransformer extends Transformer {
      override def transform(tree: Tree): Tree = {

        def unPar(tree: Tree) = {
          import scala.collection.par.Par
          val treeWithImports = addImports(tree)
          if(debug) println("typechecking " + treeWithImports)
          val typeChecked = c.typeCheck(treeWithImports)
          val result = {// we need to temporary wrap arrays to help typecheker
            if (typeChecked.tpe == typeOf[scala.collection.par.Par[Array[Int]]])
              q"new scala.collection.mutable.WrappedArray.ofInt($tree.seq)"
            else if (typeChecked.tpe == typeOf[Par[Array[Double]]]) 
              q"new scala.collection.mutable.WrappedArray.ofDouble($tree.seq)"
            else if (typeChecked.tpe == typeOf[Par[Array[Long]]])  
              q"new scala.collection.mutable.WrappedArray.ofLong($tree.seq)"
            else if (typeChecked.tpe == typeOf[Par[Array[Float]]]) 
              q"new scala.collection.mutable.WrappedArray.ofFloat($tree.seq)"
            else if (typeChecked.tpe == typeOf[Par[Array[Char]]]) 
              q"new scala.collection.mutable.WrappedArray.ofChar($tree.seq)"
            else if (typeChecked.tpe == typeOf[Par[Array[Byte]]])
              q"new scala.collection.mutable.WrappedArray.ofByte($tree.seq)"
            else if (typeChecked.tpe == typeOf[Par[Array[Short]]])
              q"new scala.collection.mutable.WrappedArray.ofShort($tree.seq)"
            else if (typeChecked.tpe == typeOf[Par[Array[Boolean]]])
              q"new scala.collection.mutable.WrappedArray.ofBoolean($tree.seq)"
            else if (typeChecked.tpe == typeOf[Par[Array[Unit]]])
              q"new scala.collection.mutable.WrappedArray.ofUnit($tree.seq)"
            else if (typeChecked.tpe == typeOf[Par[Array[AnyRef]]])
              q"new scala.collection.mutable.WrappedArray.ofRef($tree.seq)"
            else if (typeChecked.tpe == typeOf[Par[_]])  q"$tree.seq"
            else tree
          }
          result 
        }

        def unWrapArray(tree: Tree) = {
          val treeWithImports = addImports(tree)
          if (debug) println("unwrapping " + treeWithImports)
          val typeChecked = c.typeCheck(treeWithImports)
          if (debug) println("got type " + typeChecked.tpe)
          val result = {
            if (typeChecked.tpe.baseClasses.contains(typeOf[scala.collection.mutable.WrappedArray[_]].typeSymbol)) {
                if (debug) println("*********************unwrapping to array!")
                val r = q"$tree.array"
                r.setType(c.typeCheck(q"$typeChecked.array").tpe)
                r
              }
              else {
                if (typeChecked.tpe.baseClasses.contains(typeOf[scala.collection.mutable.ArrayOps[_]].typeSymbol)) {
                  if (debug) println("*********************unwrapping to array!")
                  val r = q"$tree.repr"
                  r.setType(c.typeCheck(q"$typeChecked.repr").tpe)
                  r
                } else tree
              }
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
          typeOf[Array[_]].typeSymbol
        )

        // TODO: fix broken newTermName("groupBy") 
        val methodWhiteList:Set[Name] = Set(TermName("foreach"), TermName("map"), TermName("reduce"), TermName("exists"), TermName("filter"), TermName("find"), TermName("flatMap"), TermName("forall"), TermName("count"), TermName("fold"), TermName("sum"), TermName("product"), TermName("min"), TermName("max"))
        val maintain2ArgumentList: Set[Name] = Set(TermName("fold"))
        val append2ArgumentList: Set[Name] = Set(TermName("sum"), TermName("product"), TermName("min"), TermName("max"))

        tree match {
          case q"$seqOriginal.aggregate[$tag]($zeroOriginal)($seqopOriginal, $comboopOriginal)" =>
            val seq = unWrapArray(transform(seqOriginal))
            val whiteListed = (seq.tpe ne null) && typesWhiteList.contains(seq.tpe.typeSymbol)
            if (debug) println(s"\n\nAGGREGATEtransforming ${tree}\n seq type is ${seq.tpe}\n type is in whiteList: $whiteListed")
            val rewrite: Tree = 
              if (whiteListed) {
                val seqop = transform(seqopOriginal)
                val comboop = transform(comboopOriginal)
                val zero = transform(zeroOriginal)
                unPar(q"$seq.toPar.aggregate($zero)($seqop)($comboop)")
              } else super.transform(tree)
            if (whiteListed) if (debug) println("quasiquote formed " + rewrite + "\n")
            rewrite
          case q"$seqOriginal.$method[..$targs]($argsOriginal)($cbf)" => //map
            val seq = unWrapArray(transform(seqOriginal))
            val whiteListed = (seq.tpe ne null) && typesWhiteList.contains(seq.tpe.typeSymbol)
            val methodListed = methodWhiteList.contains(method)
            if (debug) println(s"\n\nMAPtransforming ${tree}\n seq type is ${seq.tpe}\n type is in whiteList: $whiteListed\nmethod is ${method}\nmethod is in whiteList: $methodListed")
            val rewrite: Tree = 
              if (whiteListed && methodListed) {
                val args = transform(argsOriginal)
                if (maintain2ArgumentList.contains(method))
                  unPar(q"$seq.toPar.$method($args)($cbf)")
                else unPar(q"$seq.toPar.$method($args)")
              } else super.transform(tree)
            if (whiteListed) if (debug) println(s"quasiquote formed ${rewrite}")
            rewrite
          case q"$seqOriginal.$method[..$targs]($argsOriginal)" => //foreach, reduce, groupBy(broken?), exists, filter
            val seq = unWrapArray(transform(seqOriginal))
            val whiteListed = (seq.tpe ne null) && typesWhiteList.contains(seq.tpe.typeSymbol)
            val methodListed = methodWhiteList.contains(method)
            if (debug) println(s"\n\nFOREACHtransforming ${tree}\n seq type is ${seq.tpe}\n type is in whiteList: $whiteListed\nmethod is ${method}\nmethod is in whiteList: $methodListed")
            val rewrite: Tree = if (whiteListed && methodListed) {
              val args = transform(argsOriginal)
              if (append2ArgumentList.contains(method))
                  unPar(q"$seq.toPar.$method($args, dummy$$0)")
              else unPar(q"$seq.toPar.$method($args)") // q"$seq.foreach[..$targs]($args)"
            } else super.transform(tree)
            if (whiteListed && methodListed) if (debug) println(s"quasiquote formed ${rewrite}")
            rewrite
          case _ =>
            if (debug) println(s"not transforming $tree") 
            super.transform(tree)
        }
      }
    }

    val t = CastingTransformer.transform(exp.tree)
    
    val resultWithImports = addImports(t)
     if (debug) println(s"\n\n\nresult: $resultWithImports")

    (c.resetAllAttrs(resultWithImports))
  }
}
