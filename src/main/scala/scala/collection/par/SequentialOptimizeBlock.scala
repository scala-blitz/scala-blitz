package scala.collection.par

import scala.language.experimental.macros
import scala.reflect.macros._

object SequentialOptimizeBlock {

  implicit def extender[T](t:T) = new {
    def toList2 = List(t)
  }


  def optimize[T](exp: T) = macro optimize_impl[T]

  def optimize_impl[T: c.WeakTypeTag](c: WhiteboxContext)(exp: c.Expr[T]) = {
    import c.universe._
    import Flag._
    object CastingTransformer extends Transformer {

    

      override def transform(tree: Tree): Tree = {

        def unPar(tree: Tree) = {
           println("typechecking " + tree)
          val typeChecked = c.typeCheck(q"""
import scala.collection.par._
implicit val dummy = Scheduler.Implicits.dummy
$tree
""")

          val result = if(typeOf[scala.collection.par.Par[_]].typeSymbol == typeChecked.tpe.typeSymbol) q"$tree.seq" else tree
          result
      }
       

        val typesWhiteList = Set(typeOf[List[_]].typeSymbol,
          typeOf[Range].typeSymbol,
          typeOf[scala.collection.immutable.Range.Inclusive].typeSymbol,
          typeOf[scala.collection.immutable.Map[_, _]].typeSymbol,
          typeOf[scala.collection.immutable.Set[_]].typeSymbol,
          typeOf[scala.collection.mutable.HashMap[_, _]].typeSymbol,
          typeOf[scala.collection.mutable.HashSet[_]].typeSymbol
//          typeOf[Array[_]].typeSymbol not working, scala.this.Predef.booleanArrayOps & etc
        )

        // TODO: fix broken newTermName("groupBy") 
        val methodWhiteList:Set[Name] = Set(TermName("foreach"), TermName("map"), TermName("reduce"), TermName("exists"), TermName("filter"), TermName("find"), TermName("flatMap"), TermName("forall"), TermName("count"), TermName("fold"), TermName("sum"), TermName("product"), TermName("min"), TermName("max"))
        val maintain2ArgumentList: Set[Name] = Set(TermName("fold"))
        val append2ArgumentList: Set[Name] = Set(TermName("sum"), TermName("product"), TermName("min"), TermName("max"))

        tree match {
          case q"$seq.aggregate[$tag]($zero)($seqop, $comboop)" =>
            // println("AGGREGATEtransforming " + tree)
            // println("\n\nseq type is " + seq.tpe)
            val whiteListed = (seq.tpe ne null) && typesWhiteList.contains(seq.tpe.typeSymbol)
            // println("type is in whiteList: " + whiteListed) 
            val rewrite: Tree = 
              if(whiteListed)
                  unPar(q"$seq.toPar.aggregate($zero)($seqop)($comboop)")
               else tree
            // if (whiteListed) println("quasiquote formed " + rewrite + "\n")

            super.transform(rewrite)

          case q"$seq.$method[..$targs]($bla)($cbf)" => //map
            // println("MAPtransforming " + tree)
            // println("\n\nseq type is " + seq.tpe)
            // println("method is " + method)
            val whiteListed = (seq.tpe ne null) && typesWhiteList.contains(seq.tpe.typeSymbol)
            val methodListed = methodWhiteList.contains(method)
            // println("type is in whiteList: " + whiteListed)
            // println("method is in whiteList: " + methodListed)
            val rewrite: Tree = 
              if(whiteListed && methodListed)
                if(maintain2ArgumentList.contains(method))
                  unPar(q"$seq.toPar.$method($bla)($cbf)")
                else unPar(q"$seq.toPar.$method($bla)")
               else tree
            // if (whiteListed) println("quasiquote formed " + rewrite + "\n")

            super.transform(rewrite)
          case q"$seq.$method[..$targs]($bla)" => //foreach, reduce, groupBy(broken?), exists, filter
            // println("FOREACHtransforming " + tree)
            // println("\n\nseq type is " + seq.tpe)
            // println("method is " + method)
            val whiteListed = (seq.tpe ne null) && typesWhiteList.contains(seq.tpe.typeSymbol)
            val methodListed = methodWhiteList.contains(method)
            // println("type is in whiteList: " + whiteListed)
            // println("method is in whiteList: " + methodListed)
            val rewrite: Tree = if(whiteListed && methodListed) 
              if(append2ArgumentList.contains(method))
                  unPar(q"$seq.toPar.$method($bla, dummy)")
              else unPar(q"$seq.toPar.$method($bla)") // q"$seq.foreach[..$targs]($bla)"
               else tree

            // if (whiteListed && methodListed) println("quasiquote formed " + rewrite + "\n")
            super.transform(rewrite)
          case _ =>
            // println("not transforming " + tree) 
            super.transform(tree)
        }
      }
    }

    val t = CastingTransformer.transform(exp.tree)
    
    val resultWithImports = q"""
import scala.collection.par._
implicit val dummy = Scheduler.Implicits.dummy
$t
"""
    // println("\n\n\nresult: " + resultWithImports)

    (c.resetAllAttrs(resultWithImports))

  }
}
