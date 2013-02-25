package scala.collection.workstealing



import scala.language.experimental.macros
import scala.reflect.macros._



trait ParOperations[T] {

  def foreach[U](f: T => U): Unit = macro ParOperations.foreach[T, U]

}


object ParOperations {
  
  def foreach[T: c.WeakTypeTag, U: c.WeakTypeTag](c: Context)(f: c.Expr[T => U]): c.Expr[Unit] = {
    import c.universe._

    val prefix = c.applicationPrefix

    val callee = c.Expr[Nothing](prefix)
    val loop = reify {
      val xs = callee.splice.asInstanceOf[Workstealing[T]]
      xs.invokeParallelOperation(new xs.Kernel[T, Unit] {
        def zero = ()
        def combine(a: Unit, b: Unit) = a
        def apply(node: xs.N[Unit], chunkSize: Int) = {
          var left = chunkSize
          while (left > 0) {
            f.splice(node.next())
            left -= 1
          }
        }
      })
    }
    c.inlineAndReset(loop)
  }

}

















