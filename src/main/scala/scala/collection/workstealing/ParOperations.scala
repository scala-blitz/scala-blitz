package scala.collection.workstealing



import scala.language.experimental.macros
import scala.reflect.macros._



trait ParOperations[T] {

  def foreach[U](f: T => U): Unit = macro ParOperations.foreach[T, U]

  def fold[U >: T](z: U)(op: (U, U) => U): U = macro ParOperations.fold[T, U]

}


object ParOperations {
  
  // TODO fix for case where `f` is not a function literal
  def foreach[T: c.WeakTypeTag, U: c.WeakTypeTag](c: Context)(f: c.Expr[T => U]): c.Expr[Unit] = {
    import c.universe._

    val prefix = c.applicationPrefix
    val callee = c.Expr[Nothing](prefix)
    val kernel = reify {
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
    c.inlineAndReset(kernel)
  }

  // TODO fix for case where `z` and `op` are not literals
  def fold[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(z: c.Expr[U])(op: c.Expr[(U, U) => U]): c.Expr[U] = {
    import c.universe._

    val prefix = c.applicationPrefix
    val callee = c.Expr[Nothing](prefix)
    val kernel = reify {
      val xs = callee.splice.asInstanceOf[Workstealing[T]]
      xs.invokeParallelOperation(new xs.Kernel[T, U] {
        val zero = z.splice
        def combine(a: U, b: U) = op.splice(a, b)
        def apply(node: xs.N[Unit], chunkSize: Int) = {
          var left = chunkSize
          var sum = zero
          while (left > 0) {
            sum = op.splice(sum, node.next())
            left -= 1
          }
          sum
        }
      })
    }
    c.inlineAndReset(kernel)
  }

}

















