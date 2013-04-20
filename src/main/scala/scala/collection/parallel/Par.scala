package scala.collection.parallel






class Par[T, Repr](val data: Repr) extends AnyVal


object Par {

  implicit class Traversable2ToParOps[Repr](val xs: Repr) extends AnyVal {
    def toPar[T](implicit isTraversable: Repr <:< TraversableOnce[T]): Par[T, Repr] = new Par(xs)
  }

  implicit def reducableOps[T, Repr](p: Par[T, Repr])(implicit cr: CanReduce[T, Repr]): cr.Ops = cr.reducableOps(p.data)

  implicit def context[T, Repr](implicit c: CanReduce[T, Repr]): c.Ctx = c.context

}


trait CanReduce[T, Repr] extends Any {
  type Ctx <: Context
  type Ops <: ReducableOps[T, Repr, Ctx]
  def reducableOps(repr: Repr): Ops
  def context: Ctx
}


trait CanZip[T, Repr] extends Any with CanReduce[T, Repr] {
  type Ctx <: Context
  type Ops <: ZippableOps[T, Repr, Ctx]
  def zippableOps(repr: Repr): Ops
}


object Test {
  import Par._

  implicit val s: Context = null

  class RangeZippableOps(val r: Range) extends AnyVal with ZippableOps[Int, Range, Context] {
    def reduce[U >: Int](op: (U, U) => U)(implicit ctx: Context): U = null.asInstanceOf[U]
    def copyToArray[U >: Int](arr: Array[U], start: Int, len: Int): Unit = ()
  }

  class RangeCanZip(val context: Context) extends AnyVal with CanZip[Int, Range] {
    type Ctx = Context
    type Ops = RangeZippableOps
    def reducableOps(r: Range) = new Ops(r)
    def zippableOps(r: Range) = new Ops(r)
  }

  implicit def rangeCanZip(implicit s: Context) = new RangeCanZip(s)

  val pr = (0 until 10).toPar
  pr.reduce(_ + _)

  Test2.foo(pr)

}


object Test2 {
  import Par._

  def foo[CC](xs: Par[Int, CC])(implicit cr: CanReduce[Int, CC]) = {
    xs.reduce(_ + _)
  }

}