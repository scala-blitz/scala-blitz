package scala.collection.parallel






class Par[T, Repr](val data: Repr) extends AnyVal


object Par {

  implicit class Traversable2ToParOps[Repr](val xs: Repr) extends AnyVal {
    def toPar[T](implicit isTraversable: Repr <:< TraversableOnce[T]): Par[T, Repr] = new Par(xs)
  }

  trait CanReduce[T, Repr] {
    type Sch <: Scheduler
    type Ops <: ReducableOps[T, Repr, Sch]
    def reducableOps(repr: Repr): Ops
    def scheduler: Sch
  }

  @inline implicit def reducableOps[T, Repr](p: Par[T, Repr])(implicit cr: CanReduce[T, Repr]): cr.Ops = cr.reducableOps(p.data)

  @inline implicit def scheduler[T, Repr](implicit c: CanReduce[T, Repr]): c.Sch = c.scheduler

}


object Test {
  import Par._

  implicit val s: Scheduler = null

  class RangeReduceOps(val r: Range) extends AnyVal with ReducableOps[Int, Range, Scheduler] {
    def reduce[U >: Int](op: (U, U) => U)(implicit req: Scheduler): U = null.asInstanceOf[U]
  }

  implicit def rangeCanReduce(implicit s: Scheduler = null) = new CanReduce[Int, Range] {
    type Sch = Scheduler
    type Ops = RangeReduceOps
    def reducableOps(r: Range) = new Ops(r)
    def scheduler = s
  }

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