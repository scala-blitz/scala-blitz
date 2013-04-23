package scala.collection.parallel






object Syntax {

  def zippableReduce() {
    import Par._
    import workstealing.Ops._

    implicit val ws: Scheduler = null

    val z: Zippable[Int] = null
    z.reduce(_ + _)

    val r: Par[Range] = (0 until 10).toPar
    r.reduce(_ + _)

    def foo(z: Zippable[Int]) {
      z.reduce(_ + _)
    }

    foo(r)
  }

  def zippableMap() {
    import Par._
    import workstealing.Ops._

    implicit val ws: Scheduler = null

    val z: Zippable[Int] = null
    z.map(_ + 1)

    val r: Par[Range] = (0 until 10).toPar
    r.map(_ + 1)
  }

}



