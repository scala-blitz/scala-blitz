package scala.collection.parallel



import Par._



object Syntax {

  def zippableReduce() {
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
    import workstealing.Ops._

    implicit val ws: Scheduler = null

    val z: Zippable[Int] = null
    z.map(_ + 1)

    val r: Par[Range] = (0 until 10).toPar
    r.map(_ + 1)
  }

  def zippableConcReduce() {
    import workstealing.Ops._

    implicit val ws: Scheduler = null

    val pc: Par[Conc[Int]] = Conc.Zero.toPar
    pc.reduce(_ + _)

    def foo(z: Zippable[Int]) {
      z.reduce(_ + _)
    }

    foo(pc)
  }

  def flatMap() {
    import workstealing.Ops._

    implicit val ws: Scheduler = null
    val list = List(2, 3, 5)
    val pa: Par[Array[Int]] = Array(1, 2, 3).toPar
    for {
      x <- pa
      y <- list
    } yield {
      x * y
    }: @unchecked
  }

  def flatMap2() {
    import workstealing.Ops._

    implicit val ws: Scheduler = null
    val list = List(2, 3, 5)
    val pa: Par[Array[Int]] = Array(1, 2, 3).toPar
    pa.flatMap(x => list.map((_ * x): @unchecked))
  }

  def flatMap3() {
    import workstealing.Ops._

    implicit val ws: Scheduler = null
    val list = List(2, 3, 5)
    val pa: Par[Array[Int]] = Array(1, 2, 3).toPar
    for { 
      x <- pa     
      y <- list
      zzzzzzzz <- list
    } yield {
      x * y * zzzzzzzz          
    }: @unchecked
  }

}

















