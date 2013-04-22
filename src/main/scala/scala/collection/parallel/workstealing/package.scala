package scala.collection.parallel



import sun.misc.Unsafe



package object workstealing {

  /* utilities */

  val unsafe = getUnsafe()

  def getUnsafe(): Unsafe = {
    if (this.getClass.getClassLoader == null) Unsafe.getUnsafe()
    try {
      val fld = classOf[Unsafe].getDeclaredField("theUnsafe")
      fld.setAccessible(true)
      return fld.get(this.getClass).asInstanceOf[Unsafe]
    } catch {
      case e: Throwable => throw new RuntimeException("Could not obtain access to sun.misc.Unsafe", e)
    }
  }

}


object Test {
  import Par._
  import workstealing.Ops._

  implicit val ws: Scheduler = null

  val z: Zippable[Int] = null
  z.reduce(_ + _)

  val r: Par[Range] = (0 until 10).toPar
  r.reduce(_ + _)

}

