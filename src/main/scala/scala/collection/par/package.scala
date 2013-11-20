package scala.collection



import sun.misc.Unsafe



package object par extends par.ParDefs with par.workstealing.OpsDefs {

  object Configuration {
    val manualOptimizations = sys.props.get("scala.collection.par.range.manual_optimizations").map(_.toBoolean).getOrElse(true)
  }

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

