package scala.collection



import sun.misc.Unsafe



package object parallel {
  
}


package parallel {

  trait StatisticsBenchmark extends testing.Benchmark {
    protected def printStatistics(name: String, measurements: Seq[Long]) {
      val avg = (measurements.sum.toDouble / measurements.length)
      val stdev = math.sqrt(measurements.map(x => (x - avg) * (x - avg)).sum / (measurements.length - 1))

      println(name)
      println(" min:   " + measurements.min)
      println(" max:   " + measurements.max)
      println(" med:   " + measurements.sorted.apply(measurements.length / 2))
      println(" avg:   " + avg.toLong)
      println(" stdev: " + stdev)
    }
  }


  object Utils {

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

}

