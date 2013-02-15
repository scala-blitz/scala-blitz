package scala.collection.parallel






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