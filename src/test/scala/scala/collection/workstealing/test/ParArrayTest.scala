package scala.collection.workstealing
package test



import scala.collection._



object ParArrayTest extends App {

  def checkFilter(sz: Int) {
    println("For size: " + sz)
    try {
      val pa = new ParArray[Int]((0 until sz).toArray, Workstealing.DefaultConfig)
      val res = pa.filter(x => true)
      assert(res.array.toList == (0 until sz), "size " + sz)
    } catch {
      case e: Exception =>
        println("Exception for size: " + sz)
        throw e
    }
  }

  checkFilter(10)
  checkFilter(4)
  for (i <- 0 until 16) checkFilter(i)
  for (i <- 4 until 16) checkFilter(4 * i)
  for (i <- 4 until 16) checkFilter(16 * i)
  for (i <- 4 until 16) checkFilter(64 * i)
  checkFilter(2048)
  checkFilter(2049)
  checkFilter(2050)
  checkFilter(3000)
  checkFilter(4096)
  checkFilter(5120)
  for (i <- 4 until 16) checkFilter(1024 * i)
  for (i <- 4 until 16) checkFilter(16384 * i)

}


