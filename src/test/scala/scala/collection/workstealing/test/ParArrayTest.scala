package scala.collection.workstealing
package test



import scala.collection._



object ParArrayTest extends App {

  def checkFilter(sz: Int) {
    try {
      val pa = new ParArray[Int](sz, Workstealing.DefaultConfig)
      pa.filter(_ % 2 == 0)
    } catch {
      case e: Exception =>
        println("Exception for size: " + sz)
        throw e
    }
  }

  checkFilter(10)
  // for (i <- 0 until 16) checkFilter(i)
  // for (i <- 4 until 16) checkFilter(4 * i)
  // for (i <- 4 until 16) checkFilter(16 * i)
  // for (i <- 4 until 16) checkFilter(64 * i)

}


