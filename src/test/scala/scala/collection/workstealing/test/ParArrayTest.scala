package scala.collection.workstealing
package test



import scala.collection._



object ParArrayTest extends App {

  def checkFilter(sz: Int) {
    val pa = new ParArray[Int](sz, Workstealing.DefaultConfig)
    pa.filter(_ % 2 == 0)
  }

  checkFilter(1)

}


