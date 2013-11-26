package org.scala.optimized.test.par
package scalatest



import org.scalatest._
import scala.collection.par._



class DefaultScheduler extends FunSuite {

  test("default global scheduler") {
    import Scheduler.Implicits.global
    val parreduced = (0 until 1000000).toPar.reduce(_ + _)
    val reduced = (0 until 1000000).reduce(_ + _)
    assert(parreduced === reduced)
  }

}
