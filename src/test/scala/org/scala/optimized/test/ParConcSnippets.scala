package org.scala.optimized.test.par     




import scala.reflect.ClassTag
import scala.collection.par._
import Scheduler.Config




trait ParConcSnippets {

  def reduceSequential(a: Array[Int]) = {
    var i = 0
    val until = a.length
    var sum = 1
    while (i < until) {
      sum += a(i)
      i += 1
    }
    sum
  }

  def reduceParallel(c: Conc[Int])(implicit s: Scheduler) = c.toPar.reduce(_ + _)

  def copyToArraySequential(t: (Array[Int], Array[Int])) = t match {
    case (src, dest) => Array.copy(src, 0, dest, 0, src.length)
  }

  def copyToArrayParallel(t: (Conc[Int], Array[Int]))(implicit s: Scheduler) = t match {
    case (c, dest) => c.toPar.copyToArray(dest, 0, c.length)
  }

}









