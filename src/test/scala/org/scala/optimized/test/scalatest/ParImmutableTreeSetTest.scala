package org.scala.optimized.test.par
package scalatest



import org.scalatest._
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._
import scala.collection.par._
import scala.collection.immutable.TreeSet
import Conc._



class ParImmutableTreeSetTest extends FunSuite with Timeouts with Tests[TreeSet[String]] with ParImmutableTreeSetSnippets {

  def testForSizes(method: Range => Unit) {
    for (i <- 1 to 600) {
      method(0 to i)
      method(i to 0 by -1)
    }
    for (i <- 600 to 10000 by 20) {
      method(0 to i)
      method(i to 0 by -1)
    }
    for (i <- 10000 to 32000 by 500) {
      method(0 to i)
      method(i to 0 by -1)
    }
  }

  def targetCollections(r: Range) = Seq(
    createTreeSet(r)
  )

  def createTreeSet(r: Range) = TreeSet(r.map(_.toString): _*)

  test("aggregate") {
    val rt = (r: Range) => aggregateSequential(createTreeSet(r))
    val ct = (t: TreeSet[String]) => aggregateParallel(t)
    testOperation(testEmpty = false)(rt)(ct)
  }

}

















