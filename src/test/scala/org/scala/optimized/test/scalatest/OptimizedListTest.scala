package org.scala.optimized.test.scalatest

import org.scalatest._
import org.scalatest.concurrent.Timeouts
import scala.collection.par._
import scala.collection.optimizer.{Lists, Optimized}


class OptimizedListTest extends FunSuite with Timeouts {

  test("aggregate") {
    val res0 = List(1,3,4,5,6,7)

    val res1 = new Optimized(res0)


    val res2 = new Lists.Ops(res1)

    println(res2.aggregate(0)(_+_)(_+_))

  }
}

