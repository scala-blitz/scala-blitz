package org.scala.optimized.test.scalatest

import org.scalatest._
import org.scalatest.concurrent.Timeouts
import Matchers._
import scala.collection.par._
import scala.collection.optimizer._
import org.scalatest.prop.{PropertyChecks}
import scala.util.Try


class OptimizedListTest extends FunSuite with Timeouts with PropertyChecks {

  test("aggregate") {
    forAll { (res0: List[Int]) =>
      val res2 = res0.opt
      res2.aggregate(0)(_ + _)(_ + _) shouldBe res0.aggregate(0)(_ + _, _ + _)
    }
  }

  test("reduce") {
    forAll { (res0: List[Int]) =>
      val res2 = res0.opt
      whenever(res0.nonEmpty)(res2.reduce(_ + _) shouldBe res0.reduce(_ + _))
    }
  }


  test("fold") {
    forAll { (res0: List[Int]) =>
      val res2 = res0.opt
      res2.fold(0)(_ + _) shouldBe res0.fold(0)(_ + _)
    }
  }

  test("min") {
    forAll { (res0: List[Int]) =>
      val res2 = res0.opt
      whenever(res0.nonEmpty)(res2.min shouldBe res0.min)
    }
  }

  test("max") {
    forAll { (res0: List[Int]) =>
      val res2 = res0.opt
      whenever(res0.nonEmpty)(res2.max shouldBe res0.max)
    }
  }

  test("sum") {
    forAll { (res0: List[Int]) =>
      val res2 = res0.opt
      res2.sum shouldBe res0.sum
    }
  }

  test("product") {
    forAll { (res0: List[Int]) =>
      val res2 = res0.opt
      res2.product shouldBe res0.product
    }
  }

  test("find") {
    forAll { (res0: List[Int], q: Int) =>
      val res2 = res0.opt
      res2.find(_ == q) shouldBe res0.find(_ == q)
    }
  }

  test("exists") {
    forAll { (res0: List[Int], q: Int) =>
      val res2 = res0.opt
      res2.exists(_ == q) shouldBe res0.exists(_ == q)
    }
  }

  test("map") {
    forAll { (res0: List[Int], q: Int => Int) =>
      val res2 = res0.opt
      res2.map(q) shouldBe res0.map(q)
    }
  }

  test("flatMap") {
    forAll { (res0: List[Int], q: Int => List[Int]) =>
      val res2 = res0.opt
      res2.flatMap(q) shouldBe res0.flatMap(q)
    }
  }

  test("count") {
    forAll { (res0: List[Int], q: Int => Boolean) =>
      val res2 = res0.opt
      res2.count(q) shouldBe res0.count(q)
    }
  }

  test("filter") {
    forAll { (res0: List[Int], q: Int => Boolean) =>
      val res2 = res0.opt
      res2.filter(q) shouldBe res0.filter(q)
    }
  }
}