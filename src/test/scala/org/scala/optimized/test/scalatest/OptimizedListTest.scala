package org.scala.optimized.test.scalatest

import org.scalatest._
import org.scalatest.concurrent.Timeouts
import Matchers._
import scala.collection.par._
import scala.collection.optimizer.{Lists, Optimized}
import org.scalatest.prop.{PropertyChecks}
import scala.util.Try


class OptimizedListTest extends FunSuite with Timeouts with PropertyChecks {

  test("aggregate") {
    forAll { (res0: List[Int]) =>
      val res1 = new Optimized(res0)
      val res2 = new Lists.Ops(res1)
      res2.aggregate(0)(_ + _)(_ + _) shouldBe res0.aggregate(0)(_ + _, _ + _)
    }
  }

  test("reduce") {
    forAll { (res0: List[Int]) =>
      val res1 = new Optimized(res0)
      val res2 = new Lists.Ops(res1)
      whenever(res0.nonEmpty)(res2.reduce(_ + _) shouldBe res0.reduce(_ + _))
    }
  }

  test("fold") {
    forAll { (res0: List[Int]) =>
      val res1 = new Optimized(res0)
      val res2 = new Lists.Ops(res1)
      res2.fold(0)(_ + _) shouldBe res0.fold(0)(_ + _)
    }
  }

  test("min") {
    forAll { (res0: List[Int]) =>
      val res1 = new Optimized(res0)
      val res2 = new Lists.Ops(res1)
      whenever(res0.nonEmpty)(res2.min shouldBe res0.min)
    }
  }

  test("max") {
    forAll { (res0: List[Int]) =>
      val res1 = new Optimized(res0)
      val res2 = new Lists.Ops(res1)
      whenever(res0.nonEmpty)(res2.max shouldBe res0.max)
    }
  }

  test("sum") {
    forAll { (res0: List[Int]) =>
      val res1 = new Optimized(res0)
      val res2 = new Lists.Ops(res1)
      res2.sum shouldBe res0.sum
    }
  }

  test("product") {
    forAll { (res0: List[Int]) =>
      val res1 = new Optimized(res0)
      val res2 = new Lists.Ops(res1)
      res2.product shouldBe res0.product
    }
  }

  test("find") {
    forAll { (res0: List[Int], q: Int) =>
      val res1 = new Optimized(res0)
      val res2 = new Lists.Ops(res1)
      res2.find(_ == q) shouldBe res0.find(_ == q)
    }
  }

  test("exists") {
    forAll { (res0: List[Int], q: Int) =>
      val res1 = new Optimized(res0)
      val res2 = new Lists.Ops(res1)
      res2.exists(_ == q) shouldBe res0.exists(_ == q)
    }
  }

  test("map") {
    forAll { (res0: List[Int], q: Int => Int) =>
      val res1 = new Optimized(res0)
      val res2 = new Lists.Ops(res1)
      res2.map(q) shouldBe res0.map(q)
    }
  }

  test("flatMap") {
    forAll { (res0: List[Int], q: Int => List[Int]) =>
      val res1 = new Optimized(res0)
      val res2 = new Lists.Ops(res1)
      res2.flatMap(q) shouldBe res0.flatMap(q)
    }
  }
}

/*

trait Tree {
  def withParent(parent: Tree): this.type
}
object NoParent extends Tree
class Sel private(private[this] var _p: Tree, private[this] var _n:Tree, private val _parent:Tree = NoParent, private val orig: Sel = null) extends Tree{
  def withParent(parent: Tree) = {
    Sel(null, null, parent, this)
  }
  def parent = _parent
  def p: Tree = {
    if(_p eq null && (orig ne null)) _p = orig.p.withParent(this)
    /*if(this._p == NoParent) ??? else*/ _p
  }
  def n: Tree = {
    if(_n eq null &&(orig ne null)) _n = orig.p.withParent(this)
    /*if(this._n == NoParent) ??? else*/ _n
  }
}

object Sel{
  def apply(p: Tree, n: Tree) = {
    val s = new Sel(NoParent, NoParent, NoParent)
    s._p = p.withParent(s)
    s._n = n.withParent(s)
  }
}*/