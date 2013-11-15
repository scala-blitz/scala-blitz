package org.scala.optimized.test.par
package scalatest



import org.scalatest._
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._
import scala.collection._
import scala.collection.par._



trait Tests[Repr] extends Timeouts with scala.collection.par.scalatest.Helpers {

  implicit val scheduler = new workstealing.Scheduler.ForkJoin(
    //new workstealing.Scheduler.Config.Default(1)
  )

  def testForSizes(method: Range => Unit): Unit

  def targetCollections(r: Range): Seq[Repr]

  def testOperation[T, S](numSec: Int = 3000, testEmpty: Boolean = true, comparison: (Range, T, S) => Boolean = (r: Range, x: T, y: S) => x == y)(rop: Range => T)(collop: Repr => S): Unit = {
    testForSizes { r =>
      testOperationForSize(r, numSec, testEmpty, comparison)(rop)(collop)
    }
  }

  def testOperationForSize[T, S](r: Range, numSec: Int = 10, testEmpty: Boolean = true, comparison: (Range, T, S) => Boolean = (r: Range, x: T, y: S) => x == y)(rop: Range => T)(collop: Repr => S): Unit = {
    val colls = targetCollections(r)

    for (coll <- colls) try {
      failAfter(numSec seconds) {
        val x = rop(r)
        val px = collop(coll)
        val compResult = comparison(r, x, px)
        if (!compResult) assert(compResult, "for: " + r + ": " + x + ", " + px)
      }
    } catch {
      case e: exceptions.TestFailedDueToTimeoutException =>
        assert(false, "timeout for: " + r)
    }
  }

  def seqComparison[T](range : Range, x: Seq[T], y: Seq[T]) = x == y

  def arrayComparison[T](range: Range, r: Seq[T], pa: Par[Array[T]]) = r == pa.seq.toBuffer

  def concComparison[T](range: Range, r: Seq[T], pc: Par[Conc[T]]) = r == pc.seq.toBuffer

  def hashMapComparison[K, V](range: Range, hm: mutable.Map[K, V], phm: Par[mutable.HashMap[K, V]]) = hm == phm.seq

  def hashMapArrayComparison[K, V](range: Range, hm: mutable.Map[K, Seq[V]], phm: Par[mutable.HashMap[K, Par[Array[V]]]]) = {
    val hm2 = phm.seq
    hm.keySet ==  hm2.keySet && hm.forall(x=> {x._2.toSet == hm2(x._1).seq.toSet})
  }

  def hashMapImmutableSetComparison[K, V](range: Range, hm: mutable.Map[K, Seq[V]], phm: Par[mutable.HashMap[K, Par[immutable.Set[V]]]]) = {
    val hm2 = phm.seq
    hm.keySet ==  hm2.keySet && hm.forall(x=> {x._2.toSet == hm2(x._1).seq.toSet})
  }

  def hashMapSetComparison[K, V](range: Range, hm: mutable.Map[K, Seq[V]], phm: Par[mutable.HashMap[K, Par[mutable.Set[V]]]]) = {
    val hm2 = phm.seq
    hm.keySet ==  hm2.keySet && hm.forall(x=> {x._2.toSet == hm2(x._1).seq.toSet})
  }

  def immutableHashMapComparison[K, V](range: Range, hm: immutable.HashMap[K, V], phm: Par[immutable.HashMap[K, V]]) = hm == phm.seq

  def hashSetComparison[T](range: Range, hs: mutable.Set[T], phs: Par[mutable.HashSet[T]]) = hs == phs.seq

  def hashTrieSetComparison[T](range: Range, hs: immutable.Set[T], phs: Par[immutable.HashSet[T]]) = hs == phs.seq

  def hashTrieMapComparison[K, V](range: Range, hm: immutable.Map[K, V], phm: Par[immutable.HashMap[K, V]]) = hm == phm.seq

}
