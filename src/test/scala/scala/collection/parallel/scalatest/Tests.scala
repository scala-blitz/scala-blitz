package scala.collection.parallel
package scalatest



import org.scalatest._
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._
import scala.collection._



trait Tests[Repr] extends Timeouts {

  implicit val scheduler = new workstealing.WorkstealingTreeScheduler.ForkJoin(
    new workstealing.WorkstealingTreeScheduler.Config.Default(1)
  )

  def testForSizes(method: Range => Unit): Unit

  def targetCollections(r: Range): Seq[Repr]

  def testOperation[T, S](numSec: Int = 6, testEmpty: Boolean = true, comparison: (Range, T, S) => Boolean = (r: Range, x: T, y: S) => x == y)(rop: Range => T)(collop: Repr => S): Unit = {
    testForSizes { r =>
      testOperationForSize(r, numSec, testEmpty, comparison)(rop)(collop)
    }
  }

  def testOperationForSize[T, S](r: Range, numSec: Int = 6, testEmpty: Boolean = true, comparison: (Range, T, S) => Boolean = (r: Range, x: T, y: S) => x == y)(rop: Range => T)(collop: Repr => S): Unit = {
    import Par._
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

  def hashMapComparison[K, V](range: Range, hm: mutable.HashMap[K, V], phm: Par[mutable.HashMap[K, V]]) = hm == phm.seq

}
