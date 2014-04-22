package org.scala.optimized.test.par
package scalameter



import scala.collection.par._
import org.scalameter.api._
import org.scalameter.PerformanceTest.OnlineRegressionReport


class HashesBench extends OnlineRegressionReport with Serializable with Generators with scala.collection.parallel.scalatest.Helpers{
  import Scheduler.Config

  /* config */

  val from = 3000000

  var createTuple1 = (x: Int) => (x, x)

  def createTuple2(x: Int) = (x, x)

  def arrayPairs(from: Int) = withArrays(arrays(from))

  class TupleMerger(val keys: Array[Int], val vals: Array[Int]) {
    final def +=(i: Int, kv: (Int, Int)) {
      keys(i) = kv._1
      vals(i) = kv._2
    }
  }

  def tupleMerger(kvs: (Array[Int], Array[Int])) = new TupleMerger(kvs._1, kvs._2)

  class DirectMerger(val keys: Array[Int], val vals: Array[Int]) {
    final def +=(i: Int, k: Int, v: Int) {
      keys(i) = k
      vals(i) = v
    }
  }

  def directMerger(kvs: (Array[Int], Array[Int])) = new DirectMerger(kvs._1, kvs._2)

  def mergers[F](f: ((Array[Int], Array[Int])) => F)(from: Int) = for (kvs <- arrayPairs(from)) yield {
    val m = f(kvs)
    (m, kvs._1.length)
  }

  performance of "Hashes" config(
    exec.independentSamples -> 3,
    reports.regression.noiseMagnitude -> 0.20
  ) in {

    performance of "derivative" in {
      measure method "add" in {
        using(arrayPairs(from)) curve("direct-direct") in { kvs =>
          val keys = kvs._1
          val vals = kvs._2
          val sz = keys.length
          var i = 0
          while (i < sz) {
            keys(i) = i
            vals(i) = i
            i += 1
          }
        }
  
        using(arrayPairs(from)) curve("tuplefunc-direct") in { kvs =>
          val keys = kvs._1
          val vals = kvs._2
          val sz = keys.length
          var i = 0
          while (i < sz) {
            val t = createTuple1(i)
            keys(i) = t._1
            vals(i) = t._2
            i += 1
          }
        }
  
        using(arrayPairs(from)) curve("tuplemethod-direct") in { kvs =>
          val keys = kvs._1
          val vals = kvs._2
          val sz = keys.length
          var i = 0
          while (i < sz) {
            val t = createTuple2(i)
            keys(i) = t._1
            vals(i) = t._2
            i += 1
          }
        }
  
        using(mergers(tupleMerger)(from)) curve("tuplemethod-tuplemethod") in { fl =>
          val f = fl._1
          val sz = fl._2
          var i = 0
          while (i < sz) {
            val t = createTuple2(i)
            f.+=(i, t)
            i += 1
          }
        }
  
        using(mergers(directMerger)(from)) curve("tuplemethod-directmethod") in { fl =>
          val f = fl._1
          val sz = fl._2
          var i = 0
          while (i < sz) {
            val t = createTuple2(i)
            f.+=(i, t._1, t._2)
            i += 1
          }
        }
  
        using(mergers(tupleMerger)(from)) curve("direct-tuplemethod") in { fl =>
          val f = fl._1
          val sz = fl._2
          var i = 0
          while (i < sz) {
            val t = (i, i)
            f.+=(i, t)
            i += 1
          }
        }
      }
    }

    measure method "add" in {
      using(sizes(from)) curve("direct") in { sz =>
        val keys = new Conc.Buffer[Int]
        val vals = new Conc.Buffer[Int]
        var i = 0
        while (i < sz) {
          keys += i
          vals += i
          i += 1
        }
      }

      using(sizes(from)) curve("HashMapMerger") in hashMapMergerAdd

      using(sizes(from / 10)) curve("ParHashMapCombiner") in parHahsMapCombinerAdd
    }

  }

  def hashMapMergerAdd(sz: Int) {
    val merger = new workstealing.HashTables.HashMapDiscardingMerger[Int, Int](5, 450, 27, null)
    var i = 0
    while (i < sz) {
      val kv = (i, i)
      merger += kv
      i += 1
    }
  }

  def parHahsMapCombinerAdd(sz: Int) {
    val cmb = getParHashMapCombiner[Int, Int]
    var i = 0
    while (i < sz) {
      val kv = (i, i)
      cmb += kv
      i += 1
    }
  }

}




