package org.scala.optimized.test.par
package scalameter



import scala.collection.par._
import org.scalameter.api._
import scala.reflect.ClassTag
import org.scalameter.PerformanceTest.OnlineRegressionReport


class ParArrayBench extends OnlineRegressionReport with Serializable with ParArraySnippets with Generators {

  /* config */

  val tiny = 100000
  val small = 1000000
  val large = 10000000

  /* generators */

  val opts = Context(
    exec.minWarmupRuns -> 50,
    exec.maxWarmupRuns -> 100,
    exec.benchRuns -> 48,
    exec.independentSamples -> 6,
    exec.outliers.suspectPercent -> 40,
    exec.jvmflags -> "-server -Xms3072m -Xmx3072m -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=64m -XX:+UseCondCardMark -XX:CompileThreshold=100 -Dscala.collection.parallel.range.manual_optimizations=false",
    reports.regression.noiseMagnitude -> 0.15)

  val oldopts = Context(
    exec.minWarmupRuns -> 2,
    exec.maxWarmupRuns -> 4,
    exec.benchRuns -> 4,
    exec.independentSamples -> 1,
    reports.regression.noiseMagnitude -> 0.75)

  /* benchmarks */
  class VolatileContainer {
    @volatile var m = 1
  }
  class NonVolatileContainer {
    var m = 1
  }


  performance of "Par[Array]" config (opts) in {

    measure method "NonContendedVolatileReads" config (opts) in {
      def arraysInitV(from: Int) =
	for (size <- sizes(from)) yield (0 until size).map(i=>new VolatileContainer).toArray
      def arraysInitNV(from: Int) =
	for (size <- sizes(from)) yield (0 until size).map(i=>new NonVolatileContainer).toArray

/*      using(arraysInitV(small)) curve ("volatile") in { x => (1 to 100).map{a: Int =>{ var i = 0
	val to = x.size
	var acc = 0
	while(i<to) {acc = acc + x(i).m
	  i = i + 1}
	acc}
      }
      }*/
      using(arraysInitNV(small)) curve ("nonVolatile") in { x => (1 to 100).map{a: Int =>{ var i = 0
	val to = x.size
	var acc = 0
	while(i<to) {acc = acc + x(i).m
	  i = i + 1}
	acc}
      }
      }

    }
    measure method "Boxing" config (opts) in {
      using(arrays(large)) curve ("Sequential") in { x => noboxing(x)(_ + _) }
      using(arrays(large)) curve ("SequentialBoxedOp") in { x => boxing(x)(_ + _) }
      using(arrays(large)) curve ("SequentialSpecialized") in { x => boxingSpec(x)(_ + _) }
      using(arraysBoxed(small)) curve ("SequentialBoxedOpBoxedData") in { x => boxingSpec(x)(_ + _) }
    }

    measure method "Dispatch" config (opts) in {
      val helpers = new Helpers
      import helpers._

      var j = 0
      var methodId = 0

      using(arrays(large)) curve ("Dynamic") in { arr =>
        val method = methodId % 2
        methodId = methodId + 1

        val inc: Int = if (method == 1) sum1(arr)(_ + _ + 3)
        else sum1(arr)(_ + _ + 4)
        j = j + inc
      }
      using(arrays(large)) curve ("Static") in { arr =>
        sum2(arr)
      }
    }

    measure method "reduce" in {
      using(arrays(large)) curve ("Sequential") in reduceSequential
      using(withSchedulers(arrays(large))) curve ("Par") in { t => reduceParallel(t._1)(t._2) }
      performance of "old" config (oldopts) in {
        using(arrays(small)) curve ("ParArray") in { _.par.reduce(_ + _) }
      }
    }

    measure method "mapReduce" in {
      using(arrays(large)) curve ("Sequential") in mapReduceSequential
      using(withSchedulers(arrays(large))) curve ("Par") in { t => mapReduceParallel(t._1)(t._2) }
    }

    measure method "aggregate" in {
      using(arrays(small)) curve ("Sequential") in aggregateSequential
      using(withSchedulers(arrays(small))) curve ("Par") in { t => aggregateParallel(t._1)(t._2) }
    }

    measure method "find(sin)" in {
      using(arrays(tiny)) curve ("Sequential") in findSinSequential
      using(withSchedulers(arrays(tiny))) curve ("Par") in { t => findSinParallel(t._1)(t._2) }
    }

    measure method "map(sqrt)" in {
      using(arrays(small)) curve ("Sequential") in mapSqrtSequential
      using(withSchedulers(arrays(small))) curve ("Par") in { t => mapSqrtParallel(t._1)(t._2) }
    }

    measure method "filter(mod3)" config (
      exec.minWarmupRuns -> 80,
      exec.maxWarmupRuns -> 160) in {
        using(arrays(small)) curve ("Sequential") in filterMod3Sequential
        using(withSchedulers(arrays(small))) curve ("Par") in { t => filterMod3Parallel(t._1)(t._2) }
        performance of "old" config (oldopts) in {
          using(arrays(tiny)) curve ("ParArray") in { _.par.filter(_ % 3 == 0) }
        }
      }

    measure method "filter(cos)" in {
      using(arrays(tiny)) curve ("Sequential") in filterCosSequential
      using(withSchedulers(arrays(tiny))) curve ("Par") in { t => filterCosParallel(t._1)(t._2) }
    }

    measure method "flatMap" in {
      using(arrays(small)) curve ("Sequential") in flatMapSequential
      using(withSchedulers(arrays(small))) curve ("Par") in { t => flatMapParallel(t._1)(t._2) }
    }

    performance of "derivative" in {
      measure method "fold(product)" in {
        using(arrays(small)) curve ("Sequential") in foldProductSequential
        using(withSchedulers(arrays(small))) curve ("Par") in { t => foldProductParallel(t._1)(t._2) }
      }

      measure method "foreach" in {
        using(arrays(small)) curve ("Sequential") in foreachSequential
        using(withSchedulers(arrays(small))) curve ("Par") in { t => foreachParallel(t._1)(t._2) }
      }

      measure method ("sum") in {
        using(arrays(small)) curve ("Sequential") in sumSequential
        using(withSchedulers(arrays(small))) curve ("Par") in { t => sumParallel(t._1)(t._2) }
      }

      measure method ("product") in {
        using(arrays(small)) curve ("Sequential") in productSequential
        using(withSchedulers(arrays(small))) curve ("Par") in { t => productParallel(t._1)(t._2) }
      }

      measure method ("count(squareMod3)") in {
        using(arrays(small)) curve ("Sequential") in countSquareMod3Sequential
        using(withSchedulers(arrays(small))) curve ("Par") in { t => countSquareMod3Parallel(t._1)(t._2) }
      }
    }

  }

}

class Helpers extends Serializable {

  def sum1(source: Array[Int])(f: (Int, Int) => Int): Int = {
    var idx = 1
    var sum = source(0)
    val limit1 = source.size / 2
    val limit2 = source.size
    while (idx < limit1) { // intensionaly increasing method size, to make sure that method doesn't inline entirely
      sum = f(sum, source(idx))
      idx = idx + 1
    }
    while (idx < limit2) {
      sum = f(sum, source(idx))
      idx = idx + 1
    }
    sum
  }

  def sum2(source: Array[Int]): Int = {
    var idx = 1
    var sum = source(0)
    val limit1 = source.size / 2
    val limit2 = source.size
    while (idx < limit1) { // intensionaly increasing method size, to make sure that method doesn't inline entirely
      sum = sum + source(idx) + 5
      idx = idx + 1
    }
    while (idx < limit2) {
      sum = sum + source(idx) + 5
      idx = idx + 1
    }
    sum
  }
}
