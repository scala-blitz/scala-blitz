package org.scala.optimized.test.par
package scalameter


import scala.collection.par._
import org.scalameter.api._
import scala.reflect.ClassTag
import org.scalameter.PerformanceTest.OnlineRegressionReport


class ParConcBench extends OnlineRegressionReport with Serializable with ParConcSnippets with Generators {

  /* config */

  val concFrom = 300000
  val bufferFrom = 10000000

  val opts = Context(
    exec.minWarmupRuns -> 25,
    exec.maxWarmupRuns -> 50,
    exec.benchRuns -> 48,
    exec.independentSamples -> 6,
    exec.outliers.suspectPercent -> 40,
    exec.jvmflags -> "-server -Xms1536m -Xmx1536m -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=64m -XX:+UseCondCardMark -XX:CompileThreshold=100 -Dscala.collection.parallel.range.manual_optimizations=false",
    reports.regression.noiseMagnitude -> 0.15
  )

  /* tests */

  performance of "Par[Conc]" config(opts) in {

    measure method "reduce" in {
      using(arrays(bufferFrom)) curve("Array") in reduceSequential
      using(withSchedulers(normalizedConcs(concFrom))) curve("Conc") in { t => reduceParallel(t._1)(t._2) }
      using(withSchedulers(bufferConcs(bufferFrom))) curve("Conc.Buffer") in { t => reduceParallel(t._1)(t._2) }
    }

    measure method "copyToArray" in {
      using(withArrays(arrays(bufferFrom))) curve("Array") in copyToArraySequential
      using(withSchedulers(withArrays(bufferConcs(bufferFrom)))) curve("Conc.Buffer") in { t => copyToArrayParallel(t._1)(t._2) }
    }

  }

}






