package org.scala.optimized.test.par
package scalameter


import scala.collection.par._
import org.scalameter.api._
import scala.reflect.ClassTag
import Scheduler.Config
import org.scalameter.PerformanceTest.OnlineRegressionReport


class ParImmutableTreeSetBench extends OnlineRegressionReport with Serializable with ParImmutableTreeSetSnippets with Generators {


  /* config */

  val treesFrom = 6000

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

  performance of "Par[immutable.TreeSet]" config(opts) in {

    measure method "aggregate" in {
      using(withSchedulers(immutableTreeSets(treesFrom))) curve("immutable.TreeSet") in { t => 
        aggregateParallel(t._1)(t._2)
      }
    }

  }

}






