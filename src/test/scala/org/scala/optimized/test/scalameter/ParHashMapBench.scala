package org.scala.optimized.test.par
package scalameter



import scala.collection.par._
import org.scalameter.api._
import scala.reflect.ClassTag
import org.scalameter.PerformanceTest.OnlineRegressionReport


class ParHashMapBench extends OnlineRegressionReport with Serializable with ParHashMapSnippets with Generators {
  import Scheduler.Config

  /* config */

  val tiny = 10000
  val small = 100000
  val normal = 300000

  val opts = Context(
    exec.minWarmupRuns -> 30,
    exec.maxWarmupRuns -> 60,
    exec.benchRuns -> 40,
    exec.independentSamples -> 5,
    exec.outliers.suspectPercent -> 40,
    exec.outliers.retries -> 20,
    exec.jvmflags -> "-server -Xms1536m -Xmx1536m -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=64m -XX:+UseCondCardMark -XX:CompileThreshold=100 -Dscala.collection.parallel.range.manual_optimizations=false",
    reports.regression.noiseMagnitude -> 0.15)

  /* tests */

  performance of "Par[HashMap]" config (opts) in {

    measure method "aggregate" in {
      using(hashMaps(normal)) curve ("Sequential") in aggregateSequential
      using(withSchedulers(hashMaps(normal))) curve ("Par") in { t => aggregateParallel(t._1)(t._2) }
    }

    measure method "filter" in {
      using(hashMaps(small)) curve ("Sequential") in filterSequential
      using(withSchedulers(hashMaps(small))) curve ("Par") in { t => filterParallel(t._1)(t._2) }
    }

    measure method "mapReduce" in {
      using(hashMaps(normal)) curve ("Sequential") in mapReduceSequential
      using(withSchedulers(hashMaps(normal))) curve ("Par") in { t => mapReduceParallel(t._1)(t._2) }
    }

    measure method "find(sin)" in {
      using(hashMaps(tiny)) curve ("Sequential") in findSinSequential
      using(withSchedulers(hashMaps(tiny))) curve ("Par") in { t => findSinParallel(t._1)(t._2) }
    }

    measure method "find" in {
      using(hashMaps(tiny)) curve ("Sequential") in (x => findSequential(x, -1))
      using(withSchedulers(hashMaps(tiny))) curve ("Par") in { t => findParallel(t._1, -1)(t._2) }
    }

    performance of "derivative" in {
      measure method "count" in {
        using(hashMaps(normal)) curve ("Sequential") in countSequential
        using(withSchedulers(hashMaps(normal))) curve ("Par") in { t => countParallel(t._1)(t._2) }
      }

      measure method "foreach" in {
        using(hashMaps(normal)) curve ("Sequential") in foreachSequential
        using(withSchedulers(hashMaps(normal))) curve ("Par") in { t => foreachParallel(t._1)(t._2) }
      }

      measure method "sum" in {
        using(hashMaps(normal)) curve ("Sequential") in sumSequential
        using(withSchedulers(hashMaps(normal))) curve ("Par") in { t => sumParallel(t._1)(t._2) }
      }

      measure method "product" in {
        using(hashMaps(normal)) curve ("Sequential") in productSequential
        using(withSchedulers(hashMaps(normal))) curve ("Par") in { t => productParallel(t._1)(t._2) }
      }

      measure method "min" in {
        using(hashMaps(normal)) curve ("Sequential") in minSequential
        using(withSchedulers(hashMaps(normal))) curve ("Par") in { t => minParallel(t._1)(t._2) }
      }

      measure method "max" in {
        using(hashMaps(normal)) curve ("Sequential") in maxSequential
        using(withSchedulers(hashMaps(normal))) curve ("Par") in { t => maxParallel(t._1)(t._2) }
      }

    }

  }

}

