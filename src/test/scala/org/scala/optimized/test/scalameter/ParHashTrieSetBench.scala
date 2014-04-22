package org.scala.optimized.test.par
package scalameter


import scala.collection.par._
import org.scalameter.api._
import scala.reflect.ClassTag
import org.scalameter.PerformanceTest.OnlineRegressionReport


class ParHashTrieSetBench extends OnlineRegressionReport with Serializable with ParHashTrieSetSnippets with Generators {
  import Scheduler.Config

  /* config */

  val tiny = 12500
  val small = 25000
  val normal = 150000

  val opts = Context(
    exec.minWarmupRuns -> 30,
    exec.maxWarmupRuns -> 60,
    exec.benchRuns -> 40,
    exec.independentSamples -> 4,
    exec.outliers.suspectPercent -> 40,
    exec.outliers.retries -> 20,
    exec.jvmflags -> "-server -Xms1536m -Xmx1536m -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=64m -XX:+UseCondCardMark -XX:CompileThreshold=100 -Dscala.collection.parallel.range.manual_optimizations=false",
    reports.regression.noiseMagnitude -> 0.15
  )

  /* tests */

  performance of "Par[immutable.HashSet]" config(opts) in {

    measure method "aggregate" in {
      using(hashTrieSets(normal)) curve("Sequential") in aggregateSequential
      using(withSchedulers(hashTrieSets(normal))) curve("Par") in { t => aggregateParallel(t._1)(t._2) }
    }

    measure method "map" in {
      using(hashTrieSets(small)) curve("Sequential") in mapSequential
      using(withSchedulers(hashTrieSets(small))) curve("Par") in { t => mapParallel(t._1)(t._2) }
    }
    measure method "mapReduce" in {
      using(hashTrieSets(normal)) curve("Sequential") in mapReduceSequential
      using(withSchedulers(hashTrieSets(normal))) curve("Par") in { t => mapReduceParallel(t._1)(t._2) }
    }

    measure method "aggregate" in {
      using(hashTrieSets(normal)) curve ("Sequential") in aggregateSequential
      using(withSchedulers(hashTrieSets(normal))) curve ("Par") in { t =>
        implicit val scheduler = t._2
        aggregateParallel(t._1)(t._2)
      }
    }

    measure method "find" in {
      using(hashTrieSets(normal)) curve ("Sequential") in {t => findSequential(t, -1)}
      using(withSchedulers(hashTrieSets(normal))) curve ("Par") in { t =>
        implicit val scheduler = t._2
        findParallel(t._1, -1)(t._2)
      }
    }

    measure method "find(sin)" in {
      using(hashTrieSets(small)) curve ("Sequential") in findSinSequential
      using(withSchedulers(hashTrieSets(small))) curve ("Par") in { t =>
        implicit val scheduler = t._2
        findSinParallel(t._1)(t._2)
      }
    }

    measure method "map" in {
      using(hashTrieSets(small)) curve ("Sequential") in mapSequential
      using(withSchedulers(hashTrieSets(small))) curve ("Par") in { t =>
        implicit val scheduler = t._2
        mapParallel(t._1)(t._2)
      }
    }

    measure method "flatMap" in {
      using(hashTrieSets(small)) curve ("Sequential") in flatMapSequential
      using(withSchedulers(hashTrieSets(small))) curve ("Par") in { t =>
        implicit val scheduler = t._2
        flatMapParallel(t._1)(t._2)
      }
    }

    performance of "derivative" in {

      measure method "reduce" in {
        using(hashTrieSets(normal)) curve ("Sequential") in reduceSequential
        using(withSchedulers(hashTrieSets(normal))) curve ("Par") in { t =>
          implicit val scheduler = t._2
          reduceParallel(t._1)
        }
      }

      measure method "fold" in {
        using(hashTrieSets(normal)) curve ("Sequential") in foldSequential
        using(withSchedulers(hashTrieSets(normal))) curve ("Par") in { t =>
          implicit val scheduler = t._2
          foldParallel(t._1)(t._2)
        }
      }

      measure method "filter(mod3)" config (
        exec.minWarmupRuns -> 80,
        exec.maxWarmupRuns -> 160) in {
          using(hashTrieSets(small)) curve ("Sequential") in filterMod3Sequential
          using(withSchedulers(hashTrieSets(small))) curve ("Par") in { t =>
            implicit val scheduler = t._2
            filterMod3Parallel(t._1)(t._2)
          }
        }

      measure method "filter(sin)" in {
        using(hashTrieSets(tiny)) curve ("Sequential") in findSinSequential
        using(withSchedulers(hashTrieSets(tiny))) curve ("Par") in { t =>
          implicit val scheduler = t._2
          filterSinParallel(t._1)(t._2)
        }
      }

      measure method "foreach" in {
        using(hashTrieSets(small)) curve ("Sequential") in foreachSequential
        using(withSchedulers(hashTrieSets(small))) curve ("Par") in { t =>
          implicit val scheduler = t._2
          foreachParallel(t._1)(t._2)
        }
      }

      measure method ("sum") in {
        using(hashTrieSets(normal)) curve ("Sequential") in sumSequential
        using(withSchedulers(hashTrieSets(normal))) curve ("Par") in { t =>
          implicit val scheduler = t._2
          sumParallel(t._1)(t._2)
        }
      }

      measure method ("product") in {
        using(hashTrieSets(normal)) curve ("Sequential") in productSequential
        using(withSchedulers(hashTrieSets(normal))) curve ("Par") in { t =>
          implicit val scheduler = t._2
          productParallel(t._1)(t._2)
        }
      }

      measure method ("count(&1==0)") in {
        using(hashTrieSets(normal)) curve ("Sequential") in countSquareMod3Sequential
        using(withSchedulers(hashTrieSets(normal))) curve ("Par") in { t =>
          implicit val scheduler = t._2
          countSquareMod3Parallel(t._1)(t._2)
        }
      }
    

  }


  }

}






