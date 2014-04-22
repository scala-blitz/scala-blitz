package org.scala.optimized.test.par
package scalameter


import scala.collection.par._
import org.scalameter.api._
import scala.reflect.ClassTag
import org.scalameter.PerformanceTest.OnlineRegressionReport
import org.scalameter.KeyValue


class ReducableBench extends OnlineRegressionReport with Serializable with ReducableSnippets with Generators {

  /* config */

  val tiny =  60000
  val small = 600000
  val large = 1000000

  val TEST_TRIES = false // we shouldn't benchmark TRIE based data structures as they still fail tests

  /* generators */

  val opts = Context(
    exec.minWarmupRuns -> 25,
    exec.maxWarmupRuns -> 50,
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

  implicit def range2Reducable(r: Range)(implicit ctx: Scheduler): Reducable[Int] = par2zippable(r.toPar)
  implicit def array2Reducable(a: Array[Int])(implicit ctx: Scheduler): Reducable[Int] = par2zippable(a.toPar)

  implicit def hashTrieSet2Reducable(a: collection.immutable.HashSet[Int])(implicit ctx: Scheduler): Reducable[Int] = hashTrieSetIsReducable(a.toPar)
  implicit def hashSet2Reducable(a: collection.mutable.HashSet[Int])(implicit ctx: Scheduler): Reducable[Int] = hashSetIsReducable(a.toPar)

  /* benchmarks */

  performance of "Reducables" config (opts) in {

    measure method "mapReduce" in {
      using(ranges(large)) curve ("Sequential") in mapReduceSequential
      using(withSchedulers(ranges(large))) curve ("ParRange") in { t =>
        implicit val scheduler = t._2
        mapReduceParallel(t._1)(t._2)
      }
      using(withSchedulers(arrays(large))) curve ("ParArray") in { t =>
        implicit val scheduler = t._2
        mapReduceParallel(t._1)(t._2)
      }
      if (TEST_TRIES) using(withSchedulers(hashTrieSets(large))) curve ("ParHashTrieSet") in { t =>
        implicit val scheduler = t._2
        mapReduceParallel(t._1)(t._2)
      }
      using(withSchedulers(hashSets(small))) curve ("ParHashSet") in { t =>
        implicit val scheduler = t._2
        mapReduceParallel(t._1)(t._2)
      }
    }

    measure method "aggregate" in {
      using(ranges(large)) curve ("Sequential") in aggregateSequential
      using(withSchedulers(ranges(large))) curve ("ParRange") in { t =>
        implicit val scheduler = t._2
        aggregateParallel(t._1)(t._2)
      }
      using(withSchedulers(arrays(large))) curve ("ParArray") in { t =>
        implicit val scheduler = t._2
        aggregateParallel(t._1)(t._2)
      }
      if (TEST_TRIES) using(withSchedulers(hashTrieSets(large))) curve ("ParHashTrieSet") in { t =>
        implicit val scheduler = t._2
        aggregateParallel(t._1)(t._2)
      }
      using(withSchedulers(hashSets(large))) curve ("ParHashSet") in { t =>
        implicit val scheduler = t._2
        aggregateParallel(t._1)(t._2)
      }
    }

    measure method "find" in {
      using(ranges(large)) curve ("Sequential") in findNotExistingSequential
      using(withSchedulers(ranges(large))) curve ("ParRange") in { t =>
        implicit val scheduler = t._2
        findNotExistingParallel(t._1)(t._2)
      }
      using(withSchedulers(arrays(large))) curve ("ParArray") in { t =>
        implicit val scheduler = t._2
        findNotExistingParallel(t._1)(t._2)
      }
      if (TEST_TRIES) using(withSchedulers(hashTrieSets(large))) curve ("ParHashTrieSet") in { t =>
        implicit val scheduler = t._2
        findNotExistingParallel(t._1)(t._2)
      }
      using(withSchedulers(hashSets(large))) curve ("ParHashSet") in { t =>
        implicit val scheduler = t._2
        findNotExistingParallel(t._1)(t._2)
      }
    }

    measure method "find(sin)" in {
      using(ranges(tiny)) curve ("Sequential") in findSinSequential
      using(withSchedulers(ranges(tiny))) curve ("ParRange") in { t =>
        implicit val scheduler = t._2
        findSinParallel(t._1)(t._2)
      }
      using(withSchedulers(arrays(tiny))) curve ("ParArray") in { t =>
        implicit val scheduler = t._2
        findSinParallel(t._1)(t._2)
      }
      if (TEST_TRIES) using(withSchedulers(hashTrieSets(tiny))) curve ("ParHashTrieSet") in { t =>
        implicit val scheduler = t._2
        findSinParallel(t._1)(t._2)
      }
      using(withSchedulers(hashSets(tiny))) curve ("ParHashSet") in { t =>
        implicit val scheduler = t._2
        findSinParallel(t._1)(t._2)
      }
    }

    measure method "map(sqrt)" in {
      using(ranges(small)) curve ("Sequential") in mapSqrtSequential
      using(withSchedulers(ranges(small))) curve ("ParRange") in { t =>
        implicit val scheduler = t._2
        mapSqrtParallel(t._1)(t._2)
      }
      using(withSchedulers(arrays(small))) curve ("ParArray") in { t =>
        implicit val scheduler = t._2
        mapSqrtParallel(t._1)(t._2)
      }
      if (TEST_TRIES) using(withSchedulers(hashTrieSets(small))) curve ("ParHashTrieSets") in { t =>
        implicit val scheduler = t._2
        mapSqrtParallel(t._1)(t._2)
      }
      using(withSchedulers(hashSets(small))) curve ("ParHashSets") in { t =>
        implicit val scheduler = t._2
        mapSqrtParallel(t._1)(t._2)
      }
    }

    measure method "flatMap" in {
      using(ranges(small)) curve ("Sequential") in flatMapSequential
      using(withSchedulers(ranges(small))) curve ("ParRange") in { t =>
        implicit val scheduler = t._2
        flatMapParallel(t._1)(t._2)
      }
      using(withSchedulers(arrays(small))) curve ("ParArray") in { t =>
        implicit val scheduler = t._2
        flatMapParallel(t._1)(t._2)
      }
      if (TEST_TRIES) using(withSchedulers(hashTrieSets(small))) curve ("ParHashTrieSet") in { t =>
        implicit val scheduler = t._2
        flatMapParallel(t._1)(t._2)
      }
      using(withSchedulers(hashSets(small))) curve ("ParHashSet") in { t =>
        implicit val scheduler = t._2
        flatMapParallel(t._1)(t._2)
      }
    }

    performance of "derivative" in {

      measure method "reduce" in {
        using(ranges(large)) curve ("Sequential") in reduceSequential
        using(withSchedulers(ranges(large))) curve ("ParRange") in { t =>
          implicit val scheduler = t._2
          reduceParallel(t._1)
        }
        using(withSchedulers(arrays(large))) curve ("ParArray") in { t =>
          implicit val scheduler = t._2
          reduceParallel(t._1)
        }
        if (TEST_TRIES) using(withSchedulers(hashTrieSets(large))) curve ("ParHashTrieSet") in { t =>
          implicit val scheduler = t._2
          reduceParallel(t._1)
        }
        using(withSchedulers(hashSets(large))) curve ("ParHashSet") in { t =>
          implicit val scheduler = t._2
          reduceParallel(t._1)
        }
      }

      measure method "fold" in {
        using(ranges(large)) curve ("Sequential") in foldSequential
        using(withSchedulers(ranges(large))) curve ("ParRange") in { t =>
          implicit val scheduler = t._2
          foldParallel(t._1)(t._2)
        }
        using(withSchedulers(arrays(large))) curve ("ParArray") in { t =>
          implicit val scheduler = t._2
          foldParallel(t._1)(t._2)
        }
        if (TEST_TRIES) using(withSchedulers(hashTrieSets(large))) curve ("ParHashTrieSets") in { t =>
          implicit val scheduler = t._2
          foldParallel(t._1)(t._2)
        }
        using(withSchedulers(hashSets(large))) curve ("ParHashSets") in { t =>
          implicit val scheduler = t._2
          foldParallel(t._1)(t._2)
        }

      }

      measure method "filter(mod3)" config (
        exec.minWarmupRuns -> 80,
        exec.maxWarmupRuns -> 160) in {
          using(ranges(small)) curve ("Sequential") in filterMod3Sequential
          using(withSchedulers(ranges(small))) curve ("ParRange") in { t =>
            implicit val scheduler = t._2
            filterMod3Parallel(t._1)(t._2)
          }
          using(withSchedulers(arrays(small))) curve ("ParArray") in { t =>
            implicit val scheduler = t._2
            filterMod3Parallel(t._1)(t._2)
          }
          if (TEST_TRIES) using(withSchedulers(hashTrieSets(small))) curve ("ParHashTrieSets") in { t =>
            implicit val scheduler = t._2
            filterMod3Parallel(t._1)(t._2)
          }
        using(withSchedulers(hashSets(small))) curve ("ParHashSets") in { t =>
            implicit val scheduler = t._2
            filterMod3Parallel(t._1)(t._2)
          }
        }

      measure method "filter(cos)" in {
        using(ranges(tiny)) curve ("Sequential") in filterCosSequential
        using(withSchedulers(ranges(tiny))) curve ("ParRange") in { t =>
          implicit val scheduler = t._2
          filterCosParallel(t._1)(t._2)
        }
        using(withSchedulers(arrays(tiny))) curve ("ParArray") in { t =>
          implicit val scheduler = t._2
          filterCosParallel(t._1)(t._2)
        }
        if (TEST_TRIES) using(withSchedulers(hashTrieSets(tiny))) curve ("ParHashTrieSets") in { t =>
          implicit val scheduler = t._2
          filterCosParallel(t._1)(t._2)
        }
        using(withSchedulers(hashSets(tiny))) curve ("ParHashSets") in { t =>
          implicit val scheduler = t._2
          filterCosParallel(t._1)(t._2)
        }
      }

      measure method "foreach" in {
        using(ranges(small)) curve ("Sequential") in foreachSequential
        using(withSchedulers(ranges(small))) curve ("ParRange") in { t =>
          implicit val scheduler = t._2
          foreachParallel(t._1)(t._2)
        }
        using(withSchedulers(arrays(small))) curve ("ParArray") in { t =>
          implicit val scheduler = t._2
          foreachParallel(t._1)(t._2)
        }
        if (TEST_TRIES) using(withSchedulers(hashTrieSets(small))) curve ("ParHashTrieSet") in { t =>
          implicit val scheduler = t._2
          foreachParallel(t._1)(t._2)
        }
        using(withSchedulers(hashSets(small))) curve ("ParHashSet") in { t =>
          implicit val scheduler = t._2
          foreachParallel(t._1)(t._2)
        }
      }

      measure method ("sum") in {
        using(ranges(large)) curve ("Sequential") in sumSequential
        using(withSchedulers(ranges(large))) curve ("ParRange") in { t =>
          implicit val scheduler = t._2
          sumParallel(t._1)(t._2)
        }
        using(withSchedulers(arrays(large))) curve ("ParArray") in { t =>
          implicit val scheduler = t._2
          sumParallel(t._1)(t._2)
        }
        if (TEST_TRIES) using(withSchedulers(hashTrieSets(large))) curve ("ParHashTrieSet") in { t =>
          implicit val scheduler = t._2
          sumParallel(t._1)(t._2)
        }
        using(withSchedulers(hashSets(large))) curve ("ParHashSet") in { t =>
          implicit val scheduler = t._2
          sumParallel(t._1)(t._2)
        }
      }

      measure method ("product") in {
        using(ranges(large)) curve ("Sequential") in productSequential
        using(withSchedulers(ranges(large))) curve ("ParRange") in { t =>
          implicit val scheduler = t._2
          productParallel(t._1)(t._2)
        }
        using(withSchedulers(arrays(large))) curve ("ParArray") in { t =>
          implicit val scheduler = t._2
          productParallel(t._1)(t._2)
        }
        if (TEST_TRIES) using(withSchedulers(hashTrieSets(large))) curve ("ParHashTrieSet") in { t =>
          implicit val scheduler = t._2
          productParallel(t._1)(t._2)
        }
        using(withSchedulers(hashSets(large))) curve ("ParHashSet") in { t =>
          implicit val scheduler = t._2
          productParallel(t._1)(t._2)
        }

      }

      measure method ("count(&1==0)") in {
        using(ranges(large)) curve ("Sequential") in countSequential
        using(withSchedulers(ranges(large))) curve ("ParRange") in { t =>
          implicit val scheduler = t._2
          countParallel(t._1)(t._2)
        }
        using(withSchedulers(arrays(large))) curve ("ParArray") in { t =>
          implicit val scheduler = t._2
          countParallel(t._1)(t._2)
        }
        if (TEST_TRIES) using(withSchedulers(hashTrieSets(large))) curve ("ParHashTrieSet") in { t =>
          implicit val scheduler = t._2
          countParallel(t._1)(t._2)
        }
        using(withSchedulers(hashSets(large))) curve ("ParHashSet") in { t =>
          implicit val scheduler = t._2
          countParallel(t._1)(t._2)
        }
      }
    }

  }


}

