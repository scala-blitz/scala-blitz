package scala.collection.parallel
package scalameter



import org.scalameter.api._
import scala.reflect.ClassTag



class ParArrayBench extends PerformanceTest.Regression with Serializable with ParArraySnippets with Generators {

  /* config */

  def persistor = new SerializationPersistor
  val tiny = 100000
  val small = 1000000
  val large = 10000000

  /* generators */

  val opts = Seq(
    exec.minWarmupRuns -> 50,
    exec.maxWarmupRuns -> 100,
    exec.benchRuns -> 48,
    exec.independentSamples -> 6,
    exec.outliers.suspectPercent -> 40,
    exec.jvmflags -> "-server -Xms3072m -Xmx3072m -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=64m -XX:+UseCondCardMark -XX:CompileThreshold=100 -Dscala.collection.parallel.range.manual_optimizations=false",
    reports.regression.noiseMagnitude -> 0.15
  )

  val oldopts = Seq(
    exec.minWarmupRuns -> 2,
    exec.maxWarmupRuns -> 4,
    exec.benchRuns -> 4,
    exec.independentSamples -> 1,
    reports.regression.noiseMagnitude -> 0.75
  )

  /* benchmarks */

  performance of "Par[Array]" config(opts: _*) in {

    measure method "reduce" in {
      using(arrays(large)) curve ("Sequential") in reduceSequential
      using(withSchedulers(arrays(large))) curve ("Par") in { t => reduceParallel(t._1)(t._2) }
      performance of "old" config(oldopts: _*) in {
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
      exec.maxWarmupRuns -> 160
    ) in {
      using(arrays(small)) curve ("Sequential") in filterMod3Sequential
      using(withSchedulers(arrays(small))) curve("Par") in { t => filterMod3Parallel(t._1)(t._2) }
      performance of "old" config(oldopts: _*) in {
        using(arrays(tiny)) curve ("ParArray") in { _.par.filter(_ % 3 == 0) }
        }
      }
     
    measure method "filter(cos)" in {
      using(arrays(tiny)) curve ("Sequential") in filterCosSequential
      using(withSchedulers(arrays(tiny))) curve("Par") in { t => filterCosParallel(t._1)(t._2) }
    }

    measure method "flatMap" in {
      using(arrays(small)) curve ("Sequential") in flatMapSequential
      using(withSchedulers(arrays(small))) curve("Par") in { t => flatMapParallel(t._1)(t._2) }
    }

    performance of "derivative" in {
      measure method "fold(product)" in {
        using(arrays(small)) curve ("Sequential") in foldProductSequential
        using(withSchedulers(arrays(small))) curve("Par") in { t => foldProductParallel(t._1)(t._2) }
      }

      measure method "foreach" in {
        using(arrays(small)) curve ("Sequential") in foreachSequential
        using(withSchedulers(arrays(small))) curve ("Par") in { t => foreachParallel(t._1)(t._2) }
      }
  
      measure method ("sum") in {
        using(arrays(small)) curve ("Sequential") in sumSequential
        using(withSchedulers(arrays(small))) curve("Par") in { t => sumParallel(t._1)(t._2) }
      }
  
      measure method ("product") in {
        using(arrays(small)) curve ("Sequential") in productSequential
        using(withSchedulers(arrays(small))) curve("Par") in { t => productParallel(t._1)(t._2) }
      }
  
      measure method ("count(squareMod3)") in {
        using(arrays(small)) curve ("Sequential") in countSquareMod3Sequential
        using(withSchedulers(arrays(small))) curve("Par") in { t => countSquareMod3Parallel(t._1)(t._2) }
      }
    }

  }

}



