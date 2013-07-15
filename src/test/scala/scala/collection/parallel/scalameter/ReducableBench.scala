package scala.collection.parallel
package scalameter



import org.scalameter.api._
import scala.reflect.ClassTag
import Par._
import workstealing.Ops._



class ReducableBench extends PerformanceTest.Regression with Serializable with ReducableSnippets with Generators {

  /* config */

  def persistor = new SerializationPersistor
  val tiny = 300000
  val small = 3000000
  val large = 30000000

  /* generators */

  val opts = Seq(
    exec.minWarmupRuns -> 25,
    exec.maxWarmupRuns -> 50,
    exec.benchRuns -> 48,
    exec.independentSamples -> 6,
    exec.outliers.suspectPercent -> 40,
    exec.jvmflags -> "-server -Xms3072m -Xmx3072m -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=64m -XX:+UseCondCardMark -XX:CompileThreshold=100 -Dscala.collection.parallel.range.manual_optimizations=false",
    reports.regression.noiseMagnitude -> 0.15)

  val oldopts = Seq(
    exec.minWarmupRuns -> 2,
    exec.maxWarmupRuns -> 4,
    exec.benchRuns -> 4,
    exec.independentSamples -> 1,
    reports.regression.noiseMagnitude -> 0.75)

  implicit def range2Reducable(r: Range)(implicit ctx: workstealing.WorkstealingTreeScheduler): Reducable[Int] = par2zippable(r.toPar)

  /* benchmarks */

  performance of "Reduable[Range]" config (opts: _*) in {

    measure method "reduce" in {
      using(ranges(large)) curve ("Sequential") in reduceSequential
      using(withSchedulers(ranges(large))) curve ("Par") in { t =>
        implicit val scheduler = t._2
        reduceParallel(t._1)
      }
    }

    measure method "mapReduce" in {
      using(ranges(large)) curve ("Sequential") in mapReduceSequential
      using(withSchedulers(ranges(large))) curve ("Par") in { t =>
        implicit val scheduler = t._2
        mapReduceParallel(t._1)(t._2)
      }
    }

    measure method "aggregate" in {
      using(ranges(large)) curve ("Sequential") in aggregateSequential
      using(withSchedulers(ranges(small))) curve ("Par") in { t =>
        implicit val scheduler = t._2
        aggregateParallel(t._1)(t._2)
      }
    }

    measure method "find" in {
      using(ranges(large)) curve ("Sequential") in findNotExistingSequential
      using(withSchedulers(ranges(large))) curve ("Par") in { t =>
        implicit val scheduler = t._2
        findNotExistingParallel(t._1)(t._2)
      }
    }

    measure method "find(sin)" in {
      using(ranges(tiny)) curve ("Sequential") in findSinSequential
      using(withSchedulers(ranges(tiny))) curve ("Par") in { t =>
        implicit val scheduler = t._2
        findSinParallel(t._1)(t._2)
      }
    }

    measure method "map(sqrt)" in {
      using(ranges(small)) curve ("Sequential") in mapSqrtSequential
      using(withSchedulers(ranges(small))) curve ("Par") in { t =>
        implicit val scheduler = t._2
        mapSqrtParallel(t._1)(t._2)
      }
    }

    /*measure method "filter(mod3)" config (
      exec.minWarmupRuns -> 80,
      exec.maxWarmupRuns -> 160
    ) in {
      using(ranges(small)) curve ("Sequential") in filterMod3Sequential
      using(withSchedulers(ranges(small))) curve("Par") in { t => filterMod3Parallel(t._1)(t._2) }
      performance of "old" config(oldopts: _*) in {
        using(ranges(tiny)) curve ("ParArray") in { _.par.filter(_ % 3 == 0) }
        }
      }
     
    measure method "filter(cos)" in {
      using(arrays(tiny)) curve ("Sequential") in filterCosSequential
      using(withSchedulers(arrays(tiny))) curve("Par") in { t => filterCosParallel(t._1)(t._2) }
    }

    measure method "flatMap" in {
      using(arrays(small)) curve ("Sequential") in flatMapSequential
      using(withSchedulers(arrays(small))) curve("Par") in { t => flatMapParallel(t._1)(t._2) }
    }*/

    performance of "derivative" in {
      measure method "fold" in {
        using(ranges(large)) curve ("Sequential") in foldSequential
        using(withSchedulers(ranges(small))) curve ("Par") in { t =>
          implicit val scheduler = t._2
          foldParallel(t._1)(t._2)
        }
      }

      measure method "foreach" in {
        using(ranges(small)) curve ("Sequential") in foreachSequential
        using(withSchedulers(ranges(small))) curve ("Par") in { t =>
          implicit val scheduler = t._2
          foreachParallel(t._1)(t._2)
        }
      }

      measure method ("sum") in {
        using(ranges(large)) curve ("Sequential") in sumSequential
        using(withSchedulers(ranges(small))) curve ("Par") in { t =>
          implicit val scheduler = t._2
          sumParallel(t._1)(t._2)
        }
      }

      measure method ("product") in {
        using(ranges(large)) curve ("Sequential") in productSequential
        using(withSchedulers(ranges(small))) curve ("Par") in { t =>
          implicit val scheduler = t._2
          productParallel(t._1)(t._2)
        }
      }

      measure method ("count(&1==0)") in {
        using(ranges(large)) curve ("Sequential") in countSequential
        using(withSchedulers(ranges(small))) curve ("Par") in { t =>
          implicit val scheduler = t._2
          countParallel(t._1)(t._2)
        }
      }
    }

  }

}

