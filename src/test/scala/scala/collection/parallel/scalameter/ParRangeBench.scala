package scala.collection.parallel
package scalameter



import org.scalameter.api._



class ParRangeBench extends PerformanceTest.Regression with Serializable with ParRangeSnippets with Generators {

  /* config */

  def persistor = new SerializationPersistor

  val tiny  =  300000
  val small = 3000000
  val large = 30000000

  val opts = Seq(
    exec.minWarmupRuns -> 35,
    exec.maxWarmupRuns -> 90,
    exec.benchRuns -> 30,
    exec.independentSamples -> 6,
    exec.jvmflags -> "-server -Xms1024m -Xmx1024m -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=64m -XX:+UseCondCardMark -XX:CompileThreshold=100 -Dscala.collection.parallel.range.manual_optimizations=false",
    reports.regression.noiseMagnitude -> 0.15
  )

  val pcopts = Seq(
    exec.minWarmupRuns -> 2,
    exec.maxWarmupRuns -> 4,
    exec.benchRuns -> 4,
    exec.independentSamples -> 1,
    reports.regression.noiseMagnitude -> 0.75
  )

  /* benchmarks */

  performance of "Par[Range]" config(opts: _*) in {
    measure method "reduce" in {
      using(ranges(large)) curve ("Sequential") in reduceSequential
      using(withSchedulers(ranges(large))) curve ("Par") in { t => reduceParallel(t._1)(t._2) }
      performance of "old" config (pcopts: _*) in {
        using(ranges(small)) curve ("pc") in { _.par.reduce(_ + _) }
      }
    }

    measure method "aggregate" in {
      using(ranges(large)) curve ("Sequential") in aggregateSequential
      using(withSchedulers(ranges(large))) curve ("Par") in { t => aggregateParallel(t._1)(t._2) }
      performance of "old" config (pcopts: _*) in {
        using(ranges(small)) curve ("pc") in { r =>
          r.par.aggregate(0)(_ + _, _ + _)
        }
      }
    }

    measure method "find" in {
      using(ranges(large)) curve ("Sequential") in findNotExistingSequential
      using(withSchedulers(ranges(large))) curve ("Par") in { t => findNotExistingParallel(t._1)(t._2) }
    }

    measure method "copyToArray" in {
      using(withArrays(ranges(large))) curve ("Sequential") in copyAllToArraySequential
      using(withSchedulers(withArrays(ranges(large)))) curve ("Par") in { t => copyAllToArrayParallel(t._1)(t._2) }
    }

    measure method "map(sqrt)" in {
      using(ranges(small)) curve ("Sequential") in mapSqrtSequential
      using(withSchedulers(ranges(small))) curve ("Par") in { t => mapSqrtParallel(t._1)(t._2) }
    }

    measure method "filter(mod3)" config (
      exec.minWarmupRuns -> 80,
      exec.maxWarmupRuns -> 160
    ) in {
      using(ranges(small)) curve ("Sequential") in filterMod3Sequential
      using(withSchedulers(ranges(small))) curve("Par") in { t => filterMod3Parallel(t._1)(t._2) }
      }
     
    measure method "filter(cos)" in {
      using(ranges(tiny)) curve ("Sequential") in filterCosSequential
      using(withSchedulers(ranges(tiny))) curve("Par") in { t => filterCosParallel(t._1)(t._2) }
    }

    measure method "flatMap" in {
      using(ranges(small)) curve ("Sequential") in flatMapSequential
      using(withSchedulers(ranges(small))) curve("Par") in { t => flatMapParallel(t._1)(t._2) }
      }
  
    performance of "derivative" in {
      measure method "fold" in {
        using(ranges(large)) curve ("Sequential") in foldSequential
        using(withSchedulers(ranges(large))) curve ("Par") in { t => foldParallel(t._1)(t._2) }
      }

      measure method "min" in {
        using(ranges(large)) curve ("Sequential") in minSequential
        using(withSchedulers(ranges(large))) curve ("Par") in { t => minParallel(t._1)(t._2) }
      }

      measure method "max" in {
        using(ranges(large)) curve ("Sequential") in maxSequential
        using(withSchedulers(ranges(large))) curve ("Par") in { t => maxParallel(t._1)(t._2) }
      }

      measure method "sum" in {
        using(ranges(large)) curve ("Sequential") in sumSequential
        using(withSchedulers(ranges(large))) curve ("Par") in { t => sumParallel(t._1)(t._2) }
      }

      measure method "product" in {
        using(ranges(large)) curve ("Sequential") in productSequential
        using(withSchedulers(ranges(large))) curve ("Par") in { t => productParallel(t._1)(t._2) }
      }

      measure method "count" in {
        using(ranges(large)) curve ("Sequential") in countSequential
        using(withSchedulers(ranges(large))) curve ("Par") in { t => countParallel(t._1)(t._2) }
      }

      measure method "exists" in {
        using(ranges(large)) curve ("Sequential") in existsSequential
        using(withSchedulers(ranges(large))) curve ("Par") in { t => existsParallel(t._1)(t._2) }
      }
    }
   }


}
