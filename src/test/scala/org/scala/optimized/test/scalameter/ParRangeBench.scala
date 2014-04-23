package org.scala.optimized.test.par
package scalameter


import scala.collection.par._
import org.scalameter.api._
import org.scalameter.PerformanceTest.OnlineRegressionReport


class ParRangeBench extends OnlineRegressionReport with Serializable with ParRangeSnippets with Generators {

  /* config */

  val tiny = 300000
  val small = 3000000
  val large = 30000000

  val single = Gen.single("sizes")(500000)

  val opts = Context(
    exec.minWarmupRuns -> 50,
    exec.maxWarmupRuns -> 100,
    exec.benchRuns -> 30,
    exec.independentSamples -> 6,
    exec.jvmflags -> "-server -Xms3072m -Xmx3072m -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=64m -XX:+UseCondCardMark -XX:CompileThreshold=100 -Dscala.collection.parallel.range.manual_optimizations=false",
    reports.regression.noiseMagnitude -> 0.15)

  val pcopts = Context(
    exec.minWarmupRuns -> 2,
    exec.maxWarmupRuns -> 4,
    exec.benchRuns -> 4,
    exec.independentSamples -> 1,
    reports.regression.noiseMagnitude -> 0.75)

  @volatile var global = 0
  /* benchmarks */

  performance of "Par[Range]" config (opts) in {

    measure method "reduce" in {
      using(ranges(large)) curve ("Sequential") in reduceSequential
      using(withSchedulers(ranges(large))) curve ("Par") in { t => reduceParallel(t._1)(t._2) }
      performance of "old" config (pcopts) in {
        using(ranges(small)) curve ("pc") in { _.par.reduce(_ + _) }
      }
    }

    measure method "mapReduce" in {
      using(ranges(large)) curve ("Sequential") in mapReduceSequential
//      using(ranges(large)) curve ("SequentialCollections") in mapReduceSequentialCollections
      using(withSchedulers(ranges(large))) curve ("Par") in { t => mapReduceParallel(t._1)(t._2) }
      using(withSchedulers(ranges(small))) curve ("ParNotFused") in { t => mapReduceParallelNotFused(t._1)(t._2) }
    }

    measure method "reduce(step)" in {
      def workOnElement(e:Int, size:Int) = {
        var until = 1
        if (e > size * 0.97) until = 200000
        var acc = 1
        var i = 1
        while (i < until) {
          acc *= i
          i += 1
          }
          acc
        }

      using(ranges(single)) curve ("Sequential") in { r =>
        var i = r.head
        val end = r.last
        var sum = 0
        while(i!= end) {
          sum = sum + workOnElement(i, end)
          i = i + 1
        }
        global = sum
     }

      using(withSchedulers(ranges(single))) curve ("Par") in { t => 
        implicit val scheduler = t._2
        val sz = t._1.length
        t._1.toPar.mapReduce(x=>workOnElement(x,sz))(_+_)
      }

    }

    measure method "reduce(sqrt)" in {
      def workOnElement(e:Int, size:Int) = {
          var acc = 1;
          var i = 0;
          val until = math.sqrt(e)
          while(i<until) {
            acc = (acc * i) / 3;
            i = i + 1
          }
          acc
      }
      using(ranges(single)) curve ("Sequential") in { r =>

        var i = r.head
        val end = r.last
        var sum = 0
        while(i!= end) {
          sum = sum + workOnElement(i, 3000000)
          i = i + 1
        }
     }

      using(withSchedulers(ranges(single))) curve ("Par") in { t => 
        implicit val scheduler = t._2
        val sz = 3000000
        t._1.toPar.mapReduce(x=>workOnElement(x,sz))(_+_)
      }

    }

    measure method "reduce(exp)" in {
      def workOnElement(e:Int, size:Int) = {
        val until: Int = 1<< (e/30000);
        var acc = 1
        var i = 1
          while(i<until) {
          acc *= i
          i += 1
          }
          acc
      }
      using(ranges(single)) curve ("Sequential") in { r =>
        var i = r.head
        val end = r.last
        var sum = 0
        while(i!= end) {
          sum = sum + workOnElement(i, end)
          i = i + 1
        }
     }

      using(withSchedulers(ranges(single))) curve ("Par") in { t => 
        implicit val scheduler = t._2
        val sz = t._1.length
        t._1.toPar.mapReduce(x=>workOnElement(x,sz))(_+_)
      }

    }


    measure method "for3Generators" in {
      val list = List(2, 3, 5)
      using(ranges(tiny / 5)) curve ("Sequential") in { r=>
       for {
          x <- r
          y <- list
          z <- list
        } yield {
          x * y * z
        }
      }
      using(withSchedulers(ranges(tiny / 5))) curve ("Par-non-fused") in { t =>
        implicit val scheduler = t._2
        val r = t._1
        for {
          x <- r.toPar
          y <- list
          z <- list
        } yield {
           x * y * z
        }
      }
      using(withSchedulers(ranges(tiny / 5))) curve ("Par") in { t =>
        implicit val scheduler = t._2
        val r = t._1
        for {
          x <- r.toPar
          y <- list
          z <- list
        } yield {
          x * y * z
        }: @unchecked
      }
    }

    measure method "aggregate" in {
      using(ranges(large)) curve ("Sequential") in aggregateSequential
      using(withSchedulers(ranges(large))) curve ("Par") in { t => aggregateParallel(t._1)(t._2) }
      performance of "old" config (pcopts) in {
        using(ranges(small)) curve ("pc") in { r =>
          r.par.aggregate(0)(_ + _, _ + _)
        }
      }
    }

    measure method "groupAggregate" in {
      using(ranges(tiny)) curve ("Sequential") in {x=> x.groupBy(_%15).map{x=>(x._1,x._2.sum)}}
      using(withSchedulers(ranges(tiny))) curve ("Par") in { t => groupMapAggregateParallel(t._1)(t._2)}
    }

    measure method "find" in {
      using(ranges(large)) curve ("Sequential") in findNotExistingSequential
      using(withSchedulers(ranges(large))) curve ("Par") in { t => findNotExistingParallel(t._1)(t._2) }
    }

    measure method "copyToArray" in {
      using(withArrays(ranges(small))) curve ("Sequential") in copyAllToArraySequential
      using(withSchedulers(withArrays(ranges(small)))) curve ("Par") in { t => copyAllToArrayParallel(t._1)(t._2) }
    }

    measure method "map(sqrt)" in {
      using(ranges(tiny)) curve ("Sequential") in mapSqrtSequential
      using(withSchedulers(ranges(tiny))) curve ("Par") in { t => mapSqrtParallel(t._1)(t._2) }
    }

    measure method "flatMap" in {
      using(ranges(tiny)) curve ("Sequential") in flatMapSequential
      using(withSchedulers(ranges(tiny))) curve ("Par") in { t => flatMapParallel(t._1)(t._2) }
    }

    performance of "derivative" in {
      measure method "fold" in {
        using(ranges(large)) curve ("Sequential") in foldSequential
        using(withSchedulers(ranges(large))) curve ("Par") in { t => foldParallel(t._1)(t._2) }
      }

      measure method "filter(mod3)" config (
        exec.minWarmupRuns -> 80,
        exec.maxWarmupRuns -> 160) in {
          using(ranges(small)) curve ("Sequential") in filterMod3Sequential
          using(withSchedulers(ranges(small))) curve ("Par") in { t => filterMod3Parallel(t._1)(t._2) }
        }

      measure method "filter(cos)" in {
        using(ranges(tiny)) curve ("Sequential") in filterCosSequential
        using(withSchedulers(ranges(tiny))) curve ("Par") in { t => filterCosParallel(t._1)(t._2) }
      }

      measure method "foreach" in {
        using(ranges(small)) curve ("Sequential") in foreachSequential
        using(withSchedulers(ranges(small))) curve ("Par") in { t => foreachParallel(t._1)(t._2) }
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
