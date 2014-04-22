package org.scala.optimized.test.par
package scalameter


import scala.collection.optimizer._
import org.scalameter.api._



class OptimizedBlockBench extends PerformanceTest.Regression with Serializable with Generators {

  /* config */

  def persistor = new SerializationPersistor



  val opts = Context(
    exec.minWarmupRuns -> 50,
    exec.maxWarmupRuns -> 100,
    exec.benchRuns -> 30,
    exec.independentSamples -> 1,
    exec.jvmflags -> "-server -Xms3072m -Xmx3072m -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=64m -XX:+UseCondCardMark -XX:CompileThreshold=100 -Dscala.collection.parallel.range.manual_optimizations=true",
    reports.regression.noiseMagnitude -> 0.15)

  val pcopts = Context(
    exec.minWarmupRuns -> 2,
    exec.maxWarmupRuns -> 4,
    exec.benchRuns -> 4,
    exec.independentSamples -> 1,
    reports.regression.noiseMagnitude -> 0.75)

  /* benchmarks */

  performance of "Optimized[Array]" config (opts) in {

    val small = 250000
    val large = 1500000

    measure method "reduce" in {
      using(arrays(large)) curve ("collections") in { x =>
        x.reduce(_ + _)
      }
      using(arrays(large)) curve ("optimized") in { x => optimize{x.reduce(_ + _)} }
    }



    measure method "map" in {
      using(arrays(small)) curve ("collections") in { x =>
        x.map(x => x + 1)
      }
      using(arrays(small)) curve ("optimized") in { x => optimize{x.map(x => x + 1)} }
    }

    measure method "find" in {
      using(arrays(large)) curve ("collections") in { x =>
        val result: Option[Int] = x.find(_ < -1)
        result
      }
      using(arrays(large)) curve ("optimized") in { x => 
        val result: Option[Int] = optimize{x.find(_ < -1)}
        result
      }
    }

    measure method "filter" in {
      using(arrays(small)) curve ("collections") in { x =>
        x.filter(_ > 5)
      }
      using(arrays(small)) curve ("optimized") in { x => optimize{x.filter(_ > 5)} }
    }

    measure method "flatMap" in {
      using(arrays(small)) curve ("collections") in { x =>
        x.flatMap(x => List(x, x, x))
      }
      using(arrays(small)) curve ("optimized") in { x => optimize{x.flatMap(x => List(x, x, x))} }
    }

    measure method "foreach" in {
      using(arrays(large)) curve ("collections") in { x =>
        var count = 0
        x.foreach(x => count = count + 1)
      }
      using(arrays(large)) curve ("optimized") in { x => 
        var count = 0 
        optimize{x.foreach(x => count = count + 1) }
      }
    }
    
    measure method "count" in {
      using(arrays(large)) curve ("collections") in { x =>
        x.count(_ > 0)
      }
      using(arrays(large)) curve ("optimized") in { x => 
        optimize{x.count(_ > 0)}
      }
    }

    measure method "fold" in {
      using(arrays(large)) curve ("collections") in { x =>
        x.fold(0)(_ + _)
      }
      using(arrays(large)) curve ("optimized") in { x => 
        optimize{x.fold(0)(_ + _)}
      }
    }
  }

  performance of "Optimized[Range]" config (opts) in {
    val tiny = 300000
    val small = 3000000
    val large = 30000000

    measure method "reduce" in {
      using(ranges(large)) curve ("collections") in { x =>
        x.reduce(_ + _)
      }
      using(ranges(large)) curve ("optimized") in { x => optimize{x.reduce(_ + _)} }
    }

    measure method "map" in {
      using(ranges(small)) curve ("collections") in { x =>
        x.map(x => x + 1)
      }
      using(ranges(small)) curve ("optimized") in { x => optimize{x.map(x => x + 1)} }
    }

    measure method "exists" in {
      using(ranges(large)) curve ("collections") in { x =>
        x.exists(_ < Int.MinValue)
      }
      using(ranges(large)) curve ("optimized") in { x => optimize{x.exists(_ < Int.MinValue)} }
    }

    measure method "find" in {
      using(ranges(large)) curve ("collections") in { x =>
        x.find(_ < Int.MinValue)
      }
      using(ranges(large)) curve ("optimized") in { x => optimize{x.find(_ < Int.MinValue)} }
    }

    measure method "filter" in {
      using(ranges(small)) curve ("collections") in { x =>
        x.filter(_ > 5)
      }
      using(ranges(small)) curve ("optimized") in { x => optimize{x.filter(_ > 5)} }
    }

    measure method "flatMap" in {
      using(ranges(small)) curve ("collections") in { x =>
        x.flatMap(x => List(x, x, x))
      }
      using(ranges(small)) curve ("optimized") in { x => optimize{x.flatMap(x => List(x, x, x))} }
    }

    measure method "foreach" in {
      using(ranges(large)) curve ("collections") in { x =>
        var count = 0
        x.foreach(x => count = count + 1)
      }
      using(ranges(large)) curve ("optimized") in { x => 
        var count = 0 
        optimize{x.foreach(x => count = count + 1) }
      }
    }

    measure method "count" in {
      using(ranges(large)) curve ("collections") in { x =>
        x.count(_ > 0)
      }
      using(ranges(large)) curve ("optimized") in { x => 
        optimize{x.count(_ > 0)}
      }
    }

    measure method "fold" in {
      using(ranges(large)) curve ("collections") in { x =>
        x.fold(0)(_ + _)
      }
      using(ranges(large)) curve ("optimized") in { x => 
        optimize{x.fold(0)(_ + _)}
      }
    }

    measure method "aggregate(tinyCollections)" in {
      using(ranges(tiny)) curve ("collections") in { x =>
        var sum = 0
        var i = x.head
        val until = x.end
        while(i < until) {
          sum = sum + (1 to 13).aggregate(0)(_ + _, _ + _)
          i = i + 1
        }
      }
      using(ranges(tiny)) curve ("optimized") in { x => 
        optimize{
          var sum = 0
          var i = x.head
          val until = x.end
          while(i < until) {
            sum = sum + (1 to 13).aggregate(0)(_ + _, _ + _)
            i = i + 1
          }
        }
      }
    }

    measure method "ProjectEuler1" in {

      using(ranges(small)) curve ("collections") in { x =>
        x.filter(x => (x % 3 == 0)|| (x % 5 == 0)).reduce(_ + _)
      }
      using(ranges(small)) curve ("optimized") in { x => 
        optimize{x.filter(x => (x % 3 == 0)|| (x % 5 == 0)).reduce(_ + _)}
      }
    }

  }

  performance of "Optimized[ImmutableSet]" config (opts) in {

    val small = 25000
    val large = 150000

    measure method "reduce" in {
      using(hashTrieSets(large)) curve ("collections") in { x => x.reduce(_ + _) }
      using(hashTrieSets(large)) curve ("optimized") in { x => optimize{x.reduce(_ + _)} }
    }



    measure method "map" in {
      using(hashTrieSets(small)) curve ("collections") in { x => x.map(x => x) }
      using(hashTrieSets(small)) curve ("optimized") in { x => optimize{x.map(x => x)} }
    }


    measure method "exists" in {
      using(hashTrieSets(large)) curve ("collections") in { x => x.exists(_ < Int.MinValue)
      }
      using(hashTrieSets(large)) curve ("optimized") in { x => optimize{x.exists(_ < Int.MinValue)} }
    }

    measure method "find" in {
      using(hashTrieSets(large)) curve ("collections") in { x => x.find(_ < Int.MinValue) }
      using(hashTrieSets(large)) curve ("optimized") in { x => optimize{x.find(_ < Int.MinValue)} }
    }

    measure method "filter" in {
      using(hashTrieSets(small)) curve ("collections") in { x => x.filter(_ > 5) }
      using(hashTrieSets(small)) curve ("optimized") in { x => optimize{x.filter(_>5)} }
    }

    measure method "flatMap" in {
      using(hashTrieSets(small)) curve ("collections") in { x => x.flatMap(x => List(x, x, x))
      }
      using(hashTrieSets(small)) curve ("optimized") in { x => optimize{x.flatMap(x => List(x, x, x))} }
    }

    measure method "foreach" in {
      using(hashTrieSets(large)) curve ("collections") in { x =>
        var count = 0
        x.foreach(x => count = count + 1)
      }
      using(hashTrieSets(large)) curve ("optimized") in { x => 
        var count = 0 
        optimize{x.foreach(x => count = count + 1) }
      }
    }
    
    measure method "count" in {
      using(hashTrieSets(large)) curve ("collections") in { x =>
        x.count(_ > 0)
      }
      using(hashTrieSets(large)) curve ("optimized") in { x => 
        optimize{x.count(_ > 0)}
      }
    }

    measure method "fold" in {
      using(hashTrieSets(large)) curve ("collections") in { x =>
        x.fold(0)(_ + _)
      }
      using(hashTrieSets(large)) curve ("optimized") in { x => 
        optimize{x.fold(0)(_ + _)}
      }
    }
  }

}

