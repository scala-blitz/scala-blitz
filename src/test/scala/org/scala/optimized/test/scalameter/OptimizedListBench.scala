package org.scala.optimized.test.scalameter

import org.scalameter.api._
import org.scalameter.PerformanceTest.OnlineRegressionReport
import org.scala.optimized.test.par.scalameter.Generators
import scala.collection.optimizer._


class OptimizedListBench extends OnlineRegressionReport with Serializable with Generators {

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

  performance of "Optimized[List]" config (opts) in {

    measure method "reduce" in {
      using(lists(normal)) curve ("Sequential") in {
        x => x.reduce(_ + _)
      }

      using(lists(normal)) curve ("Optimized") in {
        x => x.opt.reduce(_ + _)
      }
    }

    measure method "aggregate" in {
      using(lists(normal)) curve ("Sequential") in {
        x => x.aggregate(0)(_ + _, _ + _)
      }

      using(lists(normal)) curve ("Optimized") in {
        x => x.opt.aggregate(0)(_ + _)(_ + _)
      }
    }
    /*
        measure method "filter" in {
          using(lists(normal)) curve ("Sequential") in {
            x => x.filter(_ % 3 == 0)
          }

          using(lists(normal)) curve ("Optimized") in {
            x => x.opt.filter(_ % 3 == 0)
          }
        }

        measure method "find" in {
          using(lists(normal)) curve ("Sequential") in {
            x => x.find(x => x == -1)
          }

          using(lists(normal)) curve ("Optimized") in {
            x => x.opt.find(x => x == -1)
          }
        }
    */
    performance of "derivative" in {
      /*
      measure method "count" in {
        using(lists(normal)) curve ("Sequential") in {
          x => x.count(x => x % 2 == 1)
        }

        using(lists(normal)) curve ("Optimized") in {
          x => x.opt.count(x => x % 2 == 1)
        }
      }*/

      measure method "sum" in {
        using(lists(normal)) curve ("Sequential") in {
          x => x.sum
        }

        using(lists(normal)) curve ("Optimized") in {
          x => x.opt.sum
        }
      }

      measure method "product" in {
        using(lists(normal)) curve ("Sequential") in {
          x => x.product
        }

        using(lists(normal)) curve ("Optimized") in {
          x => x.opt.product
        }
      }

      measure method "min" in {
        using(lists(normal)) curve ("Sequential") in {
          x => x.min
        }

        using(lists(normal)) curve ("Optimized") in {
          x => x.opt.min
        }
      }

      measure method "max" in {
        using(lists(normal)) curve ("Sequential") in {
          x => x.max
        }

        using(lists(normal)) curve ("Optimized") in {
          x => x.opt.max
        }
      }
    }
  }


}

