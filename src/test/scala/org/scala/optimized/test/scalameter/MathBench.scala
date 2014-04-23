package org.scala.optimized.test.par
package scalameter



import scala.collection.par._
import org.scalameter.api._
import scala.reflect.ClassTag
import org.scalameter.PerformanceTest.OnlineRegressionReport
import org.scalameter.{Key, KeyValue}



class MathBench extends OnlineRegressionReport with Serializable with Generators {

  /* config */

  val tiny =  100000

  /* generators */

  val opts = Context(
    exec.minWarmupRuns -> 50,
    exec.maxWarmupRuns -> 100,
    exec.benchRuns -> 48,
    exec.independentSamples -> 6,
    exec.outliers.suspectPercent -> 40,
    exec.jvmflags -> "-server -Xms3072m -Xmx3072m -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=64m -XX:+UseCondCardMark -XX:CompileThreshold=100 -Dscala.collection.parallel.range.manual_optimizations=false",
    reports.regression.noiseMagnitude -> 0.15)


  performance of "Math" config (opts) in {
    measure method "Math" config (opts) in {
      using(ranges(tiny)) curve ("sin") in {
        x =>
          var i = x.head
          val to = x.end
          var acc = 0.0
          while (i < to) {
            acc = acc + Math.sin(i)
            i = i + 1
          }
          acc
      }

      using(ranges(tiny)) curve ("sqrt") in {
        x =>
          var i = x.head
          val to = x.end
          var acc = 0.0
          while (i < to) {
            acc = acc + Math.sqrt(i)
            i = i + 1
          }
          acc
      }

      using(ranges(tiny)) curve ("tan") in {
        x =>
          var i = x.head
          val to = x.end
          var acc = 0.0
          while (i < to) {
            acc = acc + Math.tan(i)
            i = i + 1
          }
          acc
      }

      using(ranges(tiny)) curve ("baseline") in {
        x =>
          var i = x.head
          val to = x.end
          var acc = 1.0
          while (i < to) {
            acc = acc + acc
            i = i + 1
          }
          acc
      }


    }

  }

}


