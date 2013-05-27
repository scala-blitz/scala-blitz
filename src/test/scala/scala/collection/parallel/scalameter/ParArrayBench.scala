package scala.collection.parallel
package scalameter



import org.scalameter.api._
import scala.reflect.ClassTag



class ParArrayBench extends PerformanceTest.Regression with Serializable {
  import Par._
  import workstealing.WorkstealingTreeScheduler
  import workstealing.WorkstealingTreeScheduler.Config

  /* config */

  def persistor = new SerializationPersistor

  /* generators */

  val arraySizes = Gen.enumeration("size")(10000000, 30000000, 50000000)
  val arrays = for (size <- arraySizes) yield (0 until size).toArray
  @transient lazy val s1 = new WorkstealingTreeScheduler.ForkJoin(new Config.Default(1))
  @transient lazy val s2 = new WorkstealingTreeScheduler.ForkJoin(new Config.Default(2))
  @transient lazy val s4 = new WorkstealingTreeScheduler.ForkJoin(new Config.Default(4))
  @transient lazy val s8 = new WorkstealingTreeScheduler.ForkJoin(new Config.Default(8))

  /* tests */

  performance of "Par[Array]" config(
    exec.minWarmupRuns -> 25,
    exec.maxWarmupRuns -> 50,
    exec.benchRuns -> 48,
    exec.independentSamples -> 6,
    exec.outliers.suspectPercent -> 40,
    exec.jvmflags -> "-server -Xms1536m -Xmx1536m -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=64m -XX:+UseCondCardMark -XX:CompileThreshold=100 -Dscala.collection.parallel.range.manual_optimizations=false",
    reports.regression.noiseMagnitude -> 0.15
  ) in {

    measure method "reduce" in {
      using(arrays) curve("Array") in { a =>
        var i = 0
        val until = a.length
        var sum = 0
        while (i < until) {
          sum += a(i)
          i += 1
        }
        if (sum == 0) ???
      }

      using(arrays) curve("Par-1") in { a =>
        import workstealing.Ops._
        implicit val s = s1
        a.toPar.reduce(_ + _)
      }

      using(arrays) curve("Par-2") in { a =>
        import workstealing.Ops._
        implicit val s = s2
        a.toPar.reduce(_ + _)
      }

      using(arrays) curve("Par-4") in { a =>
        import workstealing.Ops._
        implicit val s = s4
        a.toPar.reduce(_ + _)
      }

      using(arrays) curve("Par-8") in { a =>
        import workstealing.Ops._
        implicit val s = s8
        a.toPar.reduce(_ + _)
      }
    }

  }

}






