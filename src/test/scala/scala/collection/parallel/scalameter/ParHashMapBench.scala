package scala.collection.parallel
package scalameter



import org.scalameter.api._
import scala.reflect.ClassTag



class ParHashMapBench extends PerformanceTest.Regression with Serializable with ParHashMapSnippets with Generators {
  import Par._
  import workstealing.WorkstealingTreeScheduler
  import workstealing.WorkstealingTreeScheduler.Config

  /* config */

  def persistor = new SerializationPersistor

  val small = 100000
  val normal = 300000

  val opts = Seq(
    exec.minWarmupRuns -> 30,
    exec.maxWarmupRuns -> 60,
    exec.benchRuns -> 40,
    exec.independentSamples -> 5,
    exec.outliers.suspectPercent -> 40,
    exec.outliers.retries -> 20,
    exec.jvmflags -> "-server -Xms1536m -Xmx1536m -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=64m -XX:+UseCondCardMark -XX:CompileThreshold=100 -Dscala.collection.parallel.range.manual_optimizations=false",
    reports.regression.noiseMagnitude -> 0.15
  )

  /* tests */

  performance of "Par[HashMap]" config(opts: _*) in { 

    measure method "aggregate" in {
      using(hashMaps(normal)) curve("Sequential") in aggregateSequential
      using(withSchedulers(hashMaps(normal))) curve("Par") in { t => aggregateParallel(t._1)(t._2) }
    }

    measure method "filter" in {
      using(hashMaps(small)) curve("Sequential") in filterSequential
      using(withSchedulers(hashMaps(small))) curve("Par") in { t => filterParallel(t._1)(t._2) }
    }

    performance of "derivative" in {
      measure method "count" in {
        using(hashMaps(normal)) curve("Sequential") in countSequential
        using(withSchedulers(hashMaps(normal))) curve("Par") in { t => countParallel(t._1)(t._2) }
      }
    }

  }

}






