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

  val from = 300000

  val opts = Seq(
    exec.minWarmupRuns -> 25,
    exec.maxWarmupRuns -> 50,
    exec.benchRuns -> 48,
    exec.independentSamples -> 6,
    exec.outliers.suspectPercent -> 40,
    exec.jvmflags -> "-server -Xms1536m -Xmx1536m -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=64m -XX:+UseCondCardMark -XX:CompileThreshold=100 -Dscala.collection.parallel.range.manual_optimizations=false",
    reports.regression.noiseMagnitude -> 0.15
  )

  /* tests */

  performance of "Par[HashMap]" config(opts: _*) in { 

    measure method "aggregate" in {
      using(hashMaps(from)) curve("Sequential") in aggregateSequential
      using(withSchedulers(hashMaps(from))) curve("Par") in { t => aggregateParallel(t._1)(t._2) }
    }

  }

}






