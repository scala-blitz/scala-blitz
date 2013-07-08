package scala.collection.parallel
package scalameter



import org.scalameter.api._
import scala.reflect.ClassTag



class ParHashTrieSetBench extends PerformanceTest.Regression with Serializable with ParHashTrieSetSnippets with Generators {
  import Par._
  import workstealing.WorkstealingTreeScheduler
  import workstealing.WorkstealingTreeScheduler.Config

  /* config */

  def persistor = new SerializationPersistor

  val small = 50000
  val normal = 150000

  val opts = Seq(
    exec.minWarmupRuns -> 30,
    exec.maxWarmupRuns -> 60,
    exec.benchRuns -> 40,
    exec.independentSamples -> 4,
    exec.outliers.suspectPercent -> 40,
    exec.outliers.retries -> 20,
    exec.jvmflags -> "-server -Xms1536m -Xmx1536m -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=64m -XX:+UseCondCardMark -XX:CompileThreshold=100 -Dscala.collection.parallel.range.manual_optimizations=false",
    reports.regression.noiseMagnitude -> 0.15
  )

  /* tests */

  performance of "Par[immutable.HashSet]" config(opts: _*) in { 

    measure method "aggregate" in {
      using(hashTrieSets(normal)) curve("Sequential") in aggregateSequential
      using(withSchedulers(hashTrieSets(normal))) curve("Par") in { t => aggregateParallel(t._1)(t._2) }
    }

  }

}






