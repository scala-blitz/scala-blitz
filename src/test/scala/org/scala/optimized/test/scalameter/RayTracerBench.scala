package org.scala.optimized.test.par
package scalameter



import scala.collection.par._
import org.scalameter.api._
import scala.reflect.ClassTag
import org.scala.optimized.test.examples._
import org.scalameter.PerformanceTest.OnlineRegressionReport


class RayTracerBench extends OnlineRegressionReport with Serializable with Generators {

  /* generators */

  val opts = Context(
    exec.minWarmupRuns -> 20,
    exec.maxWarmupRuns -> 100,
    exec.benchRuns -> 48,
    exec.independentSamples -> 6,
    exec.outliers.suspectPercent -> 40,
    exec.jvmflags -> "-server -Xms3072m -Xmx3072m -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=64m -XX:+UseCondCardMark -XX:CompileThreshold=100 -Dscala.collection.parallel.range.manual_optimizations=false",
    reports.regression.noiseMagnitude -> 0.15)

  val oldopts = Context(
    exec.minWarmupRuns -> 2,
    exec.maxWarmupRuns -> 4,
    exec.benchRuns -> 4,
    exec.independentSamples -> 1,
    reports.regression.noiseMagnitude -> 0.75)

  /* benchmarks */

  performance of "RayTracer" config (opts) in {

    measure method "trace" config (opts) in {

      def scenes = Gen.enumeration("scene")(RayTracer.scenes.keySet.toSeq: _*)
      
      def setupRenderer(scenes: Gen[String]) = {
        for (scene <- scenes) yield {
          val r = new RenderImage
          r.initialize(RayTracer.scenes(scene)._1.toVector, RayTracer.scenes(scene)._2, null)
          r
        }
      }
      using(withSchedulers(setupRenderer(scenes))) curve ("ScalaBlitz") in { x => 
          val sc = x._2
          val renderer = x._1
          renderer.s = sc
          renderer.drawScalaBlitz()
      }
    
      using(withTaskSupports(setupRenderer(scenes))) curve ("Old") in { x =>
        val fj = x._2
        val renderer = x._1
        renderer.fj = fj
        renderer.drawParallelOld()
      }
    }
  }

}


