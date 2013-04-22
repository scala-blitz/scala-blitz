package scala.collection.workstealing.benchmark.scalameter

import org.scalameter.{ Reporter, Gen, PerformanceTest }
import org.scalameter.persistence.SerializationPersistor
import org.scalameter.api._
import java.awt.Color
import org.scalameter.reporting.{ ChartReporter, HtmlReporter, RegressionReporter }
import collection.workstealing.{ Workstealing, ParRange }
import collection.workstealing.benchmark.MathUtils

/**
 * Comparison with original parallel collections and sequential ones.
 *
 * @author Dmitry Petrashko
 * @version 0.1
 */
object ParRangeBenchmark extends PerformanceTest.Regression {

  /* configuration */

  lazy val persistor = new SerializationPersistor

  lazy val colorsTestSample = List(Color.RED, Color.GREEN, Color.BLUE) //, new Color(14, 201, 198), new Color(212, 71, 11))

  /* inputs */

  val sizes = Gen.range("size")(300000, 1500000, 300000)

  val ranges = for (sz <- sizes) yield (0 until sz)
  val origParRanges = for (range <- ranges) yield range.par
  val newParRanges = for (range <- ranges) yield new ParRange(range, Workstealing.DefaultConfig)

  /* tests  */

  val seriesNames = Array("a", "b", "3", "4")

  override val reporter: Reporter = org.scalameter.Reporter.Composite(
    new RegressionReporter(RegressionReporter.Tester.ConfidenceIntervals(), RegressionReporter.Historian.ExponentialBackoff()),
    new HtmlReporter(HtmlReporter.Renderer.Info(),
      HtmlReporter.Renderer.Chart(ChartReporter.ChartFactory.TrendHistogram(), "Trend Histogram", colorsTestSample)))

  val testTarget = newParRanges
  val testTargetOrig = origParRanges

  performance of "ranges" config (exec.independentSamples -> 8, exec.benchRuns -> 50, exec.jvmflags -> "-Xms256M -Xmx256M") in {

    measure method "time" in {

      using(testTarget) curve ("foreach") in {
        case range =>
          @volatile var found = false
          range.foreach(x => if ((x & 0xfffff) == 0) found = true)
          assert(found)
      }

      using(testTargetOrig) curve ("foreachOrig") in {
        case range =>
          @volatile var found = false
          range.foreach(x => if ((x & 0xfffff) == 0) found = true)
          assert(found)
      }

      using(testTarget) curve ("fold") in {
        case range => val res = range.fold(0)(_ + _)
      }

      using(testTargetOrig) curve ("foldOrig") in {
        case range => val res = range.fold(0)(_ + _)
      }

      using(testTarget) curve ("reduce") in {
        case range => range.reduce(_ + _)
      }

      using(testTarget) curve ("aggregate") in {
        case range => val res = range.aggregate(0)(_ + _)(_ + _)
      }

      using(testTargetOrig) curve ("aggregateOrig") in {
        case range => val res = range.aggregate(0)(_ + _, _ + _)
      }

      using(testTarget) curve ("sum") in {
        case range => val res = range.sum
      }

      using(testTargetOrig) curve ("sumOrig") in {
        case range => val res = range.sum
      }

      using(testTarget) curve ("count") in {
        case range => range.count(x => (x & 0x3) == 0)
      }

      using(testTargetOrig) curve ("countOrig") in {
        case range => val res = range.count(x => (x & 0x3) == 0)
      }

      using(testTarget) curve ("find") in {
        case range => range.find(x => 10 * x < 0)
      }

      using(testTargetOrig) curve ("findOrig") in {
        case range => range.find(x => 10 * x < 0)
      }

      using(testTarget) curve ("filter") in {
        case range => range.filter2(x => true)
      }

      using(testTarget) curve ("filterExpensive") in {
        case range => range.filter2(x => MathUtils.taylor(x) > Math.E)
      }

    }

  }

}
