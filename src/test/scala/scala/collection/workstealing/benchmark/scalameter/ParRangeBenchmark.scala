package scala.collection.workstealing.benchmark.scalameter

import org.scalameter.{Reporter, Gen, PerformanceTest}
import org.scalameter.persistence.SerializationPersistor
import org.scalameter.api._
import java.awt.Color
import org.scalameter.reporting.{ChartReporter, HtmlReporter, RegressionReporter}
import collection.workstealing.{Workstealing, ParRange}
import collection.workstealing.benchmark.MathUtils

/**
 * Author: Dmitry Petrashko
 * Date: 27.03.13
 *
 */

object RangeBenchmark extends PerformanceTest.Regression {

  /* configuration */


  lazy val persistor = new SerializationPersistor

  lazy val colorsTestSample = List(Color.RED, Color.GREEN, Color.BLUE) //, new Color(14, 201, 198), new Color(212, 71, 11))

  /* inputs */

  val sizes = Gen.range("size")(300000, 1500000, 300000)


  val ranges = for (sz <- sizes) yield (0 until sz)
  val origParRanges = for (range <- ranges) yield range.par
  val newParRanges = for (range <- ranges) yield new ParRange(range, Workstealing.DefaultConfig)
  val origParRangesAndTargetArrays = for (range <- ranges) yield (range.par, new Array[Int](range.size))
  val newParRangesAndTargetArrays = for (range <- ranges)
  yield (new ParRange(range, Workstealing.DefaultConfig), new Array[Int](range.size))

  /* tests */

  val seriesNames = Array("a", "b", "3", "4")


  override val reporter: Reporter = org.scalameter.Reporter.Composite(
    new RegressionReporter(RegressionReporter.Tester.ConfidenceIntervals(), RegressionReporter.Historian.ExponentialBackoff()),
    new HtmlReporter(HtmlReporter.Renderer.Info(),
      HtmlReporter.Renderer.Chart(ChartReporter.ChartFactory.TrendHistogram(), "Trend Histogram", colorsTestSample))
  )

  type rangeLike = {def foreach[U](f: Int => U): Unit

    def fold[U >: Int](z: U)(op: (U, U) => U): U

    def reduce[U >: Int](op: (U, U) => U): U

  //TODO: it has different signatures
//    def aggregate[S](z: => S)(combop: (S, S) => S)(seqop: (S, Int) => S): S

    def sum[U >: Int](implicit num: Numeric[U]): U

    def product[U >: Int](implicit num: Numeric[U]): U

    def count(p: Int => Boolean): Int

    def find(p: Int => Boolean): Option[Int]
    def copyToArray[U >: Int](arr: Array[U], start: Int, len: Int): Unit
    def filter(p: Int => Boolean): AnyRef
  }

  type rangeLikeGenerator = Gen[AnyRef]

  def testRangeLike(testTarget: rangeLikeGenerator, name: String) {
    performance of name config(exec.independentSamples -> 1, exec.benchRuns -> 15) in {

      measure method "time" in {


        val testTargetWithArrays = origParRangesAndTargetArrays

        using(testTarget) curve ("foreach") in {
          case range: rangeLike =>
            @volatile var found = false
            range.foreach(x => if ((x & 0xfffff) == 0) found = true)
          case _ => sys.error("failed to get range")
        }

        using(testTarget) curve ("fold") in {
          case range: rangeLike => range.fold(0)(_ + _)
          case _ => sys.error("failed to get range")
        }

        using(testTarget) curve ("reduce") in {
          case range: rangeLike => range.reduce(_ + _)
          case _ => sys.error("failed to get range")
        }

//        using(testTarget) curve ("aggregate") in {
//          case range: rangeLike => range.aggregate(0)(_ + _)(_ + _)
//          case _ => sys.error("failed to get range")
//        }

        using(testTarget) curve ("sum") in {
          case range: rangeLike => range.sum
          case _ => sys.error("failed to get range")
        }

        using(testTarget) curve ("count") in {
          case range: rangeLike => range.count(x => (x & 0x3) == 0)
          case _ => sys.error("failed to get range")
        }

        using(testTarget) curve ("find") in {
          case range: rangeLike => range.find(x => 10 * x < 0)
          case _ => sys.error("failed to get range")
        }

        using(testTarget) curve ("filter") in {
          case range: rangeLike => range.filter(x => true)
          case _ => sys.error("failed to get range")
        }

        //        using(testTargetWithArrays) curve ("copyToArray") in {
        //          range => range.copyToArray(arr, 0, arr.length)
        //        }

        using(testTarget) curve ("filterExpensive") in {
          case range: rangeLike => range.filter(x => MathUtils.taylor(x) > Math.E)
          case _ => sys.error("failed to get range")
        }

      }
    }

  }


  testRangeLike(origParRanges.asInstanceOf[rangeLikeGenerator], "origParRange")



//  testRangeLike(newParRanges.asInstanceOf[rangeLikeGenerator], "newsdaParRange")


}
