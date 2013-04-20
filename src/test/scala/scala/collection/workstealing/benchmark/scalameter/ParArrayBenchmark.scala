package scala.collection.workstealing.benchmark.scalameter

import org.scalameter.{ Reporter, Gen, PerformanceTest }
import org.scalameter.persistence.SerializationPersistor
import org.scalameter.api._
import java.awt.Color
import org.scalameter.reporting.{ ChartReporter, HtmlReporter, RegressionReporter }
import collection.workstealing._
import collection.workstealing.benchmark.MathUtils

/**
 * Comparison with original parallel collections and sequential ones.
 *
 * @author Dmitry Petrashko
 * @version 0.1
 */
object ParArrayBenchmark extends PerformanceTest.Regression {

  /* configuration */

  lazy val persistor = new SerializationPersistor

  lazy val colorsTestSample = List(Color.RED, Color.GREEN, Color.BLUE) //, new Color(14, 201, 198), new Color(212, 71, 11))

  /* inputs */

  val sizes = Gen.range("size")(300000, 1500000, 300000)

  val ranges = for (sz <- sizes) yield (0 until sz)
  val origParArrays = for (range <- ranges) yield range.toArray.par
  val newParArrays = for (range <- ranges) yield new  ParArray(range.toArray, Workstealing.DefaultConfig)

  /* tests  */


  override val reporter: Reporter = org.scalameter.Reporter.Composite(
    new RegressionReporter(RegressionReporter.Tester.ConfidenceIntervals(), RegressionReporter.Historian.ExponentialBackoff()),
    new HtmlReporter(HtmlReporter.Renderer.Info(),
      HtmlReporter.Renderer.Chart(ChartReporter.ChartFactory.TrendHistogram(), "Trend Histogram", colorsTestSample)))

  val testTarget = newParArrays
  val testTargetOrig = origParArrays

  performance of "Arrays" config (exec.independentSamples -> 8, exec.benchRuns -> 50, exec.jvmflags -> "-Xms512M -Xmx512M") in {

    measure method "time" in {

      using(testTarget) curve ("foreach") in {
        case array =>
          @volatile var found = false
          array.foreach(x => if ((x & 0xfffff) == 0) found = true)
          assert(found)
      }

      using(testTargetOrig) curve ("foreachOrig") in {
        case array =>
          @volatile var found = false
          array.foreach(x => if ((x & 0xfffff) == 0) found = true)
          assert(found)
      }

      using(testTarget) curve ("fold") in {
        case array =>
          val res = array.fold(0)(_ + _)
      }

      using(testTargetOrig) curve ("foldOrig") in {
        case array => val res = array.fold(0)(_ + _)
      }

      using(testTarget) curve ("reduce") in {
        case array => array.reduce(_ + _)
      }

      using(testTargetOrig) curve ("reduceOrig") in {
        case array => array.reduce(_ + _)
      }


      using(testTarget) curve ("aggregate") in {
        case array => val res = array.aggregate(0)(_ + _)(_ + _)
      }

      using(testTargetOrig) curve ("aggregateOrig") in {
        case array => val res = array.aggregate(0)(_ + _, _ + _)
      }

      using(testTarget) curve ("sum") in {
        case array => val res = array.sum
      }

      using(testTargetOrig) curve ("sumOrig") in {
        case array => val res = array.sum
      }

      using(testTarget) curve ("count") in {
        case array => array.count(x => (x & 0x3) == 0)
      }

      using(testTargetOrig) curve ("countOrig") in {
        case array => val res = array.count(x => (x & 0x3) == 0)
      }
      /** FIXME: Broken. Tried 5 times, always awaits for more than an hour. Stacktrace



"Finalizer" - Thread t@3
   java.lang.Thread.State: WAITING
	at java.lang.Object.wait(Native Method)
	- waiting on <b8be647> (a java.lang.ref.ReferenceQueue$Lock)
	at java.lang.ref.ReferenceQueue.remove(ReferenceQueue.java:135)
	at java.lang.ref.ReferenceQueue.remove(ReferenceQueue.java:151)
	at java.lang.ref.Finalizer$FinalizerThread.run(Finalizer.java:177)

   Locked ownable synchronizers:
	- None

"ForkJoinPool-1-worker-2" - Thread t@9
   java.lang.Thread.State: WAITING
	at sun.misc.Unsafe.park(Native Method)
	- parking to wait for <6286fa12> (a scala.concurrent.forkjoin.ForkJoinPool)
	at scala.concurrent.forkjoin.ForkJoinPool.scan(ForkJoinPool.java:1594)
	at scala.concurrent.forkjoin.ForkJoinPool.runWorker(ForkJoinPool.java:1478)
	at scala.concurrent.forkjoin.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:104)

   Locked ownable synchronizers:
	- None

"Reference Handler" - Thread t@2
   java.lang.Thread.State: WAITING
	at java.lang.Object.wait(Native Method)
	- waiting on <dbb453d> (a java.lang.ref.Reference$Lock)
	at java.lang.Object.wait(Object.java:503)
	at java.lang.ref.Reference$ReferenceHandler.run(Reference.java:133)

   Locked ownable synchronizers:
	- None

"Signal Dispatcher" - Thread t@4
   java.lang.Thread.State: RUNNABLE

   Locked ownable synchronizers:
	- None

"main" - Thread t@1
   java.lang.Thread.State: WAITING
	at java.lang.Object.wait(Native Method)
	- waiting on <1beaed6> (a scala.collection.workstealing.Workstealing$Ptr)
	at java.lang.Object.wait(Object.java:503)
	at scala.collection.workstealing.Workstealing$class.joinWorkFJ(Workstealing.scala:223)
	at scala.collection.workstealing.ParArray$ArrayCombiner.joinWorkFJ(ParArray.scala:240)
	at scala.collection.workstealing.Workstealing$class.invokeParallelOperation(Workstealing.scala:411)
	at scala.collection.workstealing.ParArray$ArrayCombiner.invokeParallelOperation(ParArray.scala:240)
	at scala.collection.workstealing.ParArray$ArrayCombiner.result(ParArray.scala:364)
	at scala.collection.workstealing.ParArray$ArrayCombiner.result(ParArray.scala:240)
	at scala.collection.workstealing.benchmark.scalameter.ParArrayBenchmark$$anonfun$1$$anonfun$apply$mcV$sp$1$$anonfun$apply$mcV$sp$16.apply(ParArrayBenchmark.scala:112)
	at scala.collection.workstealing.benchmark.scalameter.ParArrayBenchmark$$anonfun$1$$anonfun$apply$mcV$sp$1$$anonfun$apply$mcV$sp$16.apply(ParArrayBenchmark.scala:112)
	at org.scalameter.Executor$Measurer$IgnoringGC$$anonfun$1.apply$mcD$sp(Executor.scala:171)
	at org.scalameter.Executor$Measurer$IgnoringGC$$anonfun$1.apply(Executor.scala:169)
	at org.scalameter.Executor$Measurer$IgnoringGC$$anonfun$1.apply(Executor.scala:169)
	at org.scalameter.utils.package$$anon$2.apply(package.scala:26)
	at sun.reflect.GeneratedMethodAccessor1.invoke(Unknown Source)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:601)
	at org.scalameter.Executor$Measurer$IgnoringGC.measure(Executor.scala:169)
	at org.scalameter.PerformanceTest$Regression$$anon$1.org$scalameter$Executor$Measurer$OutlierElimination$$super$measure(PerformanceTest.scala:85)
	at org.scalameter.Executor$Measurer$OutlierElimination$class.measure(Executor.scala:251)
	at org.scalameter.PerformanceTest$Regression$$anon$1.org$scalameter$Executor$Measurer$Noise$$super$measure(PerformanceTest.scala:85)
	at org.scalameter.Executor$Measurer$Noise$class.measure(Executor.scala:302)
	at org.scalameter.PerformanceTest$Regression$$anon$1.measure(PerformanceTest.scala:85)
	at org.scalameter.execution.SeparateJvmsExecutor$$anonfun$sample$1$1$$anonfun$1.apply(SeparateJvmsExecutor.scala:59)
	at org.scalameter.execution.SeparateJvmsExecutor$$anonfun$sample$1$1$$anonfun$1.apply(SeparateJvmsExecutor.scala:55)
	at scala.collection.Iterator$$anon$11.next(Iterator.scala:328)
	at scala.collection.Iterator$class.foreach(Iterator.scala:727)
	at scala.collection.AbstractIterator.foreach(Iterator.scala:1157)
	at scala.collection.generic.Growable$class.$plus$plus$eq(Growable.scala:48)
	at scala.collection.mutable.ArrayBuffer.$plus$plus$eq(ArrayBuffer.scala:103)
	at scala.collection.mutable.ArrayBuffer.$plus$plus$eq(ArrayBuffer.scala:47)
	at scala.collection.TraversableOnce$class.to(TraversableOnce.scala:273)
	at scala.collection.AbstractIterator.to(Iterator.scala:1157)
	at scala.collection.TraversableOnce$class.toBuffer(TraversableOnce.scala:265)
	at scala.collection.AbstractIterator.toBuffer(Iterator.scala:1157)
	at org.scalameter.execution.SeparateJvmsExecutor$$anonfun$sample$1$1.apply(SeparateJvmsExecutor.scala:62)
	at org.scalameter.execution.SeparateJvmsExecutor$$anonfun$sample$1$1.apply(SeparateJvmsExecutor.scala:39)
	at org.scalameter.execution.package$Main$.mainMethod(package.scala:73)
	at org.scalameter.execution.package$Main$.main(package.scala:68)
	at org.scalameter.execution.package$Main.main(package.scala)

   Locked ownable synchronizers:
	- None
        */

      using(testTarget) curve ("find") in {
        case array => array.find(x => 10 * x < 0)
      }

      using(testTargetOrig) curve ("findOrig") in {
        case array => array.find(x => 10 * x < 0)
      }

      using(testTarget) curve ("filter") in {
        case array => array.filter(x => true)
      }

      using(testTargetOrig) curve ("filterOrig") in {
        case array => array.filter(x => true)
      }

      using(testTarget) curve ("filterExpensive") in {
        case array => array.filter(x => MathUtils.taylor(x) > Math.E)
      }

      using(testTargetOrig) curve ("filterExpensiveOrig") in {
        case array => array.filter(x => MathUtils.taylor(x) > Math.E)
      }

    }

  }

}
