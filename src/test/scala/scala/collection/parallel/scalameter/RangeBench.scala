package scala.collection.parallel
package scalameter



import org.scalameter.api._



class RangeBench extends PerformanceTest.Regression with Serializable {
  import Par._
  import workstealing.WorkstealingTreeScheduler
  import workstealing.WorkstealingTreeScheduler.Config

  /* config */

  def persistor = new SerializationPersistor

  /* generators */

  val sizes = Gen.enumeration("size")(25000000, 50000000, 100000000, 150000000)
  val ranges = for (size <- sizes) yield 0 until size
  @transient lazy val s1 = new WorkstealingTreeScheduler.ForkJoin(new Config.Default(1))

  performance of "Par[Range]" in {

    measure method "fold" config(
      exec.benchRuns -> 10,
      exec.independentSamples -> 4,
      exec.jvmflags -> "-XX:+UseCondCardMark"
    ) in {
      using(ranges) curve("Sequential") in { r =>
        var i = r.head
        val to = r.last
        var sum = 0
        while (i <= to) {
          sum += i
          i += 1
        }
        if (sum == 0) ???
      }

      using(ranges) curve("Par-1") in { r =>
        import workstealing.Ops._
        implicit val s = s1
        val pr = r.toPar
        pr.fold(0)(_ + _)
      }
    }

  }

}