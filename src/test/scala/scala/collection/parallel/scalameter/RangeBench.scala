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

  val sizes = Gen.enumeration("size")(250000, 500000, 1000000, 1500000)
  val ranges = for (size <- sizes) yield 0 until size
  @transient lazy val s1 = new WorkstealingTreeScheduler.ForkJoin(new Config.Default(1))
  @transient lazy val s2 = new WorkstealingTreeScheduler.ForkJoin(new Config.Default(2))
  @transient lazy val s4 = new WorkstealingTreeScheduler.ForkJoin(new Config.Default(4))
  @transient lazy val s8 = new WorkstealingTreeScheduler.ForkJoin(new Config.Default(8))

  performance of "Par[Range]" in {

    measure method "fold" config (
      exec.minWarmupRuns -> 20,
      exec.maxWarmupRuns -> 50,
      exec.benchRuns -> 30,
      exec.independentSamples -> 6,
      exec.jvmflags -> "-XX:+UseCondCardMark") in {
        using(ranges) curve ("Sequential") in { r =>
          var i = r.head
          val to = r.last
          var sum = 0
          while (i <= to) {
            sum += i
            i += 1
          }
          if (sum == 0) ???
        }

        using(ranges) curve ("Par-1") in { r =>
          import workstealing.Ops._
          implicit val s = s1
          val pr = r.toPar
          pr.fold(0)(_ + _)
        }

        using(ranges) curve ("Par-2") in { r =>
          import workstealing.Ops._
          implicit val s = s2
          val pr = r.toPar
          pr.fold(0)(_ + _)
        }

        using(ranges) curve ("Par-4") in { r =>
          import workstealing.Ops._
          implicit val s = s4
          val pr = r.toPar
          pr.fold(0)(_ + _)
        }

        using(ranges) curve ("Par-8") in { r =>
          import workstealing.Ops._
          implicit val s = s8
          val pr = r.toPar
          pr.fold(0)(_ + _)
        }
      }

    measure method "reduce" config (
      exec.benchRuns -> 25,
      exec.independentSamples -> 5,
      exec.jvmflags -> "-XX:+UseCondCardMark") in {
        using(ranges) curve ("SequentialSum") in { r =>
          var i = r.head
          val to = r.last
          var sum = 0
          while (i <= to) {
            sum += i
            i += 1
          }
          if (sum == 0) ???
        }

        using(ranges) curve ("Par-1") in { r =>
          import workstealing.Ops._
          implicit val s = s1
          val pr = r.toPar
          pr.reduce(_ + _)
        }

        using(ranges) curve ("Par-2") in { r =>
          import workstealing.Ops._
          implicit val s = s2
          val pr = r.toPar
          pr.reduce(_ + _)
        }

        using(ranges) curve ("Par-4") in { r =>
          import workstealing.Ops._
          implicit val s = s4
          val pr = r.toPar
          pr.reduce(_ + _)
        }

        using(ranges) curve ("Par-8") in { r =>
          import workstealing.Ops._
          implicit val s = s8
          val pr = r.toPar
          pr.reduce(_ + _)
        }
      }

    measure method "aggregate" config (
      exec.benchRuns -> 25,
      exec.independentSamples -> 5,
      exec.jvmflags -> "-XX:+UseCondCardMark") in {
        using(ranges) curve ("SequentialSum") in { r =>
          var i = r.head
          val to = r.last
          var sum = 0
          while (i <= to) {
            sum += i
            i += 1
          }
          if (sum == 0) ???
        }

        using(ranges) curve ("Par-1") in { r =>
          import workstealing.Ops._
          implicit val s = s1
          val pr = r.toPar
          pr.aggregate(0)(_ + _)(_ + _)
        }

        using(ranges) curve ("Par-2") in { r =>
          import workstealing.Ops._
          implicit val s = s2
          val pr = r.toPar
          pr.aggregate(0)(_ + _)(_ + _)
        }

        using(ranges) curve ("Par-4") in { r =>
          import workstealing.Ops._
          implicit val s = s4
          val pr = r.toPar
          pr.aggregate(0)(_ + _)(_ + _)
        }

        using(ranges) curve ("Par-8") in { r =>
          import workstealing.Ops._
          implicit val s = s8
          val pr = r.toPar
          pr.aggregate(0)(_ + _)(_ + _)
        }
      }

    measure method "min" config (
      exec.benchRuns -> 25,
      exec.independentSamples -> 5,
      exec.jvmflags -> "-XX:+UseCondCardMark") in {
        using(ranges) curve ("Sequential") in { r =>
          var i = r.head
          val to = r.last
          var min = Int.MaxValue
          while (i <= to) {
            if (i < min) min = i
            i = i + 1
          }
        }

        using(ranges) curve ("Par-1") in { r =>
          import workstealing.Ops._
          implicit val s = s1
          val pr = r.toPar
          pr.min
        }

        using(ranges) curve ("Par-2") in { r =>
          import workstealing.Ops._
          implicit val s = s2
          val pr = r.toPar
          pr.min
        }

        using(ranges) curve ("Par-4") in { r =>
          import workstealing.Ops._
          implicit val s = s4
          val pr = r.toPar
          pr.min
        }

        using(ranges) curve ("Par-8") in { r =>
          import workstealing.Ops._
          implicit val s = s8
          val pr = r.toPar
          pr.min
        }
      }

    measure method "max" config (
      exec.benchRuns -> 25,
      exec.independentSamples -> 5,
      exec.jvmflags -> "-XX:+UseCondCardMark") in {
        using(ranges) curve ("Sequential") in { r =>
          var i = r.head
          val to = r.last
          var max = Int.MinValue
          while (i <= to) {
            if (i > max) max = i
            i = i + 1
          }
        }

        using(ranges) curve ("Par-1") in { r =>
          import workstealing.Ops._
          implicit val s = s1
          val pr = r.toPar
          pr.max
        }

        using(ranges) curve ("Par-2") in { r =>
          import workstealing.Ops._
          implicit val s = s2
          val pr = r.toPar
          pr.max
        }

        using(ranges) curve ("Par-4") in { r =>
          import workstealing.Ops._
          implicit val s = s4
          val pr = r.toPar
          pr.max
        }

        using(ranges) curve ("Par-8") in { r =>
          import workstealing.Ops._
          implicit val s = s8
          val pr = r.toPar
          pr.max
        }
      }

    measure method "sum" config (
      exec.benchRuns -> 25,
      exec.independentSamples -> 5,
      exec.jvmflags -> "-XX:+UseCondCardMark") in {
        using(ranges) curve ("Sequential") in { r =>
          var i = r.head
          val to = r.last
          var sum = 0
          while (i <= to) {
            sum += i
            i += 1
          }
          if (sum == 0) ???
        }

        using(ranges) curve ("Par-1") in { r =>
          import workstealing.Ops._
          implicit val s = s1
          val pr = r.toPar
          pr.sum
        }

        using(ranges) curve ("Par-2") in { r =>
          import workstealing.Ops._
          implicit val s = s2
          val pr = r.toPar
          pr.sum
        }

        using(ranges) curve ("Par-4") in { r =>
          import workstealing.Ops._
          implicit val s = s4
          val pr = r.toPar
          pr.sum
        }

        using(ranges) curve ("Par-8") in { r =>
          import workstealing.Ops._
          implicit val s = s8
          val pr = r.toPar
          pr.sum
        }
      }

    measure method "product" config (
      exec.benchRuns -> 25,
      exec.independentSamples -> 5,
      exec.jvmflags -> "-XX:+UseCondCardMark") in {
        using(ranges) curve ("Sequential") in { r =>
          var i = r.head
          val to = r.last
          var sum = 1
          while (i <= to) {
            sum *= i
            i += 1
          }
          if (sum == 1) ???
        }

        using(ranges) curve ("Par-1") in { r =>
          import workstealing.Ops._
          implicit val s = s1
          val pr = r.toPar
          pr.product
        }

        using(ranges) curve ("Par-2") in { r =>
          import workstealing.Ops._
          implicit val s = s2
          val pr = r.toPar
          pr.product
        }

        using(ranges) curve ("Par-4") in { r =>
          import workstealing.Ops._
          implicit val s = s4
          val pr = r.toPar
          pr.product
        }

        using(ranges) curve ("Par-8") in { r =>
          import workstealing.Ops._
          implicit val s = s8
          val pr = r.toPar
          pr.product
        }
      }

  }

}
