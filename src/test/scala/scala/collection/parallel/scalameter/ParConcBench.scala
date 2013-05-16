package scala.collection.parallel
package scalameter



import org.scalameter.api._
import scala.reflect.ClassTag



class ParConcBench extends PerformanceTest.Regression with Serializable {
  import Par._
  import workstealing.WorkstealingTreeScheduler
  import workstealing.WorkstealingTreeScheduler.Config

  /* config */

  def persistor = new SerializationPersistor

  /* generators */

  val concSizes = Gen.enumeration("size")(500000, 1000000, 1500000)
  val bufferSizes = Gen.enumeration("size")(10000000, 30000000, 50000000)
  val concs = for (size <- concSizes) yield {
    var conc: Conc[Int] = Conc.Zero
    for (i <- 0 until size) conc = conc <> i
    conc
  }
  val normalizedConcs = for (conc <- concs) yield conc.normalized
  val bufferConcs = for (size <- bufferSizes) yield {
    var cb = new Conc.Buffer[Int]
    for (i <- 0 until size) cb += i
    cb.result.normalized
  }
  val arrays = for (size <- bufferSizes) yield {
    (0 until size).toArray
  }
  def withArrays[T: ClassTag, CC[_] <: { def length: Int }](gen: Gen[CC[T]]): Gen[(CC[T], Array[T])] = for (v <- gen) yield {
    (v, new Array[T](v.length))
  }
  @transient lazy val s1 = new WorkstealingTreeScheduler.ForkJoin(new Config.Default(1))
  @transient lazy val s2 = new WorkstealingTreeScheduler.ForkJoin(new Config.Default(2))
  @transient lazy val s4 = new WorkstealingTreeScheduler.ForkJoin(new Config.Default(4))
  @transient lazy val s8 = new WorkstealingTreeScheduler.ForkJoin(new Config.Default(8))

  /* tests */

  performance of "Conc" config(
    exec.minWarmupRuns -> 25,
    exec.maxWarmupRuns -> 50,
    exec.benchRuns -> 40,
    exec.independentSamples -> 5,
    exec.outliers.suspectPercent -> 40,
    exec.jvmflags -> "-server -Xms1024m -Xmx1024m -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=64m -XX:+UseCondCardMark -XX:CompileThreshold=100 -Dscala.collection.parallel.range.manual_optimizations=false",
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

      using(normalizedConcs) curve("Conc-1") in { c =>
        import workstealing.Ops._
        implicit val s = s1
        c.toPar.reduce(_ + _)
      }

      using(normalizedConcs) curve("Conc-2") in { c =>
        import workstealing.Ops._
        implicit val s = s2
        c.toPar.reduce(_ + _)
      }

      using(normalizedConcs) curve("Conc-4") in { c =>
        import workstealing.Ops._
        implicit val s = s4
        c.toPar.reduce(_ + _)
      }

      using(normalizedConcs) curve("Conc-8") in { c =>
        import workstealing.Ops._
        implicit val s = s8
        c.toPar.reduce(_ + _)
      }

      using(bufferConcs) curve("Conc.Buffer-1") in { c =>
        import workstealing.Ops._
        implicit val s = s1
        c.toPar.reduce(_ + _)
      }

      using(bufferConcs) curve("Conc.Buffer-2") in { c =>
        import workstealing.Ops._
        implicit val s = s2
        c.toPar.reduce(_ + _)
      }

      using(bufferConcs) curve("Conc.Buffer-4") in { c =>
        import workstealing.Ops._
        implicit val s = s4
        c.toPar.reduce(_ + _)
      }

      using(bufferConcs) curve("Conc.Buffer-8") in { c =>
        import workstealing.Ops._
        implicit val s = s8
        c.toPar.reduce(_ + _)
      }
    }

    measure method "copyToArray" in {
      using(withArrays(arrays)) curve("Array") in { case (src, dest) =>
        Array.copy(src, 0, dest, 0, src.length)
      }

      using(withArrays(bufferConcs)) curve("Conc.Buffer-1") in { case (c, dest) =>
        import workstealing.Ops._
        implicit val s = s1
        c.toPar.copyToArray(dest, 0, c.length)
      }

      using(withArrays(bufferConcs)) curve("Conc.Buffer-2") in { case (c, dest) =>
        import workstealing.Ops._
        implicit val s = s2
        c.toPar.copyToArray(dest, 0, c.length)
      }

      using(withArrays(bufferConcs)) curve("Conc.Buffer-4") in { case (c, dest) =>
        import workstealing.Ops._
        implicit val s = s4
        c.toPar.copyToArray(dest, 0, c.length)
      }

      using(withArrays(bufferConcs)) curve("Conc.Buffer-8") in { case (c, dest) =>
        import workstealing.Ops._
        implicit val s = s8
        c.toPar.copyToArray(dest, 0, c.length)
      }
    }

  }

}






