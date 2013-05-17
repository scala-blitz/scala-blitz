package scala.collection.parallel
package scalameter



import org.scalameter.api._



class ParRangeBench extends PerformanceTest.Regression with Serializable {
  import Par._
  import workstealing.WorkstealingTreeScheduler
  import workstealing.WorkstealingTreeScheduler.Config

  /* config */

  def persistor = new SerializationPersistor

  /* generators */

  val sizes = Gen.enumeration("size")(50000000, 100000000, 150000000)
  val ranges = for (size <- sizes) yield 0 until size
  @transient lazy val s1 = new WorkstealingTreeScheduler.ForkJoin(new Config.Default(1))
  @transient lazy val s2 = new WorkstealingTreeScheduler.ForkJoin(new Config.Default(2))
  @transient lazy val s4 = new WorkstealingTreeScheduler.ForkJoin(new Config.Default(4))
  @transient lazy val s8 = new WorkstealingTreeScheduler.ForkJoin(new Config.Default(8))

  val opts = Seq[(String, Any)](
    exec.minWarmupRuns -> 20,
    exec.maxWarmupRuns -> 50,
    exec.benchRuns -> 30,
    exec.independentSamples -> 6,
    exec.jvmflags -> "-server -Xms1024m -Xmx1024m -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=64m -XX:+UseCondCardMark -XX:CompileThreshold=100 -Dscala.collection.parallel.range.manual_optimizations=false",
    reports.regression.noiseMagnitude -> 0.15
  )

  val pcopts = Seq[(String, Any)](
    exec.minWarmupRuns -> 2,
    exec.maxWarmupRuns -> 4,
    exec.benchRuns -> 4,
    exec.independentSamples -> 1,
    reports.regression.noiseMagnitude -> 0.75
  )

  performance of "Par[Range]" in {
    measure method "fold" config (opts: _*) in {
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

      performance of "extra" config (pcopts: _*) in {
        using(ranges) curve ("pc") in { r =>
          val sum = r.par.sum
          if (sum == 0) ???
        }
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

    measure method "reduce" config (opts: _*) in {
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

      performance of "extra" config (pcopts: _*) in {
        using(ranges) curve ("pc") in { r =>
          r.par.reduce(_ + _)
        }
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

    measure method "aggregate" config (opts: _*) in {
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

      performance of "extra" config (pcopts: _*) in {
        using(ranges) curve ("pc") in { r =>
          r.par.aggregate(0)(_ + _, _ + _)
        }
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

    measure method "min" config (opts: _*) in {
      using(ranges) curve ("Sequential") in { r =>
        var i = r.head
        val to = r.last
        var min = Int.MaxValue
        while (i <= to) {
          if (i < min) min = i
          i = i + 1
        }
      }

      performance of "extra" config (pcopts: _*) in {
        using(ranges) curve ("pc") in { r =>
          r.par.min
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

    measure method "max" config (opts: _*) in {
      using(ranges) curve ("Sequential") in { r =>
        var i = r.head
        val to = r.last
        var max = Int.MinValue
        while (i <= to) {
          if (i > max) max = i
          i = i + 1
        }
      }

      performance of "extra" config (pcopts: _*) in {
        using(ranges) curve ("pc") in { r =>
          r.par.max
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

    measure method "sum" config (opts: _*) in {
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

      performance of "extra" config (pcopts: _*) in {
        using(ranges) curve ("pc") in { r =>
          r.par.sum
        }
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

    measure method "product" config (opts: _*) in {
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

      performance of "extra" config (pcopts: _*) in {
        using(ranges) curve ("pc") in { r =>
          r.par.product
        }
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

    measure method "find" config (opts: _*) in {
      using(ranges) curve ("Sequential") config(
        exec.benchRuns -> 30,
        exec.independentSamples -> 3,
        reports.regression.noiseMagnitude -> 0.75
      ) in { r =>
        var i = r.head
        val to = r.last
        var found = false
        var result: Option[Int] = None
        while (i <= to && !found) {
          if (i == to) {
            found = true
            result = Some(i)
          }
          i += 1
        }
        if (result.isEmpty) ???

      }

      performance of "extra" config (pcopts: _*) in {
        using(ranges) curve ("pc") in { r =>
          val mx = r.last + 1
          r.par.find(_ == mx)
        }
      }

      using(ranges) curve ("Par-1") in { r =>
        import workstealing.Ops._
        implicit val s = s1
        val pr = r.toPar
        val mx = r.last + 1
        pr.find(_ == mx)
      }

      using(ranges) curve ("Par-2") in { r =>
        import workstealing.Ops._
        implicit val s = s2
        val pr = r.toPar
        val mx = r.last + 1
        pr.find(_ == mx)
      }

      using(ranges) curve ("Par-4") in { r =>
        import workstealing.Ops._
        implicit val s = s4
        val pr = r.toPar
        val mx = r.last + 1
        pr.find(_ == mx)
      }

      using(ranges) curve ("Par-8") in { r =>
        import workstealing.Ops._
        implicit val s = s8
        val pr = r.toPar
        val mx = r.last + 1
        pr.find(_ == mx)
      }
    }

    measure method "exists" config (opts: _*) in {
      using(ranges) curve ("Sequential") in { r =>
        var i = r.head
        val to = r.last
        var result = false
        while (i <= to && (!result)) {
          if (to == i) {
            result = true
          }
          i += 1
        }
        if (!result) ???
      }

      performance of "extra" config (pcopts: _*) in {
        using(ranges) curve ("pc") in { r =>
          val mx = r.last + 1
          r.par.exists(_ == mx)
        }
      }

      using(ranges) curve ("Par-1") in { r =>
        import workstealing.Ops._
        implicit val s = s1
        val pr = r.toPar
        val mx = r.last + 1
        pr.exists(_ == mx)
      }

      using(ranges) curve ("Par-2") in { r =>
        import workstealing.Ops._
        implicit val s = s2
        val pr = r.toPar
        val mx = r.last + 1
        pr.exists(_ == mx)
      }

      using(ranges) curve ("Par-4") in { r =>
        import workstealing.Ops._
        implicit val s = s4
        val pr = r.toPar
        val mx = r.last + 1
        pr.exists(_ == mx)
      }

      using(ranges) curve ("Par-8") in { r =>
        import workstealing.Ops._
        implicit val s = s8
        val pr = r.toPar
        val mx = r.last + 1
        pr.exists(_ == mx)
      }
    }

    measure method "forall" config (opts: _*) in {
      using(ranges) curve ("Sequential") in { r =>
        var i = r.head
        val to = r.last
        var result = true
        while (i <= to && result) {
          result = i < Int.MaxValue
          i += 1
        }
        if (!result) ???
      }

      performance of "extra" config (pcopts: _*) in {
        using(ranges) curve ("pc") in { r =>
          r.par.forall(_ < Int.MaxValue)
        }
      }

      using(ranges) curve ("Par-1") in { r =>
        import workstealing.Ops._
        implicit val s = s1
        val pr = r.toPar
        pr.forall(_ < Int.MaxValue)
      }

      using(ranges) curve ("Par-2") in { r =>
        import workstealing.Ops._
        implicit val s = s2
        val pr = r.toPar
        pr.forall(_ < Int.MaxValue)
      }

      using(ranges) curve ("Par-4") in { r =>
        import workstealing.Ops._
        implicit val s = s4
        val pr = r.toPar
        pr.forall(_ < Int.MaxValue)
      }

      using(ranges) curve ("Par-8") in { r =>
        import workstealing.Ops._
        implicit val s = s8
        val pr = r.toPar
        pr.forall(_ < Int.MaxValue)
      }
    }

  }

}
