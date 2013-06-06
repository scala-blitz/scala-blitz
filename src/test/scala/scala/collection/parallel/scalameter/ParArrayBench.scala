package scala.collection.parallel
package scalameter



import org.scalameter.api._
import scala.reflect.ClassTag



class ParArrayBench extends PerformanceTest.Regression with Serializable {
  val TEST_DERIVATIVE_METHODS = sys.props.get("scala.collection.parallel.scalameter.ParArrayBench.test_derivative_methods").map(_.toBoolean).getOrElse(true)
  import Par._
  import workstealing.WorkstealingTreeScheduler
  import workstealing.WorkstealingTreeScheduler.Config

  /* config */

  def persistor = new SerializationPersistor

  /* generators */

  val tinySizes = Gen.enumeration("size")(100000, 300000, 500000)
  val tinyArrays = for (size <- tinySizes) yield (0 until size).toArray
  val smallSizes = Gen.enumeration("size")(1000000, 3000000, 5000000)
  val smallArrays = for (size <- smallSizes) yield (0 until size).toArray
  val largeSizes = Gen.enumeration("size")(10000000, 30000000, 50000000)
  val largeArrays = for (size <- largeSizes) yield (0 until size).toArray
  @transient lazy val s1 = new WorkstealingTreeScheduler.ForkJoin(new Config.Default(1))
  @transient lazy val s2 = new WorkstealingTreeScheduler.ForkJoin(new Config.Default(2))
  @transient lazy val s4 = new WorkstealingTreeScheduler.ForkJoin(new Config.Default(4))
  @transient lazy val s8 = new WorkstealingTreeScheduler.ForkJoin(new Config.Default(8))

  val pcopts = Seq[(String, Any)](
    exec.minWarmupRuns -> 2,
    exec.maxWarmupRuns -> 4,
    exec.benchRuns -> 4,
    exec.independentSamples -> 1,
    reports.regression.noiseMagnitude -> 0.75)

  /* utils */

  class SimpleBuffer[@specialized(Int) T: ClassTag] {
    var narr = new Array[T](4)
    var narrpos = 0
    def pushback(x: T): this.type = {
      if (narrpos < narr.length) {
        narr(narrpos) = x
        narrpos += 1
        this
      } else {
        val newnarr = new Array[T](narr.length * 2)
        Array.copy(narr, 0, newnarr, 0, narr.length)
        narr = newnarr
        pushback(x)
      }
    }
  }

  /* tests */

  performance of "Par[Array]" config (
    exec.minWarmupRuns -> 25,
    exec.maxWarmupRuns -> 50,
    exec.benchRuns -> 48,
    exec.independentSamples -> 6,
    exec.outliers.suspectPercent -> 40,
    exec.jvmflags -> "-server -Xms3072m -Xmx3072m -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=64m -XX:+UseCondCardMark -XX:CompileThreshold=100 -Dscala.collection.parallel.range.manual_optimizations=false",
    reports.regression.noiseMagnitude -> 0.15
  ) in {

    if (TEST_DERIVATIVE_METHODS) { //derivative of aggregate
      measure method "fold*" in {
        using(smallArrays) curve ("Sequential") in { a =>
          var i = 0
          val until = a.length
          var sum = 1
          while (i < until) {
            sum *= a(i)
            i += 1
          }
          sum
        }

        using(smallArrays) curve ("Par-1") in { arr =>
          import workstealing.Ops._
          implicit val s = s1
          val pa = arr.toPar
          pa.fold(1)(_ * _)
        }

        using(smallArrays) curve ("Par-2") in { arr =>
          import workstealing.Ops._
          implicit val s = s2
          val pa = arr.toPar
          pa.fold(1)(_ * _)
        }

        using(smallArrays) curve ("Par-4") in { arr =>
          import workstealing.Ops._
          implicit val s = s4
          val pa = arr.toPar
          pa.fold(1)(_ * _)
        }

        using(smallArrays) curve ("Par-8") in { arr =>
          import workstealing.Ops._
          implicit val s = s8
          val pa = arr.toPar
          pa.fold(1)(_ * _)
        }
      }
    }

    measure method "reduce*" in {
      using(smallArrays) curve ("Sequential") in { a =>
        var i = 0
        val until = a.length
        var sum = 1
        while (i < until) {
          sum *= a(i)
          i += 1
        }
        sum
      }

      using(smallArrays) curve ("Par-1") in { a =>
        import workstealing.Ops._
        implicit val s = s1
        a.toPar.reduce(_ * _)
      }

      using(smallArrays) curve ("Par-2") in { a =>
        import workstealing.Ops._
        implicit val s = s2
        a.toPar.reduce(_ * _)
      }

      using(smallArrays) curve ("Par-4") in { a =>
        import workstealing.Ops._
        implicit val s = s4
        a.toPar.reduce(_ + _)
      }

      using(smallArrays) curve ("Par-8") in { a =>
        import workstealing.Ops._
        implicit val s = s8
        a.toPar.reduce(_ * _)
      }
    }

    measure method "aggregate*" in {
      using(smallArrays) curve ("Sequential") in { arr =>
        var i = 0
        val until = arr.length
        var sum = 0
        while (i < until) {
          sum += arr(i) * arr(i)
          i += 1
        }
        sum
      }

      using(smallArrays) curve ("Par-1") in { arr =>
        import workstealing.Ops._
        implicit val s = s1
        val pa = arr.toPar
        pa.aggregate(0)((x, y) => x + y * y)(_ + _)
      }

      using(smallArrays) curve ("Par-2") in { arr =>
        import workstealing.Ops._
        implicit val s = s2
        val pa = arr.toPar
        pa.aggregate(0)((x, y) => x + y * y)(_ + _)
      }

      using(smallArrays) curve ("Par-4") in { arr =>
        import workstealing.Ops._
        implicit val s = s4
        val pa = arr.toPar
        pa.aggregate(0)((x, y) => x + y * y)(_ + _)
      }

      using(smallArrays) curve ("Par-8") in { arr =>
        import workstealing.Ops._
        implicit val s = s8
        val pa = arr.toPar
        pa.aggregate(0)((x, y) => x + y * y)(_ + _)
      }
    }

    if (TEST_DERIVATIVE_METHODS) { //derivative of reduce

      measure method "sum" in {
        using(largeArrays) curve ("Sequential") in { arr =>
          var i = 0
          val until = arr.length
          var sum = 0
          while (i < until) {
            sum += arr(i)
            i += 1
          }
          if (sum == 0) ???
        }

        using(largeArrays) curve ("Par-1") in { arr =>
          import workstealing.Ops._
          implicit val s = s1
          val pa = arr.toPar
          pa.sum
        }

        using(largeArrays) curve ("Par-2") in { arr =>
          import workstealing.Ops._
          implicit val s = s2
          val pa = arr.toPar
          pa.sum
        }

        using(largeArrays) curve ("Par-4") in { arr =>
          import workstealing.Ops._
          implicit val s = s4
          val pa = arr.toPar
          pa.sum
        }

        using(largeArrays) curve ("Par-8") in { arr =>
          import workstealing.Ops._
          implicit val s = s8
          val pa = arr.toPar
          pa.sum
        }
      }

      measure method "product" in {
        using(largeArrays) curve ("Sequential") in { arr =>
          var i = 0
          val until = arr.length
          var sum = 1
          while (i < until) {
            sum *= arr(i)
            i += 1
          }
          if (sum == 1) ???
        }

        using(largeArrays) curve ("Par-1") in { arr =>
          import workstealing.Ops._
          implicit val s = s1
          val pa = arr.toPar
          pa.product
        }

        using(largeArrays) curve ("Par-2") in { arr =>
          import workstealing.Ops._
          implicit val s = s2
          val pa = arr.toPar
          pa.product
        }

        using(largeArrays) curve ("Par-4") in { arr =>
          import workstealing.Ops._
          implicit val s = s4
          val pa = arr.toPar
          pa.product
        }

        using(largeArrays) curve ("Par-8") in { arr =>
          import workstealing.Ops._
          implicit val s = s8
          val pa = arr.toPar
          pa.product
        }
      }
    }

    if (TEST_DERIVATIVE_METHODS) { //derivative of aggregate
      measure method "count*" in {
        using(smallArrays) curve ("Sequential") in { arr =>
          var i = 0
          val until = arr.length
          var count = 0
          while (i < until) {
            if ((arr(i) * arr(i)) % 3 == 1) { count += 1 }
            i += 1
          }
          count
        }

        using(smallArrays) curve ("Par-1") in { arr =>
          import workstealing.Ops._
          implicit val s = s1
          val pa = arr.toPar
          pa.count(x => (x * x) % 3 == 1)
        }

        using(smallArrays) curve ("Par-2") in { arr =>
          import workstealing.Ops._
          implicit val s = s2
          val pa = arr.toPar
          pa.count(x => (x * x) % 3 == 1)
        }

        using(smallArrays) curve ("Par-4") in { arr =>
          import workstealing.Ops._
          implicit val s = s4
          val pa = arr.toPar
          pa.count(x => (x * x) % 3 == 1)
        }

        using(smallArrays) curve ("Par-8") in { arr =>
          import workstealing.Ops._
          implicit val s = s8
          val pa = arr.toPar
          pa.count(x => (x * x) % 3 == 1)
        }
      }
    }

    measure method "find*" in {
      using(smallArrays) curve ("Sequential") in { arr =>
        var i = 0
        val to = arr.length
        var found = false
        var result = -1
        while (i < to && !found) {
          if (2.0 == math.sin(arr(i))) {
            found = true
            result = i
          }
          i += 1
        }
        result
      }

      using(smallArrays) curve ("Par-1") in { arr =>
        import workstealing.Ops._
        implicit val s = s1
        val pr = arr.toPar
        pr.find(x => math.sin(x) == 2.0)
      }

      using(smallArrays) curve ("Par-2") in { arr =>
        import workstealing.Ops._
        implicit val s = s2
        val pr = arr.toPar
        pr.find(x => math.sin(x) == 2.0)
      }

      using(smallArrays) curve ("Par-4") in { arr =>
        import workstealing.Ops._
        implicit val s = s4
        val pr = arr.toPar
        pr.find(x => math.sin(x) == 2.0)
      }

      using(smallArrays) curve ("Par-8") in { arr =>
        import workstealing.Ops._
        implicit val s = s8
        val pr = arr.toPar
        pr.find(x => math.sin(x) == 2.0)
      }
    }

    measure method "map" in {
      using(smallArrays) curve ("Sequential") in { arr =>
        var i = 0
        val until = arr.length
        val narr = new Array[Int](until)
        while (i < until) {
          narr(i) = math.sqrt(i).toInt
          i += 1
        }
        narr
      }

      using(smallArrays) curve ("Par-1") in { arr =>
        import workstealing.Ops._
        implicit val s = s1
        val pa = arr.toPar
        pa.map(x => math.sqrt(x).toInt)
      }

      using(smallArrays) curve ("Par-2") in { arr =>
        import workstealing.Ops._
        implicit val s = s2
        val pa = arr.toPar
        pa.map(x => math.sqrt(x).toInt)
      }

      using(smallArrays) curve ("Par-4") in { arr =>
        import workstealing.Ops._
        implicit val s = s4
        val pa = arr.toPar
        pa.map(x => math.sqrt(x).toInt)
      }

      using(smallArrays) curve ("Par-8") in { arr =>
        import workstealing.Ops._
        implicit val s = s8
        val pa = arr.toPar
        pa.map(x => math.sqrt(x).toInt)
      }
    }

    measure method "filter(mod3)" config (
      exec.minWarmupRuns -> 80,
      exec.maxWarmupRuns -> 160
    ) in {
      using(smallArrays) curve ("Sequential") in { arr =>
        var i = 0
        val until = arr.length
        val ib = new SimpleBuffer[Int]
        while (i < until) {
          if (i % 3 == 0) ib.pushback(i)
          i += 1
        }
      }

      using(smallArrays) curve ("Par-1") in { arr =>
        import workstealing.Ops._
        implicit val s = s1
        val pa = arr.toPar
        pa.filter(_ % 3 == 0)
      }

      using(smallArrays) curve ("Par-2") in { arr =>
        import workstealing.Ops._
        implicit val s = s2
        val pa = arr.toPar
        pa.filter(_ % 3 == 0)
      }

    }

    def filterSqrt(x: Int) = math.cos(x).toInt > 0

    measure method "filter(cos)" in {
      using(tinyArrays) curve ("Sequential") in { arr =>
        var i = 0
        val until = arr.length
        val ib = new SimpleBuffer[Int]
        while (i < until) {
          if (filterSqrt(i)) ib.pushback(i)
          i += 1
        }
      }

      using(tinyArrays) curve ("Par-1") in { arr =>
        import workstealing.Ops._
        implicit val s = s1
        val pa = arr.toPar
        pa.filter(filterSqrt)
      }

      using(tinyArrays) curve ("Par-2") in { arr =>
        import workstealing.Ops._
        implicit val s = s2
        val pa = arr.toPar
        pa.filter(filterSqrt)
      }

      using(tinyArrays) curve ("Par-4") in { arr =>
        import workstealing.Ops._
        implicit val s = s4
        val pa = arr.toPar
        pa.filter(filterSqrt)
      }

      using(tinyArrays) curve ("Par-8") in { arr =>
        import workstealing.Ops._
        implicit val s = s8
        val pa = arr.toPar
        pa.filter(filterSqrt)
      }
    }

    measure method "flatMap" in {
      val other = List(2, 3)
  
      using(smallArrays) curve ("Sequential") in { arr =>
        var i = 0
        val until = arr.length
        val ib = new SimpleBuffer[Int]
        while (i < until) {
          val elem = arr(i)
          other.foreach(y => ib.pushback(elem * y))
          i += 1
        }
      }
  
      using(smallArrays) curve ("Par-1") in { arr =>
        import workstealing.Ops._
        implicit val s = s1
        val pa = arr.toPar
        for {
          x <- pa
          y <- other
        } yield {
          x * y
        }: @unchecked
      }
  
      using(smallArrays) curve ("Par-2") in { arr =>
        import workstealing.Ops._
        implicit val s = s2
        val other = List(2, 3)
        val pa = arr.toPar
        for {
          x <- pa
          y <- other
        } yield {
          x * y
        }: @unchecked
      }
  
      using(smallArrays) curve ("Par-4") in { arr =>
        import workstealing.Ops._
        implicit val s = s4
        val other = List(2, 3)
        val pa = arr.toPar
        for {
          x <- pa
          y <- other
        } yield {
          x * y
        }: @unchecked
      }
  
      using(smallArrays) curve ("Par-8") in { arr =>
        import workstealing.Ops._
        implicit val s = s8
        val other = List(2, 3)
        val pa = arr.toPar
        for {
          x <- pa
          y <- other
        } yield {
          x * y
        }: @unchecked
      }
    }

  }

}



