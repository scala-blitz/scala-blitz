package scala.collection.workstealing



import scala.language.experimental.macros
import scala.reflect.macros._



object Workloads {

  /* 150000000 */
  final def uniform(start: Int, limit: Int, nmax: Int) = {
    var i = start
    var sum = 0
    while (i < limit) {
      sum += i
      i += 1
    }
    sum
  }

  /* 150000000 */
  final def uniform2(start: Int, limit: Int, nmax: Int) = {
    var i = start
    var sum = 0
    while (i < limit) {
      val x = i * i
      sum += x
      i += 1
    }
    sum
  }

  /* 15000000 */
  final def uniform3(start: Int, limit: Int, nmax: Int) = {
    var i = start
    var sum = 0
    while (i < limit) {
      val x = i * i * i * i * i * i * i * i
      sum += x
      i += 1
    }
    sum
  }

  /* 1500 */
  final def uniform4(start: Int, limit: Int, nmax: Int) = {
    def calc(n: Int): Int = {
      var i = 0
      var sum = 0.0f
      var numerator = 1.0f
      var denominator = 1.0f
      while (i < 50000) {
        sum += numerator / denominator
        i += 1
        numerator *= n
        denominator *= i
      }
      sum.toInt
    }

    var i = start
    var sum = 0
    while (i < limit) {
      val x = calc(i)
      sum += x
      i += 1
    }
    sum
  }

  /* 15 */ 
  final def uniform5(start: Int, limit: Int, nmax: Int) = {
    def calc(n: Int): Int = {
      var i = 0
      var sum = 0.0f
      var numerator = 1.0f
      var denominator = 1.0f
      while (i < 2000000) {
        sum += numerator / denominator
        i += 1
        numerator *= n
        denominator *= i
      }
      sum.toInt
    }

    var i = start
    var sum = 0
    while (i < limit) {
      val x = calc(i)
      sum += x
      i += 1
    }
    sum
  }

  /* 15000000 */
  final def triangle(start: Int, limit: Int, nmax: Int) = {
    def work(n: Int): Int = {
      val amountOfWork = 1 + n / 1000000
      var tmp = 1
      var j = 1
      while (j < amountOfWork) {
        tmp += j
        j += 1
      }
      tmp
    }

    var i = start
    var sum = 0
    while (i < limit) {
      sum += work(i)
      i += 1
    }
    sum
  }

  /* 500000 */
  final def triangle2(start: Int, limit: Int, nmax: Int) = {
    def work(n: Int): Int = {
      val amountOfWork = 1 + n / 1000
      var sum = 1
      var j = 1
      while (j < amountOfWork) {
        sum += j
        j += 1
      }
      sum
    }

    var i = start
    var sum = 0
    while (i < limit) {
      val res = work(i)
      sum += res
      i += 1
    }
    sum
  }

  /* 15000 */
  final def triangle3(start: Int, limit: Int, nmax: Int) = {
    def work(n: Int): Int = {
      val amountOfWork = 1 + n
      var sum = 1
      var j = 1
      while (j < amountOfWork) {
        sum += j
        j += 1
      }
      sum
    }

    var i = start
    var sum = 0
    while (i < limit) {
      val res = work(i)
      sum += res
      i += 1
    }
    sum
  }

  /* 150000 */
  final def parabola(start: Int, limit: Int, nmax: Int) = {
    def work(n: Int): Int = {
      val factor = n / 2500
      val amountOfWork = 1 + factor * factor
      var sum = 1
      var j = 1
      while (j < amountOfWork) {
        sum += j
        j += 1
      }
      sum
    }

    var i = start
    var sum = 0
    while (i < limit) {
      val res = work(i)
      sum += res
      i += 1
    }
    sum
  }

  /* 15000 */
  final def parabola2(start: Int, limit: Int, nmax: Int) = {
    def work(n: Int): Int = {
      val factor = n / 50
      val amountOfWork = 1 + factor * factor
      var sum = 1
      var j = 1
      while (j < amountOfWork) {
        sum += j
        j += 1
      }
      sum
    }

    var i = start
    var sum = 0
    while (i < limit) {
      val res = work(i)
      sum += res
      i += 1
    }
    sum
  }

  /* 1500 */
  final def parabola3(start: Int, limit: Int, nmax: Int) = {
    def work(n: Int): Int = {
      val factor = n
      val amountOfWork = 1 + factor * factor
      var sum = 1
      var j = 1
      while (j < amountOfWork) {
        sum += j
        j += 1
      }
      sum
    }

    var i = start
    var sum = 0
    while (i < limit) {
      val res = work(i)
      sum += res
      i += 1
    }
    sum
  }

  /* 150 */
  final def parabola4(start: Int, limit: Int, nmax: Int) = {
    def work(n: Int): Int = {
      val factor = n
      val amountOfWork = 1 + 500 * factor * factor
      var sum = 1
      var j = 1
      while (j < amountOfWork) {
        sum += j
        j += 1
      }
      sum
    }

    var i = start
    var sum = 0
    while (i < limit) {
      val res = work(i)
      sum += res
      i += 1
    }
    sum
  }

  /* 15 */
  final def parabola5(start: Int, limit: Int, nmax: Int) = {
    def work(n: Int): Int = {
      val factor = n
      val amountOfWork = 1 + 100000 * factor * factor
      var sum = 1
      var j = 1
      while (j < amountOfWork) {
        sum += j
        j += 1
      }
      sum
    }

    var i = start
    var sum = 0
    while (i < limit) {
      val res = work(i)
      sum += res
      i += 1
    }
    sum
  }

  /* 15000 */
  final def exp(start: Int, limit: Int, nmax: Int) = {
    def work(n: Int): Int = {
      val factor = n / 1000
      val amountOfWork = 1 + math.pow(2.0, factor)
      var sum = 1
      var j = 1
      while (j < amountOfWork) {
        sum += j
        j += 1
      }
      sum
    }

    var i = start
    var sum = 0
    while (i < limit) {
      val res = work(i)
      sum += res
      i += 1
    }
    sum
  }

  /* 95 */
  final def exp2(start: Int, limit: Int, nmax: Int) = {
    def work(n: Int): Int = {
      val factor = n
      val amountOfWork = 1 + math.pow(1.2, factor)
      var sum = 1
      var j = 1
      while (j < amountOfWork) {
        sum += j
        j += 1
      }
      sum
    }

    var i = start
    var sum = 0
    while (i < limit) {
      val res = work(i)
      sum += res
      i += 1
    }
    sum
  }

  /* 25 */
  final def exp3(start: Int, limit: Int, nmax: Int) = {
    def work(n: Int): Int = {
      val factor = n
      val amountOfWork = 1 + math.pow(2.0, factor)
      var sum = 1
      var j = 1
      while (j < amountOfWork) {
        sum += j
        j += 1
      }
      sum
    }

    var i = start
    var sum = 0
    while (i < limit) {
      val res = work(i)
      sum += res
      i += 1
    }
    sum
  }

  /* 150 */
  final def invtriangle(start: Int, limit: Int, nmax: Int) = {
    def work(n: Int): Int = {
      val amountOfWork = (nmax - n) * 10000
      var sum = 1
      var j = 1
      while (j < amountOfWork) {
        sum += j
        j += 1
      }
      sum
    }

    var i = start
    var sum = 0
    while (i < limit) {
      val res = work(i)
      sum += res
      i += 1
    }
    sum
  }

  /* 15 */
  final def invtriangle2(start: Int, limit: Int, nmax: Int) = {
    def work(n: Int): Int = {
      val amountOfWork = (nmax - n) * 1000000
      var sum = 1
      var j = 1
      while (j < amountOfWork) {
        sum += j
        j += 1
      }
      sum
    }

    var i = start
    var sum = 0
    while (i < limit) {
      val res = work(i)
      sum += res
      i += 1
    }
    sum
  }

  /* 150 */
  final def hill(start: Int, limit: Int, nmax: Int) = {
    def work(n: Int): Int = {
      val factor = 40000
      val amountOfWork = if (n < nmax / 2) n * factor else (nmax - n) * factor
      var sum = 1
      var j = 1
      while (j < amountOfWork) {
        sum += j
        j += 1
      }
      sum
    }

    var i = start
    var sum = 0
    while (i < limit) {
      val res = work(i)
      sum += res
      i += 1
    }
    sum
  }

  /* 15 */
  final def hill2(start: Int, limit: Int, nmax: Int) = {
    def work(n: Int): Int = {
      val factor = 4000000
      val amountOfWork = if (n < nmax / 2) n * factor else (nmax - n) * factor
      var sum = 1
      var j = 1
      while (j < amountOfWork) {
        sum += j
        j += 1
      }
      sum
    }

    var i = start
    var sum = 0
    while (i < limit) {
      val res = work(i)
      sum += res
      i += 1
    }
    sum
  }

  /* 15 */
  final def valley(start: Int, limit: Int, nmax: Int) = {
    def work(n: Int): Int = {
      val factor = 4000000
      val amountOfWork = if (n < nmax / 2) (nmax / 2 - n) * factor else (n - nmax / 2) * factor
      var sum = 1
      var j = 1
      while (j < amountOfWork) {
        sum += j
        j += 1
      }
      sum
    }

    var i = start
    var sum = 0
    while (i < limit) {
      val res = work(i)
      sum += res
      i += 1
    }
    sum
  }

  /* 1500 */
  final def gaussian(start: Int, limit: Int, nmax: Int) = {
    def work(n: Int): Int = {
      val amountOfWork = math.min(100000, math.abs(30000 * scala.util.Random.nextGaussian()))
      var sum = 1
      var j = 1
      while (j < amountOfWork) {
        sum += j
        j += 1
      }
      sum
    }

    var i = start
    var sum = 0
    while (i < limit) {
      val res = work(i)
      sum += res
      i += 1
    }
    sum
  }

  /* 150 */
  final def randif(start: Int, limit: Int, nmax: Int) = {
    def work(n: Int): Int = {
      val amountOfWork = if (math.abs(n * 0x9e3775cd) % 100 < 10) 5000000 else 1
      var sum = 1
      var j = 1
      while (j < amountOfWork) {
        sum += j
        j += 1
      }
      sum
    }

    var i = start
    var sum = 0
    while (i < limit) {
      val res = work(i)
      sum += res
      i += 1
    }
    sum
  }

  /* 15000 */
  final def step(start: Int, limit: Int, nmax: Int) = {
    def work(n: Int): Int = {
      val amountOfWork = if (n < nmax * 11 / 15) 1 else 25000
      var sum = 1
      var j = 1
      while (j < amountOfWork) {
        sum += j
        j += 1
      }
      sum
    }

    var i = start
    var sum = 0
    while (i < limit) {
      val res = work(i)
      sum += res
      i += 1
    }
    sum
  }

  /* 1500 */
  final def step2(start: Int, limit: Int, nmax: Int) = {
    def work(n: Int): Int = {
      val amountOfWork = if (n < nmax / 5) 500000 else 1
      var sum = 1
      var j = 1
      while (j < amountOfWork) {
        sum += j
        j += 1
      }
      sum
    }

    var i = start
    var sum = 0
    while (i < limit) {
      val res = work(i)
      sum += res
      i += 1
    }
    sum
  }

  /* 1500 */
  final def step3(start: Int, limit: Int, nmax: Int) = {
    def work(n: Int): Int = {
      val amountOfWork = if (n < nmax * 11 / 15) 1 else 500000
      var sum = 1
      var j = 1
      while (j < amountOfWork) {
        sum += j
        j += 1
      }
      sum
    }

    var i = start
    var sum = 0
    while (i < limit) {
      val res = work(i)
      sum += res
      i += 1
    }
    sum
  }

  /* 1500 */
  final def step4(start: Int, limit: Int, nmax: Int) = {
    def work(n: Int): Int = {
      val amountOfWork = if (n < nmax * 3 / 15 || n > nmax * 7 / 15) 1 else 500000
      var sum = 1
      var j = 1
      while (j < amountOfWork) {
        sum += j
        j += 1
      }
      sum
    }

    var i = start
    var sum = 0
    while (i < limit) {
      val res = work(i)
      sum += res
      i += 1
    }
    sum
  }

  def kernel(start: Int, limit: Int, nmax: Int) = macro kernel_impl

  def kernel_impl(c: Context)(start: c.Expr[Int], limit: c.Expr[Int], nmax: c.Expr[Int]): c.Expr[Int] = {
    import c.universe._
    
    sys.props.getOrElse("kernel", "uniform") match {
      case "uniform" => reify {
        uniform(start.splice, limit.splice, nmax.splice)
      }
      case "uniform2" => reify {
        uniform2(start.splice, limit.splice, nmax.splice)
      }
      case "uniform3" => reify {
        uniform3(start.splice, limit.splice, nmax.splice)
      }
      case "uniform4" => reify {
        uniform4(start.splice, limit.splice, nmax.splice)
      }
      case "uniform5" => reify {
        uniform5(start.splice, limit.splice, nmax.splice)
      }
      case "triangle" => reify {
        triangle(start.splice, limit.splice, nmax.splice)
      }
      case "triangle2" => reify {
        triangle2(start.splice, limit.splice, nmax.splice)
      }
      case "triangle3" => reify {
        triangle3(start.splice, limit.splice, nmax.splice)
      }
      case "parabola" => reify {
        parabola(start.splice, limit.splice, nmax.splice)
      }
      case "parabola2" => reify {
        parabola2(start.splice, limit.splice, nmax.splice)
      }
      case "parabola3" => reify {
        parabola3(start.splice, limit.splice, nmax.splice)
      }
      case "parabola4" => reify {
        parabola4(start.splice, limit.splice, nmax.splice)
      }
      case "parabola5" => reify {
        parabola5(start.splice, limit.splice, nmax.splice)
      }
      case "exp" => reify {
        exp(start.splice, limit.splice, nmax.splice)
      }
      case "exp2" => reify {
        exp2(start.splice, limit.splice, nmax.splice)
      }
      case "exp3" => reify {
        exp3(start.splice, limit.splice, nmax.splice)
      }
      case "invtriangle" => reify {
        invtriangle(start.splice, limit.splice, nmax.splice)
      }
      case "invtriangle2" => reify {
        invtriangle2(start.splice, limit.splice, nmax.splice)
      }
      case "hill" => reify {
        hill(start.splice, limit.splice, nmax.splice)
      }
      case "hill2" => reify {
        hill2(start.splice, limit.splice, nmax.splice)
      }
      case "valley" => reify {
        valley(start.splice, limit.splice, nmax.splice)
      }
      case "gaussian" => reify {
        gaussian(start.splice, limit.splice, nmax.splice)
      }
      case "randif" => reify {
        randif(start.splice, limit.splice, nmax.splice)
      }
      case "step" => reify {
        step(start.splice, limit.splice, nmax.splice)
      }
      case "step2" => reify {
        step2(start.splice, limit.splice, nmax.splice)
      }
      case "step3" => reify {
        step3(start.splice, limit.splice, nmax.splice)
      }
      case "step4" => reify {
        step4(start.splice, limit.splice, nmax.splice)
      }
    }
  }

  def interruptibleKernel(request: Boolean, intfreq: Int, size: Int): (Int, Int) = macro interruptibleKernel_impl

  def interruptibleKernel_impl(c: Context)(request: c.Expr[Boolean], intfreq: c.Expr[Int], size: c.Expr[Int]): c.Expr[(Int, Int)] = {
    import c.universe._

    reify {
      var sum = 0
      sum += 0
      if (request.splice) (sum, 1) else {
        sum += 1
        sum += 2
        if (request.splice) (sum, 3) else {
          sum += 3
          sum += 4
          sum += 5
          sum += 6
          if (request.splice) (sum, 7) else {
            sum += 7
            sum += 8
            sum += 9
            sum += 10
            sum += 11
            sum += 12
            sum += 13
            sum += 14
            if (request.splice) (sum, 15) else {
              sum += 15
              sum += 16
              sum += 17
              sum += 18
              sum += 19
              sum += 20
              sum += 21
              sum += 22
              sum += 23
              sum += 24
              sum += 25
              sum += 26
              sum += 27
              sum += 28
              sum += 29
              sum += 30
              if (request.splice) (sum, 31) else {
                var i = 31
                while (i < size.splice && !request.splice) {
                  var next = i + intfreq.splice
                  if (next > size.splice) next = size.splice
                  while (i < next) {
                    sum += i
                    i += 1
                  }
                }
                (i, sum)
              }
            }
          }
        }
      }
    }
  }

  lazy val items = new Array[Int](sys.props("size").toInt)

  private def uniformCheck(start: Int, limit: Int, nmax: Int) = {
    var i = start
    var sum = 0
    while (i < limit) {
      items(i) += 1
      sum += 1
      i += 1
    }
    sum
  }

}

