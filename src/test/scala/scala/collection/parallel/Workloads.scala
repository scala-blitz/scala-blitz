package scala.collection.parallel






trait Workloads {

  /* 150000000 */
  private def uniform(start: Int, limit: Int, nmax: Int) = {
    var i = start
    var sum = 0
    while (i < limit) {
      sum += i
      i += 1
    }
    sum
  }

  /* 150000000 */
  private def uniform2(start: Int, limit: Int, nmax: Int) = {
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
  private def uniform3(start: Int, limit: Int, nmax: Int) = {
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
  private def uniform4(start: Int, limit: Int, nmax: Int) = {
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
  private def uniform5(start: Int, limit: Int, nmax: Int) = {
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
  private def triangle(start: Int, limit: Int, nmax: Int) = {
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
  private def triangle2(start: Int, limit: Int, nmax: Int) = {
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
  private def triangle3(start: Int, limit: Int, nmax: Int) = {
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
  private def parabola(start: Int, limit: Int, nmax: Int) = {
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
  private def parabola2(start: Int, limit: Int, nmax: Int) = {
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
  private def parabola3(start: Int, limit: Int, nmax: Int) = {
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
  private def parabola4(start: Int, limit: Int, nmax: Int) = {
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
  private def parabola5(start: Int, limit: Int, nmax: Int) = {
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
  private def exp(start: Int, limit: Int, nmax: Int) = {
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
  private def exp2(start: Int, limit: Int, nmax: Int) = {
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
  private def exp3(start: Int, limit: Int, nmax: Int) = {
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
  private def invtriangle(start: Int, limit: Int, nmax: Int) = {
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
  private def invtriangle2(start: Int, limit: Int, nmax: Int) = {
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
  private def hill(start: Int, limit: Int, nmax: Int) = {
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
  private def hill2(start: Int, limit: Int, nmax: Int) = {
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
  private def valley(start: Int, limit: Int, nmax: Int) = {
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
  private def gaussian(start: Int, limit: Int, nmax: Int) = {
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
  private def randif(start: Int, limit: Int, nmax: Int) = {
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
  private def step(start: Int, limit: Int, nmax: Int) = {
    def work(n: Int): Int = {
      val amountOfWork = if (n < nmax / 2) 1 else 25000
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
  private def step2(start: Int, limit: Int, nmax: Int) = {
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
  private def step3(start: Int, limit: Int, nmax: Int) = {
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
  private def step4(start: Int, limit: Int, nmax: Int) = {
    def work(n: Int): Int = {
      val amountOfWork = if (n < nmax * 6 / 15 || n > nmax * 10 / 15) 1 else 500000
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

  final def kernel(start: Int, limit: Int, nmax: Int) = {
    //uniform(start, limit, nmax)
    //uniform2(start, limit, nmax)
    //uniform3(start, limit, nmax)
    //uniform4(start, limit, nmax)
    //uniform5(start, limit, nmax)
    //triangle(start, limit, nmax)
    //triangle2(start, limit, nmax)
    //triangle3(start, limit, nmax)
    //parabola(start, limit, nmax)
    //parabola2(start, limit, nmax)
    //parabola3(start, limit, nmax)
    //parabola4(start, limit, nmax)
    //parabola5(start, limit, nmax)
    //exp(start, limit, nmax)
    //exp2(start, limit, nmax)
    //exp3(start, limit, nmax)
    //invtriangle(start, limit, nmax)
    //invtriangle2(start, limit, nmax)
    //hill(start, limit, nmax)
    //hill2(start, limit, nmax)
    //valley(start, limit, nmax)
    //gaussian(start, limit, nmax)
    //randif(start, limit, nmax)
    //step(start, limit, nmax)
    //step2(start, limit, nmax)
    //step3(start, limit, nmax)
    step4(start, limit, nmax)
  }

  protected val items = new Array[Int](sys.props("size").toInt)

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




