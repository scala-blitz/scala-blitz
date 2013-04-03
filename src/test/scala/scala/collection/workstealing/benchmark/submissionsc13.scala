package scala.collection.workstealing
package benchmark





/* cheap, uniform range foreach - 150M */

object ParRangeForeachCheapSpecific extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  @volatile var found = false

  def run() {
    val range = new ParRange(0 until size, Workstealing.DefaultConfig)
    range.foreach(x => if (((x * x) & 0xffffff) == 0) found = true)
  }

}


object ParRangeForeachCheapGeneric extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  @volatile var found = false

  def run() {
    val range: ParIterable[Int] = new ParRange(0 until size, Workstealing.DefaultConfig)
    range.foreach(x => if (((x * x) & 0xffffff) == 0) found = true)
  }

}


object ParRangeForeachCheapPC extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val parlevel = sys.props("par").toInt
  val fj = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parlevel))
  @volatile var found = false

  def run() {
    val range = (0 until size).par
    range.tasksupport = fj
    range.foreach(x => if (((x * x) & 0xffffff) == 0) found = true)
  }

}


object ParRangeForeachCheapLoop extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  @volatile var found = false

  def run() {
    var i = 0
    while (i < size) {
      if (((i * i) & 0xffffff) == 0) found = true
      i += 1
    }
  }

}


/* cheap, uniform range fold - 150M */

object ParRangeFoldCheapSpecific extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range = new ParRange(0 until size, Workstealing.DefaultConfig)
    val result = range.fold(0)(_ + _)
    println(result)
  }

}


object ParRangeFoldCheapGeneric extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range: ParIterable[Int] = new ParRange(0 until size, Workstealing.DefaultConfig)
    range.fold(0)(_ + _)
  }

}


object ParRangeFoldCheapPC extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val parlevel = sys.props("par").toInt
  val fj = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parlevel))

  def run() {
    val range = (0 until size).par
    range.tasksupport = fj
    range.fold(0)(_ + _)
  }

}


object ParRangeFoldCheapLoop extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  var result = 0

  def run() {
    var sum = 0
    var i = 0
    while (i < size) {
      sum += i
      i += 1
    }
    result = sum
  }

}


/* cheap, uniform array fold - 75M */

object ParArrayFoldCheapSpecific extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val array = new ParArray((0 until size).toArray, Workstealing.DefaultConfig)

  def run() {
    array.fold(0)(_ + _)
  }

}


object ParArrayFoldCheapGeneric extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val array: ParIterable[Int] = new ParArray((0 until size).toArray, Workstealing.DefaultConfig)

  def run() {
    array.fold(0)(_ + _)
  }

}


object ParArrayFoldCheapPC extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val parlevel = sys.props("par").toInt
  val fj = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parlevel))
  val array = (0 until size).toArray.par
  array.tasksupport = fj

  def run() {
    array.fold(0)(_ + _)
  }

}


object ParArrayFoldCheapLoop extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val array = (0 until size).toArray
  var result = 0

  def run() {
    var sum = 0
    var i = 0
    while (i < size) {
      sum += array(i)
      i += 1
    }
    result = sum
  }

}


/* cheap, uniform conc fold - 15M */

object ParConcFoldSpecific extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val conc = ConcUtils.create(size)

  def run() {
    conc.fold(0)(_ + _)
  }

}


object ParConcFoldGeneric extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val conc: ParIterable[Int] = ConcUtils.create(size)

  def run() {
    conc.fold(0)(_ + _)
  }

}


object ParConcFoldRecursion extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val conc = ConcUtils.create(size)

  def run() {
    import Conc._
    def fold(c: Conc[Int]): Int = c match {
      case left || right => fold(left) + fold(right)
      case Nil() => 0
      case Single(elem) => elem
    }
    fold(conc)
  }

}


object SeqListFold extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val list = (0 until size).toList

  def run() {
    list.fold(0)(_ + _)
  }

}


/* irregular range fold - 50k */

object IrregularWorkloads {

  def peakAtEnd(x: Int, percent: Double, size: Int) = if (x < percent * size) x else {
    var sum = 0
    var i = 1
    while (i < size) {
      sum += i
      i += 1
    }
    sum
  }

  def isPrime(x: Int): Boolean = {
    var i = 2
    val to = math.sqrt(x).toInt + 2
    while (i <= to) {
      if (x % i == 0) return false
      i += 1
    }
    true
  }

  def exp(x: Int): Boolean = {
    var i = 0
    val until = 1 << (x / 100)
    var sum = 0
    while (i < until) {
      sum += i
      i += 1
    }
    sum % 2 == 0
  }

}

object ParRangeFoldIrregularSpecific extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range = new ParRange(0 until size, Workstealing.DefaultConfig)
    val result = range.aggregate(0)(_ + _) {
      (acc, x) => acc + IrregularWorkloads.peakAtEnd(x, 0.97, size)
    }
  }

}


object ParRangeFoldIrregularPC extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val parlevel = sys.props("par").toInt
  val fj = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parlevel))

  def run() {
    val range = (0 until size).par
    range.tasksupport = fj
    val result = range.aggregate(0)({
      (acc, x) => acc + IrregularWorkloads.peakAtEnd(x, 0.97, size)
    }, _ + _)
  }

}


object ParRangeFoldIrregularLoop extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  var result = 0

  def run() {
    var sum = 0
    var i = 0
    while (i < size) {
      sum += IrregularWorkloads.peakAtEnd(i, 0.97, size)
      i += 1
    }
    result = sum
  }

}


/* irregular array filter - 1M */

object ParArrayFilterIrregularSpecific extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val array = new ParArray((0 until size).toArray, Workstealing.DefaultConfig)

  def run() {
    val result = array.filter(IrregularWorkloads.isPrime)
  }

}


object ParArrayFilterIrregularPC extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val parlevel = sys.props("par").toInt
  val fj = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parlevel))
  val array = (0 until size).toArray.par
  array.tasksupport = fj

  def run() {
    val result = array.filter(IrregularWorkloads.isPrime)
  }

}


/* irregular range filter - 2500 */

object ParRangeFilterIrregularSpecific extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val range = new ParRange(0 until size, Workstealing.DefaultConfig)

  def run() {
    val result = range.filter(IrregularWorkloads.exp)
  }

}


object ParRangeFilterIrregularPC extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val parlevel = sys.props("par").toInt
  val fj = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parlevel))
  val range = (0 until size).par
  range.tasksupport = fj

  def run() {
    val result = range.filter(IrregularWorkloads.exp)
  }

}


/* standard deviation computation */

object StandardDeviationSpecific extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val numbers = new Array[Double](size)
  for (i <- 0 until size) numbers(i) = 10.0 + (i - size / 2.0) / size
  
  val measurements = new ParArray(numbers, Workstealing.DefaultConfig)

  def run() {
    val mean = measurements.fold(0.0)(_ + _) / measurements.size
    val variance = measurements.aggregate(0.0)(_ + _) {
      (acc, x) => acc + (x - mean) * (x - mean)
    }
    val stdev = math.sqrt(variance)
  }

}


object StandardDeviationPC extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val numbers = new Array[Double](size)
  for (i <- 0 until size) numbers(i) = 10.0 + (i - size / 2.0) / size

  val parlevel = sys.props("par").toInt
  val fj = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parlevel))
  val measurements = numbers.par
  measurements.tasksupport = fj

  def run() {
    val mean = measurements.fold(0.0)(_ + _) / measurements.size
    val variance = measurements.aggregate(0.0)({
      (acc, x) => acc + (x - mean) * (x - mean)
    }, _ + _)
    val stdev = math.sqrt(variance)
  }

}





















