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
    range.fold(0)(_ + _)
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


/* irregular range fold - 15M */

























