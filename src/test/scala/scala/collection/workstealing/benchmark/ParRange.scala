package scala.collection.workstealing
package benchmark






object ParRangeForeach extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range = new ParRange(0 until size, Workstealing.DefaultConfig)
    @volatile var found = false
    range.foreach(x => if ((x & 0xfffff) == 0) found = true)
  }

}


object ParRangeForeachGeneric extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range: ParIterable[Int] = new ParRange(0 until size, Workstealing.DefaultConfig)
    @volatile var found = false
    range.foreach((x: Int) => if ((x & 0xfffff) == 0) found = true)
  }

}


object ParRangeForeachPC extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range = (0 until size).par
    @volatile var found = false
    range.foreach(x => if ((x & 0xfffff) == 0) found = true)
  }

}


object ParRangeFold extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range = new ParRange(0 until size, Workstealing.DefaultConfig)
    range.fold(0)(_ + _)
  }

}


object ParRangeFoldGeneric extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range: ParIterable[Int] = new ParRange(0 until size, Workstealing.DefaultConfig)
    range.fold(0)(_ + _)
  }

}


object ParRangeFoldPC extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range = (0 until size).par
    range.fold(0)(_ + _)
  }

}


object ParRangeReduce extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range = new ParRange(0 until size, Workstealing.DefaultConfig)
    range.reduce(_ + _)
  }

}


object ParRangeReduceGeneric extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range: ParIterable[Int] = new ParRange(0 until size, Workstealing.DefaultConfig)
    range.reduce(_ + _)
  }

}


object ParRangeReducePC extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range = (0 until size).par
    range.reduce(_ + _)
  }

}


object ParRangeAggregate extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range = new ParRange(0 until size, Workstealing.DefaultConfig)
    range.aggregate(0)(_ + _)(_ + _)
  }

}


object ParRangeAggregateGeneric extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range: ParIterable[Int] = new ParRange(0 until size, Workstealing.DefaultConfig)
    range.aggregate(0)(_ + _)(_ + _)
  }

}


object ParRangeAggregatePC extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range = (0 until size).par
    range.aggregate(0)(_ + _, _ + _)
  }

}


object ParRangeSum extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range = new ParRange(0 until size, Workstealing.DefaultConfig)
    range.sum
  }

}


object ParRangeSumGeneric extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range: ParIterable[Int] = new ParRange(0 until size, Workstealing.DefaultConfig)
    range.sum
  }

}


object ParRangeSumPC extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range = (0 until size).par
    range.sum
  }

}


object ParRangeCount extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range = new ParRange(0 until size, Workstealing.DefaultConfig)
    range.count(x => (x & 0x3) == 0)
  }

}


object ParRangeCountGeneric extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range: ParIterable[Int] = new ParRange(0 until size, Workstealing.DefaultConfig)
    range.count(x => (x & 0x3) == 0)
  }

}


object ParRangeCountPC extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range = (0 until size).par
    range.count(x => (x & 0x3) == 0)
  }

}


object ParRangeFind extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  var i = 1

  def run() {
    val range = new ParRange(0 until size, Workstealing.DefaultConfig)
    range.find(x => 10 * x < 0)
  }

}


object ParRangeFindGeneric extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  var i = 1

  def run() {
    val range: ParIterable[Int] = new ParRange(0 until size, Workstealing.DefaultConfig)
    range.find(x => 10 * x < 0)
  }

}


object ParRangeFindPC extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range = (0 until size).par
    range.find(x => 10 * x < 0)
  }

}


object ParRangeFindEarly extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range = new ParRange(0 until size, Workstealing.DefaultConfig)
    range.find(x => x % 50000000 == 12000000)
  }

}


object ParRangeFindEarlyGeneric extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range: ParIterable[Int] = new ParRange(0 until size, Workstealing.DefaultConfig)
    range.find(x => x % 50000000 == 12000000)
  }

}


object ParRangeFindEarlyPC extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range = (0 until size).par
    range.find(x => x % 50000000 == 12000000)
  }

}


object ParRangeCopyToArray extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val arr = new Array[Int](size)

  def run() {
    val range = new ParRange(0 until size, Workstealing.DefaultConfig)
    range.copyToArray(arr, 0, arr.length)
  }

}


object ParRangeCopyToArrayGeneric extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val arr = new Array[Int](size)

  def run() {
    val range: ParIterable[Int] = new ParRange(0 until size, Workstealing.DefaultConfig)
    range.copyToArray(arr, 0, arr.length)
  }

}


object ParRangeCopyToArrayPC extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val arr = new Array[Int](size)

  def run() {
    val range = (0 until size).par
    range.copyToArray(arr, 0, arr.length)
  }

}


object ParRangeFilter extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val arr = new Array[Int](size)

  def run() {
    val range = new ParRange(0 until size, Workstealing.DefaultConfig)
    range.filter2(x => true)
  }

}


object ParRangeFilterGeneric extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val arr = new Array[Int](size)

  def run() {
    val range: ParIterable[Int] = new ParRange(0 until size, Workstealing.DefaultConfig)
    range.filter(x => true)
  }

}


object ParRangeFilterPC extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val arr = new Array[Int](size)

  def run() {
    val range = (0 until size).par
    range.filter(x => true)
  }

}













