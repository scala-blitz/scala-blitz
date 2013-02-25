package scala.collection.workstealing
package benchmarks






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
    val range: ParOperations[Int] = new ParRange(0 until size, Workstealing.DefaultConfig)
    @volatile var found = false
    range.foreach(x => if ((x & 0xfffff) == 0) found = true)
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
    @volatile var found = false
    range.foreach(x => if ((x & 0xfffff) == 0) found = true)
  }

}






