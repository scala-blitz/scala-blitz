package scala.collection.parallel






object Configuration {

  val manualOptimizations = sys.props.get("scala.collection.parallel.range.manual_optimizations").map(_.toBoolean).getOrElse(true)

}

