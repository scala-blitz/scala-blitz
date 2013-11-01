package scala.collection.par






object Configuration {

  val manualOptimizations = sys.props.get("scala.collection.par.range.manual_optimizations").map(_.toBoolean).getOrElse(true)

}

