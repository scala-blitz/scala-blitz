package scala.collection



package object par extends par.ParDefs with par.workstealing.OpsDefs {
  type Scheduler = workstealing.Scheduler

  object Configuration {
      val manualOptimizations = sys.props.get("scala.collection.par.range.manual_optimizations").map(_.toBoolean).getOrElse(true)
    }
}

