package scala.collection



package object par extends par.ParDefs with par.workstealing.OpsDefs {
  type Scheduler = workstealing.Scheduler
}
