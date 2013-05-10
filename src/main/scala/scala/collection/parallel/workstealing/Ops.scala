package scala.collection.parallel
package workstealing






object Ops 
extends Zippable.Scope
with Arrays.Scope
with Ranges.Scope
with Concs.Scope {

  type Scheduler = WorkstealingTreeScheduler

}

