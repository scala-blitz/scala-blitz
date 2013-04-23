package scala.collection.parallel
package workstealing






object Ops 
extends Zippable.Scope
with Arrays.Scope
with Range.Scope {

  type Scheduler = WorkstealingTreeScheduler

}

