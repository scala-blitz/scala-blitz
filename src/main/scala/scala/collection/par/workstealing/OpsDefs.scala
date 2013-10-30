package scala.collection.par
package workstealing






trait OpsDefs
extends Zippables.Scope
with Arrays.Scope
with Ranges.Scope
with Concs.Scope
with Hashes.Scope 
with Trees.Scope
with Reducables.Scope {

  type Scheduler = WorkstealingTreeScheduler

}
