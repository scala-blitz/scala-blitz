package org.scala.optimized.test
package scalameter


import org.scalameter.api._
import org.scalameter.PerformanceTest.OnlineRegressionReport
import org.scala.optimized.test.par.scalameter._


class benchmarks extends OnlineRegressionReport with Serializable {

  /* tests */
  include[OptimizedListBench]
  include[ConcBench]
  include[ConcMemory]
  include[ParRangeBench]
  include[ParConcBench]
  include[ParArrayBench]
  include[ParHashMapBench]
  include[ReducableBench]
  include[ParHashTrieSetBench]
}





