package org.scala.optimized.test.par
package scalameter


import org.scalameter.api._
import org.scalameter.PerformanceTest.OnlineRegressionReport


class benchmarks extends OnlineRegressionReport with Serializable {

  /* tests */

  include[ConcBench]
  include[ConcMemory]
  include[ParRangeBench]
  include[ParConcBench]
  include[ParArrayBench]
  include[ParHashMapBench]
  include[ReducableBench]
  include[ParHashTrieSetBench]
}





