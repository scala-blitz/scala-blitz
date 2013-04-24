package scala.collection.parallel
package scalameter



import org.scalameter.api._



class RangeBench extends PerformanceTest.Regression {
  import Par._
  import workstealing.WorkstealingTreeScheduler
  import workstealing.WorkstealingTreeScheduler.Config

  /* config */

  def persistor = new SerializationPersistor

  /* generators */

  val size = 150000000
  val range = 0 until size
  val parRange = range.toPar
  val parLevels = Gen.enumeration("par")(1, 2, 4, 8)
  val schedulers = for (p <- parLevels) yield new WorkstealingTreeScheduler.ForkJoin(new Config.Default(p))

  performance of "Par[Range]" in {

    measure method "reduce" in {
      using(schedulers) in { implicit s =>
        import workstealing.Ops._
        parRange.reduce(_ + _)
        s.pool.shutdown()
      }
    }

  }

}