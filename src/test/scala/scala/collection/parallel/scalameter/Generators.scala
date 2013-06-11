package scala.collection.parallel
package scalameter



import org.scalameter.api._
import workstealing.WorkstealingTreeScheduler
import workstealing.WorkstealingTreeScheduler.Config
import scala.collection._



trait Generators {

  def sizes(from: Int) = Gen.enumeration("size")(from, from * 3, from * 5)
  def ranges(from: Int) = for (size <- sizes(from)) yield 0 until size
  def arrays(from: Int) = for (size <- sizes(from)) yield (0 until size).toArray
  def concs(from: Int) = for (size <- sizes(from)) yield {
    var conc: Conc[Int] = Conc.Zero
    for (i <- 0 until size) conc = conc <> i
    conc
  }
  def normalizedConcs(from: Int) = for (conc <- concs(from)) yield conc.normalized
  def bufferConcs(from: Int) = for (size <- sizes(from)) yield {
    var cb = new Conc.Buffer[Int]
    for (i <- 0 until size) cb += i
    cb.result.normalized
  }
  def hashMaps(from: Int) = for (size <- sizes(from)) yield {
    val hm = new mutable.HashMap[Int, Int]
    for (i <- 0 until size) hm += ((i, i))
    hm
  }

  def withArrays[Repr <% TraversableOnce[_]](gen: Gen[Repr]) = for (coll <- gen) yield (coll, new Array[Int](coll.size))

  val parallelismLevels = Gen.exponential("par")(1, Runtime.getRuntime.availableProcessors, 2)
  val schedulers = {
    val ss = for (par <- parallelismLevels) yield new WorkstealingTreeScheduler.ForkJoin(new Config.Default(par))
    ss.cached
  }
  def withSchedulers[Repr](colls: Gen[Repr]): Gen[(Repr, WorkstealingTreeScheduler)] = for {
    c <- colls
    s <- schedulers
  } yield (c, s)

}
