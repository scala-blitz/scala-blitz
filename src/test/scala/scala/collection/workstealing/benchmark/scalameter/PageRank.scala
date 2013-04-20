package scala.collection.workstealing.benchmark.scalameter

import org.scalameter.{ Reporter, Gen, PerformanceTest }
import org.scalameter.persistence.SerializationPersistor
import org.scalameter.api._
import java.awt.Color
import org.scalameter.reporting.{ ChartReporter, HtmlReporter, RegressionReporter }
import collection.workstealing.{ Workstealing, ParRange }
import collection.workstealing.benchmark.MathUtils

/**
 * Author: Dmitry Petrashko
 * Date: 21.03.13
 *
 * PageRank test app
 */

object PageRank extends PerformanceTest.Regression {

  /* configuration */

  lazy val persistor = new SerializationPersistor

  lazy val colorsTestSample = List(Color.RED, Color.GREEN, Color.BLUE) //, new Color(14, 201, 198), new Color(212, 71, 11))

  /* inputs */

  val data = Gen.single("SamplePageRank")(sampleData)

  /* tests  */

  val seriesNames = Array("a", "b", "3", "4")

  override val reporter: Reporter = org.scalameter.Reporter.Composite(
    new RegressionReporter(RegressionReporter.Tester.ConfidenceIntervals(), RegressionReporter.Historian.ExponentialBackoff()),
    new HtmlReporter(HtmlReporter.Renderer.Info(),
      HtmlReporter.Renderer.Chart(ChartReporter.ChartFactory.TrendHistogram(), "Trend Histogram", colorsTestSample)))

  performance of "PageRank" config (exec.independentSamples -> 8, exec.benchRuns -> 50, exec.jvmflags -> "-Xms256M -Xmx256M") in {

    measure method "time" in {

      using(data) curve ("old") in {
        case data =>
          getPageRankOld(data)
      }
    }

  }

  def sampleData = Map("A" -> Nil,
    "B" -> List("C"),
    "C" -> List("B"),
    "D" -> List("A", "B"),
    "E" -> List("D", "B", "F"),
    "F" -> List("E", "B"),
    "P1" -> List("B", "E"),
    "P2" -> List("B", "E"),
    "P3" -> List("B", "E"),
    "P4" -> List("E"),
    "P5" -> List("E"))

  def getPageRankOld(graph: Map[String, List[String]], ntop: Int = 20, maxIters: Int = 50, jumpFactor: Double = .15, diffTolerance: Double = 1E-6) = {

    // Precompute some values that will be used often for the updates.
    val numVertices = graph.size
    val uniformProbability = 1.0 / numVertices
    val jumpTimesUniform = jumpFactor / numVertices
    val oneMinusJumpFactor = 1.0 - jumpFactor

    // Create the vertex actors, and put in a map so we can
    // get them by ID.
    val idsToActors = graph.par.map {
      case (vertexId, adjacencyList) =>
        val vertex = new Vertex(adjacencyList, uniformProbability, vertexId)
        (vertexId, vertex)
    }.toMap

    // The list of vertex actors, used for dispatching messages to all.
    val vertices = idsToActors.values.toList.par

    var done = false
    var currentIteration = 1
    val result = StringBuilder.newBuilder

    while (!done) {

      // Tell all vertices to spread their mass and get back the
      // missing mass.

      val missingAndRedistributedMass = vertices.map { x => x.spreadMass }.reduce { (x, y) => (x._1 + y._1, x._2 ::: (y._2)) }

      val totalMissingMass = missingAndRedistributedMass._1
      val eachVertexRedistributedMass = totalMissingMass / numVertices
      val redistributedMass = missingAndRedistributedMass._2.par.groupBy(x => x._1).map { x => (x._1, x._2.aggregate(0.)({ (x, y) => x + y._2 }, _ + _)) }
      redistributedMass.par.foreach { x => idsToActors(x._1).takeMass(x._2) }
      val diffs = vertices.map { x => x.Update(jumpTimesUniform, oneMinusJumpFactor, eachVertexRedistributedMass) }

      val averageDiff =
        diffs.sum / numVertices

      println("Iteration " + currentIteration
        + ": average diff == " + averageDiff)

      currentIteration += 1

      if (currentIteration > maxIters || averageDiff < diffTolerance) {
        done = true

        vertices.map { vertex => vertex.getPageRank }.toList.foreach { x => result ++= (x._1 + "\t" + x._2 + "\n") }

      }

    }

    result.toString()
  }

  /**
   * An actor for a vertex in the graph.
   */
  class Vertex(var neighbors: List[String], var pagerank: Double, id: String) {

    //  var neighbors: List[ActorRef] = List[ActorRef]()
    //  var pagerank = 0.0
    var outdegree = neighbors.length
    var receivedMass = 0.0

    def spreadMass = {
      if (outdegree == 0) (pagerank, Nil)
      else {
        val amountPerNeighbor = pagerank / outdegree
        (0.0, neighbors.map((_, amountPerNeighbor)))
      }
    }

    def takeMass(contribution: Double) = receivedMass += contribution
    def getPageRank = (id, pagerank)
    def Update(jumpTimesUniform: Double, oneMinusJumpFactor: Double, redistributedMass: Double) = {
      val updatedPagerank =
        jumpTimesUniform + oneMinusJumpFactor * (redistributedMass + receivedMass)
      val diff = math.abs(pagerank - updatedPagerank)
      pagerank = updatedPagerank
      receivedMass = 0.0
      diff

    }
  }

}
