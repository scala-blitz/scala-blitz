package scala.collection.parallel



import scala.reflect.ClassTag
import Par._
import workstealing.WorkstealingTreeScheduler
import workstealing.WorkstealingTreeScheduler.Config
import workstealing.Ops._


import org.scalameter.{ Reporter, Gen, PerformanceTest }
import org.scalameter.persistence.SerializationPersistor
import org.scalameter.api._
import java.awt.Color
import org.scalameter.reporting.{ ChartReporter, HtmlReporter, RegressionReporter }



/**
 * Author: Dmitry Petrashko
 * Date: 21.03.13
 *
 * PageRank test
 */

object PageRank extends PerformanceTest.Regression with Serializable  with scalameter.Generators{

  /* configuration */

  lazy val persistor = new SerializationPersistor

  

  /* inputs */

  val data =  generateData(1000)

  /* tests  */
 
  performance of "PageRank" config (exec.independentSamples -> 4, exec.benchRuns -> 20, exec.jvmflags -> "-Xms2048M -Xmx2048M") in {

    measure method "time" in {

     using(data) curve ("Sequential") in {
        data =>
          getPageRankSequential(data)
      }
      using(withSchedulers(data)) curve ("Par") in {
        datas =>
          getPageRankNew(datas._1)(datas._2)
      }
    }

  }


  def generateData(from:Int, prob:Double = 0.05) = {
    val generator = new java.util.Random(42)
    for (size <- sizes(from)) yield 
    (for(i<-0 until size) 
      yield (for(j<-0 until size; if (j!=i&& generator.nextFloat() < prob)) yield (j)).toArray
    ).toArray
  }


  def getPageRankSequential(graph: Array[Array[Int]], maxIters: Int = 50, jumpFactor: Double = .15, diffTolerance: Double = 1E-9) = {

    // Precompute some values that will be used often for the updates.
    val numVertices = graph.size
    val uniformProbability = 1.0 / numVertices
    val jumpTimesUniform = jumpFactor / numVertices
    val oneMinusJumpFactor = 1.0 - jumpFactor

    // Create the vertex, and put in a map so we can get them by ID.
    val vertices = graph.zipWithIndex.map {
      case (adjacencyList, vertexId) =>
        val vertex = new Vertex(adjacencyList, uniformProbability, vertexId)
        vertex
    }

    var done = false
    var currentIteration = 1
    val result = StringBuilder.newBuilder

    while (!done) {
      // Tell all vertices to spread their mass and get back the missing mass.
      val redistributedMassPairs = vertices.flatMap { x => x.spreadMass }

      val totalMissingMass = vertices.map{x=> x.missingMass}.sum
      val eachVertexRedistributedMass = totalMissingMass / numVertices
      val redistributedMass = redistributedMassPairs.groupBy(x => x._1).map { x => (x._1, x._2.aggregate(0.0)({ (x, y) => x + y._2 }, _ + _)) }
      redistributedMass.foreach { x => vertices(x._1).takeMass(x._2) }
      val diffs = vertices.map { x => x.Update(jumpTimesUniform, oneMinusJumpFactor, eachVertexRedistributedMass) }

      val averageDiff =  diffs.sum / numVertices
//      println("Iteration " + currentIteration        + ": average diff == " + averageDiff)
      currentIteration += 1
      if (currentIteration > maxIters || averageDiff < diffTolerance) {
        done = true
      }
    }
    vertices
  }

  def getPageRankNew(graph: Array[Array[Int]], maxIters: Int = 50, jumpFactor: Double = .15, diffTolerance: Double = 1E-9)(implicit s: WorkstealingTreeScheduler) = {

    // Precompute some values that will be used often for the updates.
    val numVertices = graph.size
    val uniformProbability = 1.0 / numVertices
    val jumpTimesUniform = jumpFactor / numVertices
    val oneMinusJumpFactor = 1.0 - jumpFactor

    // Create the vertex, and put in a map so we can get them by ID.
    val vertices = graph.zipWithIndex.toPar.map {
      case (adjacencyList, vertexId) =>
        val vertex = new Vertex(adjacencyList, uniformProbability, vertexId)
        vertex
    }

    var done = false
    var currentIteration = 1
    val result = StringBuilder.newBuilder

    while (!done) {
      // Tell all vertices to spread their mass and get back the missing mass.
      val redistributedMassPairs = vertices flatMap { x => x.spreadMass }

      val totalMissingMass = vertices.mapReduce(x => x.missingMass)(_ + _)
      val eachVertexRedistributedMass = totalMissingMass / numVertices
      val redistributedMass = redistributedMassPairs.groupMapAggregate(x => x._1)(x => x._2)((x, y) => x + y)
      redistributedMass.foreach { x => vertices.seq(x._1).takeMass(x._2) }
      val diffs = vertices.map { x => x.Update(jumpTimesUniform, oneMinusJumpFactor, eachVertexRedistributedMass) }

      val averageDiff =  diffs.sum / numVertices
//      println("Iteration " + currentIteration        + ": average diff == " + averageDiff)
      currentIteration += 1
      if (currentIteration > maxIters || averageDiff < diffTolerance) {
        done = true
      }
    }
    vertices
  }


  class Vertex(var neighbors: Array[Int], var pagerank: Double, id: Int) {

    var outdegree = neighbors.length
    var receivedMass = 0.0

    def missingMass = if (outdegree == 0) pagerank else 0.0

    def spreadMass = {
      if (outdegree == 0)  Array.empty[(Int,Double)]
      else {
        val amountPerNeighbor = pagerank / outdegree
        neighbors.map(x => (x, amountPerNeighbor))
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
