package org.scala.optimized.test.par
package scalameter



import scala.collection.par._
import scala.reflect.ClassTag
import Scheduler.Config
import org.scalameter.{ Reporter, Gen, PerformanceTest }
import org.scalameter.persistence.SerializationPersistor
import org.scalameter.api._
import scala.collection.parallel._
import java.awt.Color
import org.scalameter.reporting.{ ChartReporter, HtmlReporter, RegressionReporter }
import org.scalameter.PerformanceTest.OnlineRegressionReport

/**
 * Author: Dmitry Petrashko
 * Date: 21.03.13
 *
 * PageRank test
 */

object PageRank extends OnlineRegressionReport with Serializable with scalameter.Generators {

  /* inputs */

  val data = generateData(1000)

  /* tests  */

  performance of "PageRank" config (exec.independentSamples -> 4, exec.benchRuns -> 20, exec.jvmflags -> "-Xms3072M -Xmx3072M") in {

    measure method "time" in {

      using(data) curve ("Sequential") in {
        data =>
          getPageRankSequential(data)
      }

      using(data) curve ("SequentialOpt") in {
        data =>
          getPageRankSequentialOpt(data)
      }

      using(withTaskSupports(data)) curve ("PC") in {
        data =>
          getPageRankOldPC(data._1)(data._2)
      }

      using(withTaskSupports(data)) curve ("PCOpt") in {
        data =>
          getPageRankOldPCOpt(data._1)(data._2)
      }

      using(withSchedulers(data)) curve ("Par") in {
        datas =>
          getPageRankNew(datas._1)(datas._2)
      }
      using(withSchedulers(data)) curve ("ParOpt") in {
        datas =>
          getPageRankNewOpt(datas._1)(datas._2)
      }

    }

  }

  def generateData(from: Int, prob: Double = 0.05) = {
    val generator = new java.util.Random(42)
    for (size <- sizes(from)) yield (for (i <- 0 until size)
      yield (for (j <- 0 until size; if (j != i && generator.nextFloat() < prob)) yield (j)).toArray).toArray
  }

  def getPageRankSequentialOpt(graph: Array[Array[Int]], maxIters: Int = 50, jumpFactor: Double = .15, diffTolerance: Double = 1E-9) = {

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

      val totalMissingMass = vertices.map { x => x.missingMass }.sum
      val eachVertexRedistributedMass = totalMissingMass / numVertices
      //      val redistributedMass = redistributedMassPairs.groupBy(x => x._1).map { x => (x._1, x._2.aggregate(0.0)({ (x, y) => x + y._2 }, _ + _)) }
      redistributedMassPairs.foreach { x => vertices(x._1).takeMass(x._2) }
      val diffs = vertices.map { x => x.Update(jumpTimesUniform, oneMinusJumpFactor, eachVertexRedistributedMass) }

      val averageDiff = diffs.sum / numVertices
      //      println("Iteration " + currentIteration        + ": average diff == " + averageDiff)
      currentIteration += 1
      if (currentIteration > maxIters || averageDiff < diffTolerance) {
        done = true
      }
    }
    vertices
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

      val totalMissingMass = vertices.map { x => x.missingMass }.sum
      val eachVertexRedistributedMass = totalMissingMass / numVertices
      val redistributedMass = redistributedMassPairs.groupBy(x => x._1).map { x => (x._1, x._2.aggregate(0.0)({ (x, y) => x + y._2 }, _ + _)) }
      redistributedMass.foreach { x => vertices(x._1).takeMass(x._2) }
      val diffs = vertices.map { x => x.Update(jumpTimesUniform, oneMinusJumpFactor, eachVertexRedistributedMass) }

      val averageDiff = diffs.sum / numVertices
      //      println("Iteration " + currentIteration        + ": average diff == " + averageDiff)
      currentIteration += 1
      if (currentIteration > maxIters || averageDiff < diffTolerance) {
        done = true
      }
    }
    vertices
  }

  def getPageRankNew(graph: Array[Array[Int]], maxIters: Int = 50, jumpFactor: Double = .15, diffTolerance: Double = 1E-9)(implicit s: Scheduler) = {

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

      val averageDiff = diffs.sum / numVertices
      //      println("Iteration " + currentIteration        + ": average diff == " + averageDiff)
      currentIteration += 1
      if (currentIteration > maxIters || averageDiff < diffTolerance) {
        done = true
      }
    }
    vertices
  }

  def getPageRankNewOpt(graph: Array[Array[Int]], maxIters: Int = 50, jumpFactor: Double = .15, diffTolerance: Double = 1E-9)(implicit s: Scheduler) = {

    // Precompute some values that will be used often for the updates. 
    val numVertices = graph.size
    val uniformProbability = 1.0 / numVertices
    val jumpTimesUniform = jumpFactor / numVertices
    val oneMinusJumpFactor = 1.0 - jumpFactor

    // Create the vertex, and put in a map so we can get them by ID.
    val vertices = graph.zipWithIndex.toPar.map {
      case (adjacencyList, vertexId) =>
        val vertex = new VertexAtomic(adjacencyList, uniformProbability, vertexId)
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
      redistributedMassPairs.foreach { x => vertices.seq(x._1).takeMass(x._2) }
      val diffs = vertices.map { x => x.Update(jumpTimesUniform, oneMinusJumpFactor, eachVertexRedistributedMass) }

      val averageDiff = diffs.sum / numVertices
      //      println("Iteration " + currentIteration        + ": average diff == " + averageDiff)
      currentIteration += 1
      if (currentIteration > maxIters || averageDiff < diffTolerance) {
        done = true
      }
    }
    vertices
  }

  def getPageRankOldPC(graph: Array[Array[Int]], ntop: Int = 20, maxIters: Int = 50, jumpFactor: Double = .15, diffTolerance: Double = 1E-9)(taskSupport: ForkJoinTaskSupport) = {

    // Precompute some values that will be used often for the updates.
    val numVertices = graph.size
    val uniformProbability = 1.0 / numVertices
    val jumpTimesUniform = jumpFactor / numVertices
    val oneMinusJumpFactor = 1.0 - jumpFactor

    // Create the vertex actors, and put in a map so we can
    // get them by ID.

    val vertices =
      {
        val graphWithId = graph.zipWithIndex.par
        graphWithId.tasksupport = taskSupport
        graphWithId.map {
          case (adjacencyList, vertexId) =>
            val vertex = new Vertex(adjacencyList, uniformProbability, vertexId)
            vertex
        }
      }
    vertices.tasksupport = taskSupport

    // The list of vertex actors, used for dispatching messages to all.

    var done = false
    var currentIteration = 1
    val result = StringBuilder.newBuilder

    while (!done) {
      // Tell all vertices to spread their mass and get back the missing mass.
      val redistributedMassPairs = vertices.flatMap { x => x.spreadMass }

      redistributedMassPairs.tasksupport = taskSupport
      val totalMissingMass = {
        val mappedVertices = vertices.map { x => x.missingMass }
        mappedVertices.tasksupport = taskSupport
        mappedVertices.sum
      }
      val eachVertexRedistributedMass = totalMissingMass / numVertices
      val redistributedMass = {
        val groupped = redistributedMassPairs.groupBy(x => x._1)
        groupped.tasksupport = taskSupport
        groupped.map { x =>
          x._2.tasksupport = taskSupport
          (x._1, x._2.aggregate(0.0)({ (x, y) => x + y._2 }, _ + _))
        }
      }
      redistributedMass.tasksupport = taskSupport
      redistributedMass.foreach { x => vertices(x._1).takeMass(x._2) }
      val diffs = vertices.map { x => x.Update(jumpTimesUniform, oneMinusJumpFactor, eachVertexRedistributedMass) }
      diffs.tasksupport = taskSupport
      val averageDiff = diffs.sum / numVertices
      //      println("Iteration " + currentIteration        + ": average diff == " + averageDiff)
      currentIteration += 1
      if (currentIteration > maxIters || averageDiff < diffTolerance) {
        done = true
      }
    }

  }

  def getPageRankOldPCOpt(graph: Array[Array[Int]], ntop: Int = 20, maxIters: Int = 50, jumpFactor: Double = .15, diffTolerance: Double = 1E-9)(taskSupport: ForkJoinTaskSupport) = {

    // Precompute some values that will be used often for the updates.
    val numVertices = graph.size
    val uniformProbability = 1.0 / numVertices
    val jumpTimesUniform = jumpFactor / numVertices
    val oneMinusJumpFactor = 1.0 - jumpFactor

    // Create the vertex actors, and put in a map so we can
    // get them by ID.
    val vertices = {
      val graphWithId = graph.zipWithIndex.par
      graphWithId.tasksupport = taskSupport
      graphWithId.map {
        case (adjacencyList, vertexId) =>
          val vertex = new VertexAtomic(adjacencyList, uniformProbability, vertexId)
          vertex
      }
    }

    // The list of vertex actors, used for dispatching messages to all.

    var done = false
    var currentIteration = 1
    val result = StringBuilder.newBuilder

    while (!done) {
      // Tell all vertices to spread their mass and get back the missing mass.
      val redistributedMassPairs = vertices.flatMap { x => x.spreadMass }
      redistributedMassPairs.tasksupport = taskSupport
      val totalMissingMass = {
        val mappedVertices = vertices.map { x => x.missingMass }
        mappedVertices.tasksupport = taskSupport
        mappedVertices.sum
      }
      val eachVertexRedistributedMass = totalMissingMass / numVertices
      redistributedMassPairs.foreach { x => vertices(x._1).takeMass(x._2) }
      val diffs = vertices.map { x => x.Update(jumpTimesUniform, oneMinusJumpFactor, eachVertexRedistributedMass) }
      diffs.tasksupport = taskSupport
      val averageDiff = diffs.sum / numVertices
      //      println("Iteration " + currentIteration        + ": average diff == " + averageDiff)
      currentIteration += 1
      if (currentIteration > maxIters || averageDiff < diffTolerance) {
        done = true
      }
    }

  }

  class Vertex(var neighbors: Array[Int], var pagerank: Double, id: Int) {

    var outdegree = neighbors.length
    var receivedMass = 0.0

    def missingMass = if (outdegree == 0) pagerank else 0.0

    def spreadMass = {
      if (outdegree == 0) Array.empty[(Int, Double)]
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

  class VertexAtomic(var neighbors: Array[Int], var pagerank: Double, id: Int) {

    def long2Double(l: Long) = java.lang.Double.longBitsToDouble(l)
    def double2Long(d: Double) = java.lang.Double.doubleToRawLongBits(d)

    var outdegree = neighbors.length

    //    var receivedMass = 0.0
    val holder = new java.util.concurrent.atomic.AtomicLong(double2Long(0.0))

    def missingMass = if (outdegree == 0) pagerank else 0.0

    def spreadMass = {
      if (outdegree == 0) Array.empty[(Int, Double)]
      else {
        val amountPerNeighbor = long2Double(holder.get) / outdegree
        neighbors.map(x => (x, amountPerNeighbor))
      }
    }

    def takeMass(contribution: Double) = {
      var done = false
      while (!done) {
        val oldValue = holder.get
        val newValue = long2Double(oldValue) + contribution
        done = holder.compareAndSet(oldValue, double2Long(newValue))
      }
    }

    def getPageRank = (id, pagerank)
    def Update(jumpTimesUniform: Double, oneMinusJumpFactor: Double, redistributedMass: Double) = {
      val updatedPagerank =
        jumpTimesUniform + oneMinusJumpFactor * (redistributedMass + long2Double(holder.get))
      val diff = math.abs(pagerank - updatedPagerank)
      pagerank = updatedPagerank
      holder.set(double2Long(0.0))
      diff

    }
  }

}
