package org.scala.optimized.test.par
package scalameter


import scala.collection.par._
import org.scalameter.api._
import org.scalameter.PerformanceTest.OnlineRegressionReport


class ConcBench extends OnlineRegressionReport with Serializable {
  import Scheduler.Config

  /* generators */

  val sizes = Gen.enumeration("size")(500000, 1000000, 1500000)
  val concs = for (size <- sizes) yield {
    var conc: Conc[Int] = Conc.Zero
    for (i <- 0 until size) conc = conc <> i
    conc
  }
  val normalizedConcs = for (conc <- concs) yield conc.normalized
  val bufferConcs = for (size <- sizes) yield {
    var cb = new Conc.Buffer[Int]
    for (i <- 0 until size) cb += i
    cb.result.normalized
  }

  /* tests */

  performance of "Conc" config(
    reports.regression.noiseMagnitude -> 0.20
  ) in {

    measure method "append-element" config(
      exec.benchRuns -> 35,
      exec.independentSamples -> 5,
      exec.jvmflags -> "-XX:+UseCondCardMark"
    ) in {
      using(sizes) curve("List") in { sz =>
        var list: List[Unit] = Nil
        var i = 0
        while (i < sz) {
          list ::= (())
          i += 1
        }
      }

      using(sizes) curve("Vector") in { sz =>
        var v = collection.immutable.Vector[Unit]()
        var i = 0
        while (i < sz) {
          v = v :+ (())
          i += 1
        }
      }

      using(sizes) curve("Conc") in { sz =>
        import Conc._
        var conc: Conc[Int] = Zero
        var i = 0
        while (i < sz) {
          conc = conc <> i
          i += 1
        }
      }
    }

    performance of "Buffer" config(
      exec.minWarmupRuns -> 50,
      exec.maxWarmupRuns -> 100,
      exec.benchRuns -> 90,
      exec.independentSamples -> 15,
      exec.jvmflags -> "-XX:+UseCondCardMark"
    ) in {
      using(sizes) curve("VectorBuffer") in { sz =>
        val vb = new collection.immutable.VectorBuilder[Unit]()
        var i = 0
        while (i < sz) {
          vb += (())
          i += 1
        }
      }

      using(sizes) curve("ArrayBuffer") in { sz =>
        val ab = new collection.mutable.ArrayBuffer[Unit]()
        var i = 0
        while (i < sz) {
          ab += (())
          i += 1
        }
      }

      using(sizes) curve("Conc.Buffer") in { sz =>
        val cb = new Conc.Buffer[Int]
        var i = 0
        while (i < sz) {
          cb += i
          i += 1
        }
      }
    }

    measure method "merge" config(
      exec.benchRuns -> 35,
      exec.independentSamples -> 5,
      exec.jvmflags -> "-XX:+UseCondCardMark"
    ) in {
      using(concs) curve("Conc") in { conc =>
        var i = 0
        while (i < 10000) {
          conc <> conc
          i += 1
        }
      }
    }

    performance of "append-normalize-merge" config(
      exec.benchRuns -> 35,
      exec.independentSamples -> 5,
      exec.jvmflags -> "-XX:+UseCondCardMark"
    ) in {
      using(sizes) curve("Conc") in { sz =>
        import Conc._

        // build
        var conc: Conc[Int] = Zero
        var i = 0
        while (i < sz) {
          conc = conc <> i
          i += 1
        }

        // normalize and merge
        i = 0
        while (i < 10000) {
          conc <> conc
          i += 1
        }
      }
    }

    final class SumKernel extends scala.collection.par.workstealing.Concs.ConcKernel[Int, Int] {
      def zero = 0
      def combine(a: Int, b: Int) = a + b
      final def applyTree(t: Conc[Int], remaining: Int, acc: Int) = t match {
        case c: Conc.<>[Int] =>
          val l = applyTree(c.left, remaining, acc)
          val r = applyTree(c.right, remaining - c.left.size, acc)
          l + r
        case c: Conc.Single[Int] =>
          c.elem
        case c: Conc.Chunk[Int] =>
          applyChunk(c, 0, remaining)
        case _ =>
          ???
      }
      final def applyChunk(c: Conc.Chunk[Int], from: Int, remaining: Int, acc: Int) = {
        val sum = applyChunk(c, from, remaining)
        acc + sum
      }
      final def applyChunk(c: Conc.Chunk[Int], from: Int, remaining: Int) = {
        var i = from
        val until = math.min(from + remaining, c.size)
        var a = c.elems
        var sum = 0
        while (i < until) {
          sum += a(i)
          i += 1
        }
        sum
      }

      def sum(c: Conc[Int]) {
        val stealer = new scala.collection.par.workstealing.Concs.ConcStealer[Int](c, 0, c.size)
        val node = new scala.collection.par.Scheduler.Node[Int, Int](null, null)(stealer)

        while (stealer.isAvailable) {
          stealer.nextBatch(4096)
          this.apply(node, c.size)
        }
      }
    }

    final class SequentialSum {
      final def sum(c: Conc[Int]): Int = c match {
        case c: Conc.Single[Int] =>
          c.elem
        case c: Conc.<>[Int] =>
          sum(c.left) + sum(c.right)
        case c: Conc.Chunk[Int] =>
          var i = 0
          val until = c.size
          var a = c.elems
          var sum = 0
          while (i < until) {
            sum += a(i)
            i += 1
          }
          sum
        case _ => sys.error(c.toString)
      }
    }

    class StealerSum {
      def sum(c: Conc[Int]) = {
        val stealer = new scala.collection.par.workstealing.Concs.ConcStealer[Int](c, 0, c.size)
        var sum = 0
        stealer.nextBatch(c.size)
        while (stealer.hasNext) {
          sum += stealer.next()
        }
        if (sum == 0) ???
      }
    }

    performance of "Kernel" config(
      exec.minWarmupRuns -> 10,
      exec.maxWarmupRuns -> 20,
      exec.benchRuns -> 120,
      exec.independentSamples -> 10,
      exec.reinstantiation.frequency -> 2
    ) in {

      performance of "Buffer" config (
        exec.minWarmupRuns -> 50,
        exec.maxWarmupRuns -> 100
      ) in {
        using(bufferConcs) curve("Kernel") in { c =>
          (new SumKernel).sum(c)
        }
  
        using(bufferConcs) curve("Stealer") in { c =>
          (new StealerSum).sum(c)
        }
  
        using(bufferConcs) curve("Seq") in { root =>
          (new SequentialSum).sum(root)
        }
      }

      performance of "Append" in {
        using(normalizedConcs) curve("Kernel") in { c =>
          (new SumKernel).sum(c)
        }
  
        using(normalizedConcs) curve("Stealer") in { c =>
          (new StealerSum).sum(c)
        }
  
        using(normalizedConcs) curve("Seq") in { root =>
          (new SequentialSum).sum(root)
        }
      }
    }

  }

}





