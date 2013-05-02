package scala.collection.parallel
package scalameter



import org.scalameter.api._



class ConcBench extends PerformanceTest.Regression with Serializable {
  import Par._
  import workstealing.WorkstealingTreeScheduler
  import workstealing.WorkstealingTreeScheduler.Config

  /* config */

  def persistor = new SerializationPersistor

  /* generators */

  val sizes = Gen.enumeration("size")(500000, 1000000, 1500000)
  val concs = for (size <- sizes) yield {
    var conc: Conc[Int] = Conc.Zero
    for (i <- 0 until size) conc = conc <> i
    conc
  }

  performance of "Conc" in {

    measure method "append-element" config(
      exec.benchRuns -> 35,
      exec.independentSamples -> 5,
      exec.jvmflags -> "-XX:+UseCondCardMark"
    ) in {
      using(sizes) curve("List") in { sz =>
        var list: List[Unit] = Nil
        var i = 0
        while (i < sz) {
          list ::= ()
          i += 1
        }
      }

      using(sizes) curve("Vector") in { sz =>
        var v = collection.immutable.Vector[Unit]()
        var i = 0
        while (i < sz) {
          v = v :+ ()
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
      exec.benchRuns -> 35,
      exec.independentSamples -> 5,
      exec.jvmflags -> "-XX:+UseCondCardMark"
    ) in {
      using(sizes) curve("VectorBuffer") in { sz =>
        val vb = new collection.immutable.VectorBuilder[Unit]()
        var i = 0
        while (i < sz * 2) {
          vb += ()
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

  }

}





