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

  performance of "Conc" in {

    measure method "<>" config(
      exec.benchRuns -> 25,
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

    measure method "Buffer" config(
      exec.benchRuns -> 25,
      exec.independentSamples -> 5,
      exec.jvmflags -> "-XX:+UseCondCardMark"
    ) in {
      using(sizes) curve("VectorBuffer") in { sz =>
        var vb = new collection.immutable.VectorBuilder[Unit]()
        var i = 0
        while (i < sz) {
          vb += ()
          i += 1
        }
      }

      using(sizes) curve("Conc.Buffer") in { sz =>
        var cb = new Conc.Buffer[Int]
        var i = 0
        while (i < sz) {
          cb += i
          i += 1
        }
      }
    }

  }

}