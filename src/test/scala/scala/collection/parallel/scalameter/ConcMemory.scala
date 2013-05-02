package scala.collection.parallel
package scalameter



import org.scalameter.api._



class ConcMemory extends PerformanceTest.Regression with Serializable {
  import Par._
  import workstealing.WorkstealingTreeScheduler
  import workstealing.WorkstealingTreeScheduler.Config

  /* config */

  override def measurer = new Executor.Measurer.MemoryFootprint
  def persistor = new SerializationPersistor

  /* generators */

  val sizes = Gen.enumeration("size")(500000, 1000000, 1500000)

  /* tests */

  performance of "Conc" in {

    measure method "memory" config(
      exec.benchRuns -> 6,
      exec.independentSamples -> 2,
      exec.jvmflags -> "-XX:+UseCondCardMark"
    ) in {
      using(sizes) curve("List") in { sz =>
        var list: List[Unit] = Nil
        var i = 0
        while (i < sz) {
          list ::= ()
          i += 1
        }
        list
      }

      using(sizes) curve("Vector") in { sz =>
        var v = collection.immutable.Vector[Unit]()
        var i = 0
        while (i < sz) {
          v = v :+ ()
          i += 1
        }
        v
      }

      using(sizes) curve("Conc") in { sz =>
        import Conc._
        var conc: Conc[Int] = Zero
        var i = 0
        while (i < sz) {
          conc = conc <> i
          i += 1
        }
        conc
      }

      using(sizes) curve("Buffer") in { sz =>
        val cb = new Conc.Buffer[Int]
        var i = 0
        while (i < sz) {
          cb += i
          i += 1
        }
        cb
      }
    }
  }

}





