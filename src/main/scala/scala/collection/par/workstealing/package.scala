package scala.collection.par



import sun.misc.Unsafe
import scala.reflect.macros._



package object workstealing {

}


package workstealing {
  
  class ResultCell[@specialized T] {
    private var r: T = _
    private var empty = true
    def result: T = r
    def result_=(v: T) = {
      r = v
      empty = false
    }
    def isEmpty = empty
    override def toString = if (empty) "ResultCell(empty)" else "ResultCell(" + r + ")"
  }

  object ResultFound extends Scheduler.TerminationCause {
    def validateResult[R](r: R) = if (r.isInstanceOf[Option[_]]) r else ???
  }

  final case class ProgressStatus(val start: Int, var progress: Int)

}

