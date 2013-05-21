package scala.collection.parallel
package workstealing



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.reflect.ClassTag



object Arrays {

  trait Scope {
    implicit def arrayOps[T](a: Par[Array[T]]) = ???
    
    implicit def array2zippable[T](a: Par[Array[T]]) = ???
  }

  final class Merger[@specialized(Int, Long, Float, Double) T: ClassTag](
    private[parallel] val maxChunkSize: Int,
    private[parallel] var conc: Conc[T],
    private[parallel] var lastChunk: Array[T],
    private[parallel] var lastSize: Int,
    private val ctx: WorkstealingTreeScheduler
  ) extends Conc.BufferLike[T, Array[T], Merger[T]] {
    def classTag = implicitly[ClassTag[T]]

    def this(mcs: Int, ctx: WorkstealingTreeScheduler) = this(mcs, Conc.Zero, new Array[T](Conc.INITIAL_SIZE), 0, ctx)

    def this(ctx: WorkstealingTreeScheduler) = this(Conc.DEFAULT_MAX_SIZE, ctx)

    def newBuffer(conc: Conc[T]) = new Merger(maxChunkSize, conc, new Array[T](Conc.INITIAL_SIZE), 0, ctx)

    final def +=(elem: T) = if (lastSize < lastChunk.length) {
      lastChunk(lastSize) = elem
      lastSize += 1
      this
    } else {
      expand()
      this += elem
    }

    def result: Array[T] = {
      import Ops._
      import Par._

      pack()
      val c = conc
      clear()

      val array = new Array[T](c.size)
      c.toPar.genericCopyToArray(array, 0, array.length)(ctx)
      array
    }

  }

  // array ops implementations

}