package scala.collection.parallel
package workstealing



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
    private[parallel] var lastSize: Int
  ) extends Conc.BufferLike[T, Conc[T], Merger[T]] {
    def classTag = implicitly[ClassTag[T]]

    def this(mcs: Int) = this(mcs, Conc.Zero, new Array[T](Conc.INITIAL_SIZE), 0)

    def this() = this(Conc.DEFAULT_MAX_SIZE)

    def newBuffer(conc: Conc[T]) = new Merger(maxChunkSize, conc, new Array[T](Conc.INITIAL_SIZE), 0)

    final def +=(elem: T) = if (lastSize < lastChunk.length) {
      lastChunk(lastSize) = elem
      lastSize += 1
      this
    } else {
      expand()
      this += elem
    }

    def result = {
      pack()
      val res = conc
      clear()

      ???
    }

  }

  // array ops implementations

}