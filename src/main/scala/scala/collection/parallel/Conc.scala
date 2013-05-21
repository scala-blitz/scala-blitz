package scala.collection.parallel



import scala.annotation.unchecked.uncheckedVariance
import scala.annotation.tailrec
import scala.reflect.ClassTag



/** Generalized conc-lists.
 *
 *  These are trees representing sequences that have the following properties:
 *  - each node is either a `Zero`, `Single`, `Chunk` or `<>`
 *  - `Zero` represents an empty tree and is a leaf at level 0
 *  - `Single` represents a tree with a single element and is a leaf at level 0
 *  - `Chunk` represents many elements with a single node and is a leaf at level 0
 *  - `<>` represents an internal node -- it has a `left` and a `right` child
 *  - `<>` is at level `math.max(left.level, right.level) + 1`
 *  - `<>` is such that `math.abs(right.level - left.level) <= 1`
 *
 *  Note that the last property ensures that the tree is balanced.
 *  
 *  `Chunk` nodes can be used for the conc lists to serve as ropes.
 *  Note that these ropes are balanced only if the chunk sizes differ up to a constant factor.
 */
sealed abstract class Conc[+T] {
  
  def level: Int
  
  def size: Int
  
  def left: Conc[T]
  
  def right: Conc[T]

  def normalized: Conc[T] = this

  def length = size

  def toString(depth: Int): String = (" " * depth) + this + "\n" + right.toString(depth + 1) + "\n" + left.toString(depth + 1)
}


object Conc {

  implicit class ConcBooleanOps(val conc: Conc[Boolean]) extends AnyVal {
    def <>[U >: Boolean](that: Conc[U]): Conc[U] = Conc.<>.apply(conc, that)
    def <>(elem: Boolean) = Append.apply(conc, new Single(elem))
  }

  implicit class ConcByteOps(val conc: Conc[Byte]) extends AnyVal {
    def <>[U >: Byte](that: Conc[U]): Conc[U] = Conc.<>.apply(conc, that)
    def <>(elem: Byte) = Append.apply(conc, new Single(elem))
  }

  implicit class ConcCharOps(val conc: Conc[Char]) extends AnyVal {
    def <>[U >: Char](that: Conc[U]): Conc[U] = Conc.<>.apply(conc, that)
    def <>(elem: Char) = Append.apply(conc, new Single(elem))
  }

  implicit class ConcShortOps(val conc: Conc[Short]) extends AnyVal {
    def <>[U >: Short](that: Conc[U]): Conc[U] = Conc.<>.apply(conc, that)
    def <>(elem: Short) = Append.apply(conc, new Single(elem))
  }

  implicit class ConcIntOps(val conc: Conc[Int]) extends AnyVal {
    def <>[U >: Int](that: Conc[U]): Conc[U] = Conc.<>.apply(conc, that)
    def <>(elem: Int) = Append.apply(conc, new Single(elem))
  }

  implicit class ConcLongOps(val conc: Conc[Long]) extends AnyVal {
    def <>[U >: Long](that: Conc[U]): Conc[U] = Conc.<>.apply(conc, that)
    def <>(elem: Long) = Append.apply(conc, new Single(elem))
  }

  implicit class ConcFloatOps(val conc: Conc[Float]) extends AnyVal {
    def <>[U >: Float](that: Conc[U]): Conc[U] = Conc.<>.apply(conc, that)
    def <>(elem: Float) = Append.apply(conc, new Single(elem))
  }

  implicit class ConcDoubleOps(val conc: Conc[Double]) extends AnyVal {
    def <>[U >: Double](that: Conc[U]): Conc[U] = Conc.<>.apply(conc, that)
    def <>(elem: Double) = Append.apply(conc, new Single(elem))
  }

  implicit class ConcUnitOps(val conc: Conc[Unit]) extends AnyVal {
    def <>[U >: Unit](that: Conc[U]): Conc[U] = Conc.<>.apply(conc, that)
    def <>(elem: Unit) = Append.apply(conc, new Single(elem))
  }

  implicit class ConcAnyOps[T](val conc: Conc[T]) extends AnyVal {
    def <>[U >: T](that: Conc[U]): Conc[U] = Conc.<>.apply(conc, that)
    def <>(elem: T) = Append.apply(conc, new Single(elem))
  }

  implicit class BooleanConcOps(val elem: Boolean) extends AnyVal {
    def <>[U >: Boolean](that: Conc[U]): Conc[U] = Prepend.apply(new Single(elem), that)
    def <>(that: Boolean) = Append.apply(new Single(elem), new Single(that))
  }

  implicit class ByteConcOps(val elem: Byte) extends AnyVal {
    def <>[U >: Byte](that: Conc[U]): Conc[U] = Prepend.apply(new Single(elem), that)
    def <>(that: Byte) = Append.apply(new Single(elem), new Single(that))
  }

  implicit class CharConcOps(val elem: Char) extends AnyVal {
    def <>[U >: Char](that: Conc[U]): Conc[U] = Prepend.apply(new Single(elem), that)
    def <>(that: Char) = Append.apply(new Single(elem), new Single(that))
  }

  implicit class ShortConcOps(val elem: Short) extends AnyVal {
    def <>[U >: Short](that: Conc[U]): Conc[U] = Prepend.apply(new Single(elem), that)
    def <>(that: Short) = Append.apply(new Single(elem), new Single(that))
  }

  implicit class IntConcOps(val elem: Int) extends AnyVal {
    def <>[U >: Int](that: Conc[U]): Conc[U] = Prepend.apply(new Single(elem), that)
    def <>(that: Int) = Append.apply(new Single(elem), new Single(that))
  }

  implicit class LongConcOps(val elem: Long) extends AnyVal {
    def <>[U >: Long](that: Conc[U]): Conc[U] = Prepend.apply(new Single(elem), that)
    def <>(that: Long) = Append.apply(new Single(elem), new Single(that))
  }

  implicit class FloatConcOps(val elem: Float) extends AnyVal {
    def <>[U >: Float](that: Conc[U]): Conc[U] = Prepend.apply(new Single(elem), that)
    def <>(that: Float) = Append.apply(new Single(elem), new Single(that))
  }

  implicit class DoubleConcOps(val elem: Double) extends AnyVal {
    def <>[U >: Double](that: Conc[U]): Conc[U] = Prepend.apply(new Single(elem), that)
    def <>(that: Double) = Append.apply(new Single(elem), new Single(that))
  }

  implicit class AnyConcOps[T](val elem: T) extends AnyVal {
    def <>[U >: T](that: Conc[U]): Conc[U] = Prepend.apply(new Single(elem), that)
    def <>(that: T) = Append.apply(new Single(elem), new Single(that))
  }

  final case object Zero extends Conc[Nothing] {
    def left = throw new UnsupportedOperationException("Zero.left")
    def right = throw new UnsupportedOperationException("Zero.right")
    def size = 0
    def level = 0
    override def toString(depth: Int) = (" " * depth) + this
  }

  trait Leaf[@specialized(Int, Long, Float, Double) +T] extends Conc[T] {
    override def toString(depth: Int) = (" " * depth) + this

    def elementAt(idx: Int): T
  }

  final case class Single[@specialized(Int, Long, Float, Double) T](elem: T) extends Leaf[T] {
    def left = throw new UnsupportedOperationException("Single.left")
    def right = throw new UnsupportedOperationException("Single.right")
    def size = 1
    def level = 0
    def elementAt(idx: Int) = elem
  }

  final case class Chunk[@specialized(Int, Long, Float, Double) T](elems: Array[T], size: Int) extends Leaf[T] {
    def left = throw new UnsupportedOperationException("Chunk.left")
    def right = throw new UnsupportedOperationException("Chunk.right")
    def level = 0
    def elementAt(idx: Int) = elems(idx)
    override def toString = "Chunk(%s%s; %d)".format(elems.take(10).mkString(", "), if (size > 10) ", ..." else "", size)
  }

  final class <>[T] private[Conc] (val left: Conc[T], val right: Conc[T]) extends Conc[T] {
    val level = {
      val llev = left.level
      val rlev = right.level
      1 + (if (llev > rlev) llev else rlev)
    }
    val size = left.size + right.size
    override def toString = "<>(%d, %d)".format(level, size)
  }

  object <> {
    def unapply[T](c: Conc[T]): Option[(Conc[T], Conc[T])] = c match {
      case c: <>[T] => Some((c.left, c.right))
      case a: Append[T] => Some((c.left, c.right))
      case p: Prepend[T] => Some((c.left, c.right))
      case _ => None
    }
    def apply[T](left: Conc[T], right: Conc[T]): Conc[T] = {
      if (left == Zero) right
      else if (right == Zero) left
      else {
        val lefteval = left.normalized
        val righteval = right.normalized
        construct(lefteval, righteval)
      }
    }
    private def construct[T](left: Conc[T], right: Conc[T]): Conc[T] = {
      val llev = left.level
      val rlev = right.level
      val diff = rlev - llev
      if (diff >= -1 && diff <= 1) new <>(left, right)
      else if (diff < -1) {
        // right.level >= 0
        // => left.level >= 2
        // => (left.left.level >= 1 AND left.right.level >= 0) OR (left.left.level >= 0 AND left.right.level >= 1)
        if (left.left.level >= left.right.level) {
          val lr_r = construct(left.right, right)
          val ll = left.left
          new <>(ll, lr_r)
        } else {
          // => left.right.level >= 1
          val lrr_r = construct(left.right.right, right)
          val ll_lrl = new <>(left.left, left.right.left)
          new <>(ll_lrl, lrr_r)
        }
      } else { // diff > 1
        // left.level >= 0
        // => right.level >= 2
        // => (right.left.level >= 1 AND right.right.level >= 0) OR (right.left.level >= 0 AND right.right.level >= 1)
        if (right.right.level >= right.left.level) {
          val l_rl = construct(left, right.left)
          val rr = right.right
          new <>(l_rl, rr)
        } else {
          // => right.left.level >= 1
          val l_rll = construct(left, right.left.left)
          val rlr_rr = new <>(right.left.right, right.right)
          new <>(l_rll, rlr_rr)
        }
      }
    }
  }

  abstract class Lazy[+T] extends Conc[T]

  final class Append[T] private[Conc] (val left: Conc[T], val right: Conc[T]) extends Lazy[T] {
    val level = {
      val llev = left.level
      val rlev = right.level
      1 + (if (llev > rlev) llev else rlev)
    }
    val size = left.size + right.size
    override def normalized: Conc[T] = {
      @tailrec def fold(l: Conc[T], tree: Conc[T]): Conc[T] = l match {
        case a: Append[T] =>
          fold(a.left, a.right <> tree)
        case c: <>[T] =>
          c <> tree
        case _ => ???
      }

      fold(this.left, this.right)
    }
    override def toString = "Append(%d, %d)".format(level, size)
  }

  object Append {
    def apply[T](left: Conc[T], right: Leaf[T]): Conc[T] = left match {
      case a: Append[T] =>
        val alev = a.right.level
        if (alev > 0) new Append(a, right)
        else construct(a, right)
      case _ =>
        slowpath(left, right)
    }
    private def slowpath[T](left: Conc[T], right: Leaf[T]): Conc[T] = left match {
      case s: Leaf[T] =>
        new <>(s, right)
      case c: <>[T] =>
        new Append(c, right)
      case Zero =>
        right
      case p: Prepend[T] =>
        val n = p.normalized
        Append.apply(n, right)
      case _ =>
        ???
    }
    @tailrec private def construct[T](a: Append[T], r: Conc[T]): Conc[T] = {
      val merged = new <>(a.right, r)
      a.left match {
        case al: Append[T] =>
          val allev = al.right.level
          if (allev > merged.level) new Append(al, merged)
          else construct(al, merged)
        case c: <>[T] =>
          new Append(c, merged)
        case _ => ???
      }
    }
  }

  final class Prepend[T] private[Conc] (val left: Conc[T], val right: Conc[T]) extends Lazy[T] {
    val level = {
      val llev = left.level
      val rlev = right.level
      1 + (if (llev > rlev) llev else rlev)
    }
    val size = left.size + right.size
    override def normalized: Conc[T] = {
      @tailrec def fold(tree: Conc[T], r: Conc[T]): Conc[T] = r match {
        case p: Prepend[T] =>
          fold(tree <> p.left, p.right)
        case c: <>[T] =>
          tree <> r
        case _ => ???
      }

      fold(this.left, this.right)
    }
    override def toString = "Prepend(%d, %d)".format(level, size)
  }

  object Prepend {
    def apply[T](left: Leaf[T], right: Conc[T]): Conc[T] = right match {
      case p: Prepend[T] =>
        val plev = p.left.level
        if (plev > 0) new Prepend(left, p)
        else construct(left, p)
      case _ =>
        slowpath(left, right)
    }
    private def slowpath[T](left: Leaf[T], right: Conc[T]): Conc[T] = right match {
      case s: Leaf[T] =>
        new <>(left, s)
      case c: <>[T] =>
        new Prepend(left, c)
      case Zero =>
        left
      case a: Append[T] =>
        val n = a.normalized
        Prepend.apply(left, n)
      case _ =>
        ???
    }
    @tailrec private def construct[T](l: Conc[T], p: Prepend[T]): Conc[T] = {
      val merged = new <>(l, p.left)
      p.right match {
        case pr: Prepend[T] =>
          val prlev = pr.left.level
          if (prlev > merged.level) new Prepend(merged, pr)
          else construct(merged, pr)
        case c: <>[T] =>
          new Prepend(merged, c)
        case _ => ???
      }
    }
  }

  val INITIAL_SIZE = 8
  val DEFAULT_MAX_SIZE = 4096

  abstract class BufferLike[T, +To, Repr <: BufferLike[T, To, Repr]] extends MergerLike[T, To, Repr] {
    private[parallel] val maxChunkSize: Int
    private[parallel] var conc: Conc[T]
    private[parallel] var lastChunk: Array[T]
    private[parallel] var lastSize: Int

    implicit def classTag: ClassTag[T]

    def newBuffer(c: Conc[T]): Repr

    private[parallel] final def pack() {
      if (lastSize > 0) conc = Append.apply(conc, new Chunk(lastChunk, lastSize))
    }

    private[parallel] final def expand() {
      val oldlength = lastChunk.length
      val newlength = math.min(maxChunkSize, oldlength * 2)
      pack()
      lastChunk = new Array[T](newlength)
      lastSize = 0
    }

    def clear() = {
      lastChunk = new Array[T](8)
      lastSize = 0
      conc = Zero
    }

    def merge(that: Repr) = {
      this.pack()
      that.pack()

      val resconc = this.conc <> that.conc
      val res = newBuffer(resconc)

      this.clear()
      that.clear()

      res
    }

    def +=(elem: T): Repr
    def result: To
  }

  final class Buffer[@specialized(Int, Long, Float, Double) T: ClassTag](
    private[parallel] val maxChunkSize: Int,
    private[parallel] var conc: Conc[T],
    private[parallel] var lastChunk: Array[T],
    private[parallel] var lastSize: Int
  ) extends BufferLike[T, Conc[T], Buffer[T]] {
    def classTag = implicitly[ClassTag[T]]

    def this(mcs: Int) = this(mcs, Zero, new Array[T](INITIAL_SIZE), 0)

    def this() = this(DEFAULT_MAX_SIZE)

    def newBuffer(conc: Conc[T]) = new Buffer(maxChunkSize, conc, new Array[T](INITIAL_SIZE), 0)

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
      res
    }

  }

}








