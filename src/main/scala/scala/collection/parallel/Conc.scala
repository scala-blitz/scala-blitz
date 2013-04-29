package scala.collection.parallel



import scala.annotation.unchecked.uncheckedVariance



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

  def evaluated: Conc[T] = this

  def toString(depth: Int): String = (" " * depth) + this + "\n" + right.toString(depth + 1) + "\n" + left.toString(depth + 1)

}


object Conc {

  implicit class concIntOps(val conc: Conc[Int]) extends AnyVal {
    final def <>[U >: Int](that: Conc[U]): Conc[U] = Conc.<>.apply(conc, that)
    def <>(elem: Int) = Append.apply(conc, new Single(elem))
  }

  implicit class concAnyRefOps[T <: AnyRef](val conc: Conc[T]) extends AnyVal {
    final def <>[U >: T](that: Conc[U]): Conc[U] = Conc.<>.apply(conc, that)
    def <>(elem: T) = Append.apply(conc, new Single(elem))
  }

  final case object Zero extends Conc[Nothing] {
    def left = throw new UnsupportedOperationException("Zero.left")
    def right = throw new UnsupportedOperationException("Zero.right")
    def size = 0
    def level = 0
    override def toString(depth: Int) = (" " * depth) + this
  }

  abstract class Leaf[T] extends Conc[T] {
    override def toString(depth: Int) = (" " * depth) + this
  }

  final case class Single[@specialized T](elem: T) extends Leaf[T] {
    def left = throw new UnsupportedOperationException("Single.left")
    def right = throw new UnsupportedOperationException("Single.right")
    def size = 1
    def level = 0
  }

  final class Chunk[@specialized T](val elems: Array[T], val size: Int) extends Leaf[T] {
    def left = throw new UnsupportedOperationException("Chunk.left")
    def right = throw new UnsupportedOperationException("Chunk.right")
    def level = 0
    override def toString = "Chunk(%s, %d)".format(elems, size)
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
      case _ => None
    }
    def apply[T](left: Conc[T], right: Conc[T]): Conc[T] = {
      if (left == Zero) right
      else if (right == Zero) left
      else {
        val lefteval = left.evaluated
        val righteval = right.evaluated
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

  final class Append[T] private (val left: Conc[T], val right: Conc[T]) extends Conc[T] {
    val level = {
      val llev = left.level
      val rlev = right.level
      1 + (if (llev > rlev) llev else rlev)
    }
    val size = left.size + right.size
    override def toString = "Append(%d, %d)".format(level, size)
  }

  object Append {
    def apply[T](left: Conc[T], right: Single[T]): Conc[T] = left match {
      case a: Append[T] =>
        val alev = a.right.level
        if (alev > 0) new Append(a, right)
        else construct1(a, right, alev, 0)
      case s: Leaf[T] =>
        new <>(s, right)
      case c: <>[T] =>
        new Append(c, right)
      case Zero =>
        right
      case _ =>
        ???
    }
    private def construct[T](a: Append[T], r: Conc[T]): Append[T] = {
      val rlev = r.level
      val alev = a.right.level
      if (alev > rlev) new Append(a, r)
      else construct1(a, r, alev, rlev)
    }
    private def construct1[T](a: Append[T], r: Conc[T], alev: Int, rlev: Int): Append[T] = {
      a.left match {
        case al: Append[T] =>
          val allev = al.right.level
          if (allev > alev) new Append(a, r)
          else {
            val merged = new <>(al.right, a.right)
            al.left match {
              case all: Append[T] =>
                val pushed = construct(all, merged)
                new Append(pushed, r)
              case c: <>[T] =>
                val pushed = new Append(c, merged)
                new Append(pushed, r)
              case _ => ???
            }
          }
        case _: <>[T] =>
          new Append(a, r)
        case _ => ???
      }
    }
  }

}








