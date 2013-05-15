package scala.collection.parallel
package workstealing



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.annotation.tailrec



object Concs {

  trait Scope {
    implicit def concOps[T](c: Par[Conc[T]]) = new Concs.Ops[T](c.xs)
  }

  class Ops[T](val c: Conc[T]) extends AnyVal with Zippable.OpsLike[T, Par[Conc[T]]] {
    def stealer: Stealer[T] = new ConcStealer(c, 0, c.size)
    override def reduce[U >: T](operator: (U, U) => U)(implicit ctx: WorkstealingTreeScheduler): U = macro methods.ConcsMacros.reduce[T, U]
  }
  
  /* stealer implementation */

  import WorkstealingTreeScheduler.{ Kernel, Node }

  class ConcStealer[@specialized(Int, Long, Float, Double) T](val conc: Conc[T], sidx: Int, eidx: Int) extends IndexedStealer[T](sidx, eidx) {
    val stack = new Array[Conc[T]](conc.level + 1)
    var depth = 0
    var idx = 0
    var leaf: Conc.Leaf[Any] = null // to circumvent a specialization bug where the wrong field is assigned in the non-specialized `init`

    var padding12: Int = _
    var padding13: Int = _
    var padding14: Int = _
    var padding15: Int = _

    private def init() {
      stack(0) = conc
      while (stack(depth).level != 0) {
        depth += 1
        stack(depth) = stack(depth - 1).left
      }
      if (stack(depth).isInstanceOf[Conc.Leaf[_]]) {
        leaf = stack(depth).asInstanceOf[Conc.Leaf[T]]
      }
      if (leaf != null) moveN(startIndex)
    }

    init()

    private[parallel] final def push(v: Conc[T]) {
      depth += 1
      stack(depth) = v
    }

    private[parallel] final def pop() = if (depth >= 0) {
      val t = stack(depth)
      depth -= 1
      t
    } else null

    private[parallel] final def peek = if (depth >= 0) stack(depth) else null

    private[parallel] final def switch() = if (depth >= 0) {
      val t = stack(depth)
      stack(depth) = t.right
    }

    private def move0(): Unit = {
      if (idx >= leaf.size) {
        pop()
        if (peek != null) {
          val inner = pop()
          push(inner.right)
          while (peek.level != 0) push(peek.left)
        }
        leaf = peek.asInstanceOf[Conc.Leaf[T]]
        idx = 0
      }
    }

    private[parallel] final def move1(): Unit = {
      idx += 1
      nextProgress += 1
      move0()
    }

    private[parallel] def moveN(numElems: Int): Unit = {
      @tailrec def moveUp(num: Int): Int = {
        val inner = pop()
        if (inner == null) 0
        else if (inner.right.size > num) {
          push(inner.right)
          num
        } else {
          moveUp(num - inner.right.size)
        }
      }
      @tailrec def moveDown(num: Int): Int = {
        val inner = peek
        if (inner != null) {
          if (inner.level == 0) num
          else if (inner.left.size < num) {
            pop()
            push(inner.right)
            moveDown(num - inner.left.size)
          } else {
            push(inner.left)
            moveDown(num)
          }
        } else num
      }

      val remElems = math.min(leaf.size - idx, numElems)
      idx += numElems
      if (idx >= leaf.size) {
        pop()
        val afterUp = moveUp(numElems - remElems)
        val afterDown = moveDown(afterUp)
        leaf = peek.asInstanceOf[Conc.Leaf[Any]]
        idx = afterDown
        if (leaf != null) move0()
      }
      nextProgress += numElems
    }

    def next(): T = if (hasNext) {
      val res = leaf.asInstanceOf[Conc.Leaf[T]].elementAt(idx)
      move1()
      res
    } else throw new NoSuchElementException
  
    def hasNext: Boolean = nextProgress < nextUntil
  
    def split: (ConcStealer[T], ConcStealer[T]) = {
      val total = elementsRemainingEstimate
      psplit(total / 2)
    }
  
    def psplit(leftSize: Int): (ConcStealer[T], ConcStealer[T]) = {
      val ls = decode(READ_PROGRESS)
      val lu = ls + leftSize
      val rs = lu
      val ru = untilIndex

      (new ConcStealer[T](conc, ls, lu), new ConcStealer[T](conc, rs, ru))
    }
  }

  abstract class ConcKernel[@specialized(Int, Long, Float, Double) T, @specialized(Int, Long, Float, Double) R]
  extends IndexedStealer.IndexedKernel[T, R] {
    override def defaultIncrementStepFactor = 4

    def apply(node: Node[T, R], chunkSize: Int): R = {
      val stealer = node.stealer.asInstanceOf[ConcStealer[T]]
      var remaining = stealer.nextUntil - stealer.nextProgress
      var from = stealer.idx
      var result = node.READ_INTERMEDIATE

      stealer.peek match {
        case c: Conc.Chunk[T] =>
          val remainingInChunk = c.size - from
          result = applyChunk(c, from, math.min(remaining, remainingInChunk), result)
          if (remainingInChunk > remaining) {
            stealer.idx += remaining
            remaining = 0
          } else {
            stealer.idx = 0
            remaining -= remainingInChunk
            stealer.pop()
            stealer.switch()
          }
        case _ =>
          // nothing
      }

      while (stealer.peek != null && remaining > 0) {
        // descend until subtree is smaller
        var top = stealer.peek
        while (top.size > remaining && top.level > 0) {
          stealer.push(top.left)
          top = stealer.peek
        }

        // process subtree and decrease remaining
        result = applyTree(top, remaining, result)
        remaining -= top.size

        // move to the next subtree if the leaf is completed
        if (remaining >= 0) {
          stealer.pop()
          stealer.switch()
        } else {
          // if the leaf was a chunk with remaining elements
          stealer.idx = top.size + remaining
        }
      }

      result
    }

    def applyTree(t: Conc[T], remaining: Int, acc: R): R

    def applyChunk(t: Conc.Chunk[T], from: Int, remaining: Int, acc: R): R
  }

}












