package scala.collection.parallel
package workstealing



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.reflect.ClassTag
import scala.collection.parallel.generic._
import scala.collection.immutable.HashSet
import scala.collection.immutable.TrieIterator



object Trees {

  import WorkstealingTreeScheduler.{ Kernel, Node }

  val TRIE_ITERATOR_NAME = classOf[TrieIterator[_]].getName.replace(".", "$")
  val HASHSET1_NAME = classOf[TrieIterator[_]].getName.replace(".", "$")

  def mangledT(x: String) = TRIE_ITERATOR_NAME + "$$" + x
  def mangledHS(x: String) = TRIE_ITERATOR_NAME + "$$" + x

  val OFFSET_DEPTH = unsafe.objectFieldOffset(classOf[TrieChunkIterator[_, _]].getField(mangledT("depth")))
  val OFFSET_ARRAY_STACK = unsafe.objectFieldOffset(classOf[TrieChunkIterator[_, _]].getField(mangledT("arrayStack")))
  val OFFSET_POS_STACK = unsafe.objectFieldOffset(classOf[TrieChunkIterator[_, _]].getField(mangledT("posStack")))
  val OFFSET_ARRAY_D = unsafe.objectFieldOffset(classOf[TrieChunkIterator[_, _]].getField(mangledT("arrayD")))
  val OFFSET_POS_D = unsafe.objectFieldOffset(classOf[TrieChunkIterator[_, _]].getField(mangledT("posD")))
  val OFFSET_SUBITER = unsafe.objectFieldOffset(classOf[TrieChunkIterator[_, _]].getField(mangledT("subIter")))
  val OFFSET_HS_KEY = unsafe.objectFieldOffset(classOf[HashSet.HashSet1[_]].getDeclaredField("key"))

  final def key[T](hs1: HashSet.HashSet1[T]) = unsafe.getObject(hs1, OFFSET_HS_KEY).asInstanceOf[T]

  abstract class TrieChunkIterator[T, Repr] extends TrieIterator[T](null) with TreeStealer.ChunkIterator[T] {
    final def setDepth(d: Int) = unsafe.putInt(this, OFFSET_DEPTH, d)
    final def setPosD(p: Int) = unsafe.putInt(this, OFFSET_POS_D, p)
    final def setArrayD(a: Array[Iterable[T]]) = unsafe.putObject(this, OFFSET_ARRAY_D, a)
    final def getArrayD = unsafe.getObject(this, OFFSET_ARRAY_D).asInstanceOf[Array[Iterable[T]]]
    final def getSubIter = unsafe.getObject(this, OFFSET_SUBITER).asInstanceOf[Iterator[T]]
    final def clearArrayStack() {
      val arrayStack = unsafe.getObject(this, OFFSET_ARRAY_STACK).asInstanceOf[Array[Array[Iterable[T]]]]
      var i = 0
      while (i < 6 && arrayStack(i) != null) {
        arrayStack(i) = null
        i += 1
      }
    }
    final def clearPosStack() {
     val posStack = unsafe.getObject(this, OFFSET_POS_STACK).asInstanceOf[Array[Int]]
     var i = 0
     while (i < 6) {
       posStack(i) = 0
       i += 1
     }
    }
    final def clearSubIter() = unsafe.putObject(this, OFFSET_SUBITER, null)
    final def root = getArrayD(0).asInstanceOf[Repr]
  }

  trait Scope {
    implicit def hashTrieSetOps[T](a: Par[HashSet[T]]) = new Trees.HashSetOps(a)
    implicit def canMergeHashTrieSet[T: ClassTag](implicit ctx: WorkstealingTreeScheduler) = new CanMergeFrom[Par[HashSet[_]], T, Par[HashSet[T]]] {
      def apply(from: Par[HashSet[_]]) = new HashSetMerger[T](ctx)
      def apply() = new HashSetMerger[T](ctx)
    }
    implicit def hashTrieSetIsReducable[T] = new IsReducable[HashSet[T], T] {
      def apply(pa: Par[HashSet[T]]) = ???
    }
  }

  class HashSetOps[T](val hashset: Par[HashSet[T]]) extends AnyVal with Reducables.OpsLike[T, Par[HashSet[T]]] {
    def stealer: Stealer[T] = {
      val s = new HashSetStealer(hashset.seq)
      s.rootInit()
      s
    }
    def aggregate[S](z: S)(combop: (S, S) => S)(seqop: (S, T) => S)(implicit ctx: WorkstealingTreeScheduler) = macro methods.HashTrieSetMacros.aggregate[T, S]
    override def map[S, That](func: T => S)(implicit cmf: CanMergeFrom[Par[HashSet[T]], S, That], ctx: WorkstealingTreeScheduler): That = macro methods.HashTrieSetMacros.map[T, S, That]
  }

  class HashSetStealer[T](val root: HashSet[T]) extends {
    val classTag = implicitly[ClassTag[HashSet[T]]]
    val totalSize = root.size
  } with TreeStealer.External[T, HashSet[T]] {
    val chunkIterator = new TrieChunkIterator[T, HashSet[T]] {
      final def getElem(x: AnyRef): T = {
        val hs1 = x.asInstanceOf[HashSet.HashSet1[T]]
        Trees.key(hs1)
      }
    }
    val leafArray = new Array[Iterable[T]](1)

    var padding10: Int = 0
    var padding11: Int = 0
    var padding12: Int = 0
    var padding13: Int = 0
    var padding14: Int = 0
    var padding15: Int = 0

    final def newStealer = new HashSetStealer(root)
    final def resetIterator(n: HashSet[T]): Unit = if (n.nonEmpty) {
      chunkIterator.setDepth(0)
      chunkIterator.setPosD(0)
      chunkIterator.clearArrayStack()
      chunkIterator.clearPosStack()
      chunkIterator.clearSubIter()
      leafArray(0) = n
      chunkIterator.setArrayD(leafArray)
    } else {
      chunkIterator.clearSubIter()
      chunkIterator.setDepth(-1)
    }
    final def child(n: HashSet[T], idx: Int) = {
      val trie = n.asInstanceOf[HashSet.HashTrieSet[T]]
      if (trie.elems.length > 1 || idx == 1) trie.elems(idx - 1)
      else if (idx == 2) HashSet.empty
      else sys.error("error state")
    }
    final def elementAt(n: HashSet[T], idx: Int): T = ???
    final def depthBound(totalSize: Int): Int = 6
    final def isLeaf(n: HashSet[T]) = n match {
      case _: HashSet.HashTrieSet[_] => false
      case _ => true
    }
    final def estimateSubtree(n: HashSet[T], depth: Int, totalSize: Int) = n.size
    final def totalChildren(n: HashSet[T]) = n match {
      case n: HashSet.HashTrieSet[_] =>
        val len = n.elems.length
        if (len == 1) 2 else len
      case _ =>
        0
    }
  }

  trait HashUtils {
    protected final def improve(hcode: Int) = {
      var h: Int = hcode + ~(hcode << 9)
      h = h ^ (h >>> 14)
      h = h + (h << 4)
      h ^ (h >>> 10)
    }
  }

  class HashSetMerger[@specialized(Int, Long) T: ClassTag](
    val ctx: WorkstealingTreeScheduler
  ) extends HashBuckets[T, T, HashSetMerger[T], Par[HashSet[T]]] with HashUtils {
    val emptyTrie = HashSet.empty[T]
    val elems = new Array[Conc.Buffer[T]](1 << width)

    def newHashBucket = new HashSetMerger[T](ctx)

    def width = 5

    def clearBucket(idx: Int) {
      elems(idx) = null
    }

    def mergeBucket(idx: Int, that: HashSetMerger[T], res: HashSetMerger[T]) {
      val thise = this.elems(idx)
      val thate = that.elems(idx)
      if (thise == null) {
        res.elems(idx) = thate
      } else if (thate == null) {
        res.elems(idx) = thise
      } else {
        thise.prepareForMerge()
        thate.prepareForMerge()
        res.elems(idx) = thise merge thate
      }
    }

    def +=(elem: T): HashSetMerger[T] = {
      val es = elems
      val hc = improve(elem.##)
      val idx = hc & 0x1f
      var bucket = es(idx)
      if (bucket eq null) {
        es(idx) = new Conc.Buffer[T]
        bucket = es(idx)
      }
      bucket += elem
      this
    }

    def result = {
      import Par._
      import Ops._
      val stealer = (0 until elems.length).toPar.stealer
      val root = new Array[HashSet[T]](1 << 5)
      val kernel = new HashSetMergerResultKernel(elems, root)
      
      ctx.invokeParallelOperation(stealer, kernel)

      val fulltries = root.filter(_ != null)
      var bitmap = 0
      var sz = 0
      var i = 0
      while (i < root.length) {
        if (root(i) ne null) {
          sz += root(i).size
          bitmap |= 1 << i
        }
        i += 1
      }

      val hs = if (sz == 0) HashSet.empty[T]
        else if (sz == 1) root(0)
        else new HashSet.HashTrieSet(bitmap, fulltries, sz)

      hs.toPar
    }
  }

  class HashSetMergerResultKernel[@specialized(Int, Long) T](
    val elems: Array[Conc.Buffer[T]],
    val root: Array[HashSet[T]]
  ) extends IndexedStealer.IndexedKernel[Int, Unit] with HashUtils {
    override def incrementStepFactor(config: WorkstealingTreeScheduler.Config) = 1
    def zero = ()
    def combine(a: Unit, b: Unit) = a
    def apply(node: Node[Int, Unit], chunkSize: Int) {
      val stealer = node.stealer.asInstanceOf[Ranges.RangeStealer]
      var i = stealer.nextProgress
      val until = stealer.nextUntil
      while (i < until) {
        storeBucket(i)
        i += 1
      }
    }
    private def storeBucket(i: Int) {
      def traverse(es: Conc[T], res: HashSet[T]): HashSet[T] = es match {
        case knode: Conc.<>[T] =>
          val lres = traverse(knode.left, res)
          traverse(knode.right, lres)
        case kchunk: Conc.Chunk[T] =>
          var cres = res
          val kchunkarr = kchunk.elems
          var i = 0
          val until = kchunk.size
          while (i < until) {
            val k = kchunkarr(i)
            val hc = improve(k.##)
            cres = cres.updated0(k, hc, 5)
            i += 1
          }
          cres
        case _ =>
          sys.error("unreachable: " + es)
      }

      root(i) = if (elems(i) ne null) traverse(elems(i).result.normalized, HashSet.empty[T]) else null
    }
  }

  abstract class HashSetKernel[T, R] extends Kernel[T, R] {
    def apply(node: Node[T, R], chunkSize: Int): R = {
      val stealer = node.stealer.asInstanceOf[HashSetStealer[T]]
      apply(node, stealer.chunkIterator)
    }
    def apply(node: Node[T, R], ci: TrieChunkIterator[T, HashSet[T]]): R
  }

}
