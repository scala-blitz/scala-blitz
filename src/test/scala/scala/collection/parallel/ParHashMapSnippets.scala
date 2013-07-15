package scala.collection.parallel



import scala.reflect.ClassTag
import Par._
import workstealing.WorkstealingTreeScheduler
import workstealing.WorkstealingTreeScheduler.Config
import workstealing.Ops._
import collection.mutable.HashMap



trait ParHashMapSnippets {

  def aggregateSequential(hm: HashMap[Int, Int]) = {
    val it = hm.iterator
    var sum = 0
    while (it.hasNext) {
      val kv = it.next()
      sum += kv._2
    }
    sum
  }

  def aggregateParallel(hm: HashMap[Int, Int])(implicit s: WorkstealingTreeScheduler) = hm.toPar.aggregate(0)(_ + _)(_ + _._2)

  def countSequential(hm: HashMap[Int, Int]) = {
    val it = hm.iterator
    var cnt = 0
    while (it.hasNext) {
      val kv = it.next()
      if (kv._2 % 2 == 0) cnt += 1
    }
    cnt
  }

  def countParallel(hm: HashMap[Int, Int])(implicit s: WorkstealingTreeScheduler) = hm.toPar.count(_._2 % 2 == 0)

  def filterSequential(hm: HashMap[Int, Int]) = {
    var res = new HashMap[Int, Int]
    val it = hm.iterator
    while (it.hasNext) {
      val kv = it.next()
      if (kv._2 % 5 != 0) res.put(kv._1, kv._2)
    }
    res
  }

  def filterParallel(hm: HashMap[Int, Int])(implicit s: WorkstealingTreeScheduler) = hm.toPar.filter (_._2 % 5 != 0)


  def mapReduceSequential(hm: HashMap[Int, Int]) = {
    val it = hm.iterator
    var sum = 0
    while (it.hasNext) {
      val entry = it.next
      sum += entry._1 + entry._2 + 1
    }
    sum
  }

  def mapReduceParallel(hm: HashMap[Int, Int])(implicit s: WorkstealingTreeScheduler) = hm.toPar.mapReduce(x=>x._1+x._2+1)(_+_)

  def foreachSequential(r:Range) = {
    val ai = new java.util.concurrent.atomic.AtomicLong(0)
    HashMap(r zip r: _*).foreach(x=>  if ((x._1+x._2) % 500 == 0) ai.incrementAndGet())
    ai.get
  }

  def foreachSequential(hm: HashMap[Int, Int]) = {
    val ai = new java.util.concurrent.atomic.AtomicLong(0)
    hm.foreach(x=>  if ((x._1+x._2) % 500 == 0) ai.incrementAndGet())
    ai.get
  }

  def foreachParallel(hm: HashMap[Int, Int])(implicit s: WorkstealingTreeScheduler) = {
    val ai = new java.util.concurrent.atomic.AtomicLong(0)
    hm.toPar.foreach(x=> if ((x._1+x._2) % 500 == 0) ai.incrementAndGet())
    ai.get
  }

  object CustomOrd extends Ordering[(Int,Int)] {
      def compare(x: (Int,Int), y: (Int,Int)) = if(Ordering.Int.compare(x._1,y._1)!=0 ) Ordering.Int.compare(x._1,y._1) else Ordering.Int.compare(x._2,y._2)
      }

  def minSequential(hm: HashMap[Int, Int]) = {
    hm.min(CustomOrd)
  }

  def minParallel(hm: HashMap[Int, Int])(implicit s: WorkstealingTreeScheduler) = hm.toPar.min(CustomOrd,s)

  def maxSequential(hm: HashMap[Int, Int]) = {
    hm.max(CustomOrd)
  }

  def maxParallel(hm: HashMap[Int, Int])(implicit s: WorkstealingTreeScheduler) = hm.toPar.max(CustomOrd,s)

  object CustomNumPlus extends Numeric[(Int,Int)] {
      def fromInt(x: Int): (Int,Int) = ???
      def minus(x: (Int,Int), y: (Int,Int)): (Int,Int) = ???
      def negate(x: (Int, Int)): (Int, Int) = ???
      def plus(x: (Int,Int), y: (Int,Int)) : (Int,Int) = (x._1+y._1,x._2+y._2)
      def times(x: (Int,Int), y: (Int,Int)) = ???
      def toDouble(x: (Int,Int)): Double = ???
      def toFloat(x: (Int,Int)): Float = ???
      def toInt(x: (Int,Int)): Int = ???
      def toLong(x: (Int,Int)): Long = ???
      override def zero = (0,0)
      override def one = ???
      def compare(x: (Int, Int),y: (Int, Int)): Int = ???
    }

  def sumSequential(hm: HashMap[Int, Int]) = {
    hm.sum(CustomNumPlus)
  }

  def sumParallel(hm: HashMap[Int, Int])(implicit s: WorkstealingTreeScheduler) = hm.toPar.sum(CustomNumPlus,s)

  object CustomNumTimes extends Numeric[(Int,Int)] {
      def fromInt(x: Int): (Int,Int) = ???
      def minus(x: (Int,Int), y: (Int,Int)): (Int,Int) = ???
      def negate(x: (Int, Int)): (Int, Int) = ???
      def plus(x: (Int,Int), y: (Int,Int)) : (Int,Int) = ???
      def times(x: (Int,Int), y: (Int,Int)) = (x._1+y._1,x._2+y._2)
      def toDouble(x: (Int,Int)): Double = ???
      def toFloat(x: (Int,Int)): Float = ???
      def toInt(x: (Int,Int)): Int = ???
      def toLong(x: (Int,Int)): Long = ???
      override def zero = ???
      override def one = (0,0)
      def compare(x: (Int, Int),y: (Int, Int)): Int = ???
    }
  
  def productSequential(hm: HashMap[Int, Int]) = {
    hm.product(CustomNumTimes)
  }

  def productParallel(hm: HashMap[Int, Int])(implicit s: WorkstealingTreeScheduler) = hm.toPar.product(CustomNumTimes,s)
}

