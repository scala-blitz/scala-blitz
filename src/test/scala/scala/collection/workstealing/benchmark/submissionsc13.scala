package scala.collection.workstealing
package benchmark





/* cheap, uniform range foreach - 150M */

object ParRangeForeachCheapSpecific extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  @volatile var found = false

  def run() {
    val range = new ParRange(0 until size, Workstealing.DefaultConfig)
    range.foreach(x => if (((x * x) & 0xffffff) == 0) found = true)
  }

}


object ParRangeForeachCheapGeneric extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  @volatile var found = false

  def run() {
    val range: ParIterable[Int] = new ParRange(0 until size, Workstealing.DefaultConfig)
    range.foreach(x => if (((x * x) & 0xffffff) == 0) found = true)
  }

}


object ParRangeForeachCheapPC extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val parlevel = sys.props("par").toInt
  val fj = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parlevel))
  @volatile var found = false

  def run() {
    val range = (0 until size).par
    range.tasksupport = fj
    range.foreach(x => if (((x * x) & 0xffffff) == 0) found = true)
  }

}


object ParRangeForeachCheapLoop extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  @volatile var found = false

  def run() {
    var i = 0
    while (i < size) {
      if (((i * i) & 0xffffff) == 0) found = true
      i += 1
    }
  }

}


/* cheap, uniform range fold - 150M */

object ParRangeFoldCheapSpecific extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range = new ParRange(0 until size, Workstealing.DefaultConfig)
    val result = range.fold(0)(_ + _)
  }

}


object ParRangeFoldCheapGeneric extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range: ParIterable[Int] = new ParRange(0 until size, Workstealing.DefaultConfig)
    range.fold(0)(_ + _)
  }

}


object ParRangeFoldCheapPC extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val parlevel = sys.props("par").toInt
  val fj = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parlevel))

  def run() {
    val range = (0 until size).par
    range.tasksupport = fj
    range.fold(0)(_ + _)
  }

}


object ParRangeFoldCheapLoop extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  var result = 0

  def run() {
    var sum = 0
    var i = 0
    while (i < size) {
      sum += i
      i += 1
    }
    result = sum
  }

}


/* cheap, uniform array fold - 50M */

object ParArrayFoldCheapSpecific extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val array = new ParArray((0 until size).toArray, Workstealing.DefaultConfig)

  def run() {
    array.fold(0)(_ + _)
  }

}


object ParArrayFoldCheapGeneric extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val array: ParIterable[Int] = new ParArray((0 until size).toArray, Workstealing.DefaultConfig)

  def run() {
    array.fold(0)(_ + _)
  }

}


object ParArrayFoldCheapPC extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val parlevel = sys.props("par").toInt
  val fj = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parlevel))
  val array = (0 until size).toArray.par
  array.tasksupport = fj

  def run() {
    array.fold(0)(_ + _)
  }

}


object ParArrayFoldCheapLoop extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val array = (0 until size).toArray
  var result = 0

  def run() {
    var sum = 0
    var i = 0
    while (i < size) {
      sum += array(i)
      i += 1
    }
    result = sum
  }

}


/* cheap, uniform conc fold - 15M */

object ParConcFoldSpecific extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val conc = ConcUtils.create(size)

  def run() {
    conc.fold(0)(_ + _)
  }

}


object ParConcFoldGeneric extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val conc: ParIterable[Int] = ConcUtils.create(size)

  def run() {
    conc.fold(0)(_ + _)
  }

}


object ParConcFoldRecursion extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val conc = ConcUtils.create(size)

  def run() {
    import Conc._
    def fold(c: Conc[Int]): Int = c match {
      case left || right => fold(left) + fold(right)
      case Nil() => 0
      case Single(elem) => elem
    }
    fold(conc)
  }

}


object SeqListFold extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val list = (0 until size).toList

  def run() {
    list.fold(0)(_ + _)
  }

}


/* irregular range fold - 50k */

object IrregularWorkloads {

  def peakAtEnd(x: Int, percent: Double, size: Int) = if (x < percent * size) x else {
    var sum = 0
    var i = 1
    while (i < size) {
      sum += i
      i += 1
    }
    sum
  }

  def isPrime(x: Int): Boolean = {
    var i = 2
    val to = math.sqrt(x).toInt + 2
    while (i <= to) {
      if (x % i == 0) return false
      i += 1
    }
    true
  }

  def exp(x: Int): Boolean = {
    var i = 0
    val until = 1 << (x / 100)
    var sum = 0
    while (i < until) {
      sum += i
      i += 1
    }
    sum % 2 == 0
  }

}

object ParRangeFoldIrregularSpecific extends StatisticsBenchmark {

  val size = sys.props("size").toInt

  def run() {
    val range = new ParRange(0 until size, Workstealing.DefaultConfig)
    val result = range.aggregate(0)(_ + _) {
      (acc, x) => acc + IrregularWorkloads.peakAtEnd(x, 0.97, size)
    }
  }

}


object ParRangeFoldIrregularPC extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val parlevel = sys.props("par").toInt
  val fj = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parlevel))

  def run() {
    val range = (0 until size).par
    range.tasksupport = fj
    val result = range.aggregate(0)({
      (acc, x) => acc + IrregularWorkloads.peakAtEnd(x, 0.97, size)
    }, _ + _)
  }

}


object ParRangeFoldIrregularLoop extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  var result = 0

  def run() {
    var sum = 0
    var i = 0
    while (i < size) {
      sum += IrregularWorkloads.peakAtEnd(i, 0.97, size)
      i += 1
    }
    result = sum
  }

}


/* irregular array filter - 1M */

object ParArrayFilterIrregularSpecific extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val array = new ParArray((0 until size).toArray, Workstealing.DefaultConfig)

  def run() {
    val result = array.filter(IrregularWorkloads.isPrime)
  }

}


object ParArrayFilterIrregularPC extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val parlevel = sys.props("par").toInt
  val fj = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parlevel))
  val array = (0 until size).toArray.par
  array.tasksupport = fj

  def run() {
    val result = array.filter(IrregularWorkloads.isPrime)
  }

}


object ParArrayFilterIrregularSeq extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val array = (0 until size).toArray

  def run() {
    array.filter(IrregularWorkloads.isPrime)
  }

}


/* irregular range filter - 2500 */

object ParRangeFilterIrregularSpecific extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val range = new ParRange(0 until size, Workstealing.DefaultConfig)

  def run() {
    val result = range.filter(IrregularWorkloads.exp)
  }

}


object ParRangeFilterIrregularPC extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val parlevel = sys.props("par").toInt
  val fj = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parlevel))
  val range = (0 until size).par
  range.tasksupport = fj

  def run() {
    val result = range.filter(IrregularWorkloads.exp)
  }

}


object ParRangeFilterIrregularSeq extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val range = (0 until size)

  def run() {
    val result = range.filter(IrregularWorkloads.exp)
  }

}


/* standard deviation computation - 15M */

object StandardDeviationSpecific extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val numbers = new Array[Double](size)
  for (i <- 0 until size) numbers(i) = 10.0 + (i - size / 2.0) / size
  
  val measurements = new ParArray(numbers, Workstealing.DefaultConfig)

  def run() {
    val mean = measurements.fold(0.0)(_ + _) / measurements.size
    val variance = measurements.aggregate(0.0)(_ + _) {
      (acc, x) => acc + (x - mean) * (x - mean)
    }
    val stdev = math.sqrt(variance)
  }

}


object StandardDeviationPC extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val numbers = new Array[Double](size)
  for (i <- 0 until size) numbers(i) = 10.0 + (i - size / 2.0) / size

  val parlevel = sys.props("par").toInt
  val fj = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parlevel))
  val measurements = numbers.par
  measurements.tasksupport = fj

  def run() {
    val mean = measurements.fold(0.0)(_ + _) / measurements.size
    val variance = measurements.aggregate(0.0)({
      (acc, x) => acc + (x - mean) * (x - mean)
    }, _ + _)
    val stdev = math.sqrt(variance)
  }

}


/* Mandelbrot set computation
 *
 * -2.0,-2.0,2.0,2.0, 1000x1000, 1000
 * -48.0,-48.0,0.0,0.0, 3000x3000, 10000
 * -100.0,-100.0,0.0,0.0 10000x10000, 1000
 * -2.0,-1.0,1.0,32.0 10000x10000, 10000
 */

object Mandelbrot {

  def compute(xc: Double, yc: Double, threshold: Int): Int = {
    var i = 0
    var x = 0.0
    var y = 0.0
    while (x * x + y * y < 2 && i < threshold) {
      val xt = x * x - y * y + xc
      val yt = 2 * x * y + yc

      x = xt
      y = yt

      i += 1
    }
    i
  }

}

object MandelbrotSpecific extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val threshold = sys.props("threshold").toInt
  val bounds = sys.props("bounds").split(",").map(_.toDouble)
  val image = new Array[Int](size * size)

  def run() {
    val range = new ParRange(0 until (size * size), Workstealing.DefaultConfig)
    val xlo = bounds(0)
    val ylo = bounds(1)
    val xhi = bounds(2)
    val yhi = bounds(3)

    for (idx <- range) {
      val x = idx % size
      val y = idx / size
      val xc = xlo + (xhi - xlo) * x / size
      val yc = ylo + (yhi - ylo) * y / size

      image(idx) = Mandelbrot.compute(xc, yc, threshold)
    }
  }

  override def runBenchmark(noTimes: Int): List[Long] = {
    val times = super.runBenchmark(noTimes)

    // output png
    import javax.imageio._
    import java.awt.image._
    import java.io._
    val bi = new BufferedImage(size, size, BufferedImage.TYPE_INT_ARGB)
    for (x <- 0 until size; y <- 0 until size) {
      val iters = image(y * size + x)
      if (iters == threshold) bi.setRGB(x, y, 0xff000000)
      else {
        val r = (1.0 * iters / threshold * 255).toInt
        val g = (1.5 * iters / threshold * 255).toInt
        val b = (3.0 * iters / threshold * 255).toInt
        val a = 255
        val color = (a << 24) | (r << 16) | (g << 8) | b
        bi.setRGB(x, y, color)
      }
    }
    ImageIO.write(bi, "png", new File("mandelbrot.png"))

    times
  }

}


object MandelbrotPC extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val threshold = sys.props("threshold").toInt
  val bounds = sys.props("bounds").split(",").map(_.toDouble)
  val image = new Array[Int](size * size)

  val parlevel = sys.props("par").toInt
  val fj = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parlevel))

  def run() {
    val range = (0 until (size * size)).par
    range.tasksupport = fj
    val xlo = bounds(0)
    val ylo = bounds(1)
    val xhi = bounds(2)
    val yhi = bounds(3)

    for (idx <- range) {
      val x = idx % size
      val y = idx / size
      val xc = xlo + (xhi - xlo) * x / size
      val yc = ylo + (yhi - ylo) * y / size

      image(idx) = Mandelbrot.compute(xc, yc, threshold)
    }
  }

}


/* word segmentation - incFreq = 2 */

object WordSegmentationSpecific extends StatisticsBenchmark {

  val inputText = "therearemanythingsiknoworthatiwouldliketolearnsomehow"
  val dictionary = collection.mutable.HashSet() ++= io.Source.fromFile("/usr/dict/words").getLines

  def isWord(r: Range) = dictionary(inputText.substring(r.head, r.last))

  def decode(subrange: Range): Int = {
    (if (isWord(subrange)) 1 else 0) + subrange.tail.aggregate(0)({ (acc, idx) =>
      val prefix = subrange.head until idx
      if (isWord(prefix)) acc + decode(idx until inputText.length)
      else acc
    }, _ + _)
  }

  def run() {
    val range = new ParRange(1 until inputText.length, Workstealing.DefaultConfig)
    val results = range.aggregate(0)(_ + _) { (acc, idx) =>
      val prefix = 0 until idx
      if (isWord(prefix)) acc + decode(idx until inputText.length)
      else acc
    }
    println(results)
  }

}


object WordSegmentationPC extends StatisticsBenchmark {

  val inputText = "therearemanythingsiknoworthatiwouldliketolearnsomehow"
  val dictionary = collection.mutable.HashSet() ++= io.Source.fromFile("/usr/dict/words").getLines

  val parlevel = sys.props("par").toInt
  val fj = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parlevel))

  def isWord(r: Range) = dictionary(inputText.substring(r.head, r.last))

  def decode(subrange: Range): Int = {
    (if (isWord(subrange)) 1 else 0) + subrange.tail.aggregate(0)({ (acc, idx) =>
      val prefix = subrange.head until idx
      if (isWord(prefix)) acc + decode(idx until inputText.length)
      else acc
    }, _ + _)
  }

  def run() {
    val range = (1 until inputText.length).par
    range.tasksupport = fj
    val results = range.aggregate(0)({ (acc, idx) =>
      val prefix = 0 until idx
      if (isWord(prefix)) acc + decode(idx until inputText.length)
      else acc
    }, _ + _)
    println(results)
  }

}


/* triangular matrix multiplication - 350 */

object TriMatrixMultSpecific extends StatisticsBenchmark {
  val sz = sys.props("size").toInt

  type Big = java.math.BigDecimal

  class Vector(val size: Int) {
    val array = new Array[Big](size)
    for (i <- 0 until size) array(i) = new Big("1.2345678e322")
  }

  class Matrix(val size: Int) {
    val arrays = new Array[Array[Big]](size)
    for (i <- 1 to size) {
      arrays(i - 1) = new Array[Big](i)
      for (j <- 0 until i) arrays(i - 1)(j) = new Big("1.2345678e322")
    }

    def inPlaceMult(v: Vector, result: Vector) {
      val rows = new ParRange(0 until size, Workstealing.DefaultConfig)

      for (row <- rows) {
        val marr = arrays(row)
        val varr = v.array
        var sum = new Big(0.0)
        var col = 0
        while (col <= row) {
          sum = sum add (marr(col) multiply varr(col))
          col += 1
        }
        result.array(row) = sum
      }
    }
  }

  val m = new Matrix(sz)
  val v = new Vector(sz)
  val r = new Vector(sz)

  def run() {
    m.inPlaceMult(v, r)
  }

}


object TriMatrixMultPC extends StatisticsBenchmark {
  val sz = sys.props("size").toInt
  val parlevel = sys.props("par").toInt
  val fj = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parlevel))

  type Big = java.math.BigDecimal

  class Vector(val size: Int) {
    val array = new Array[Big](size)
    for (i <- 0 until size) array(i) = new Big("1.2345678e322")
  }

  class Matrix(val size: Int) {
    val arrays = new Array[Array[Big]](size)
    for (i <- 1 to size) {
      arrays(i - 1) = new Array[Big](i)
      for (j <- 0 until i) arrays(i - 1)(j) = new Big("1.2345678e322")
    }

    def inPlaceMult(v: Vector, result: Vector) {
      val rows = (0 until size).par
      rows.tasksupport = fj

      for (row <- rows) {
        val marr = arrays(row)
        val varr = v.array
        var sum = new Big(0.0)
        var col = 0
        while (col <= row) {
          sum = sum add (marr(col) multiply varr(col))
          col += 1
        }
        result.array(row) = sum
      }
    }
  }

  val m = new Matrix(sz)
  val v = new Vector(sz)
  val r = new Vector(sz)

  def run() {
    m.inPlaceMult(v, r)
  }

}


/* raytracing */

object Raytracing {

  case class Vector(x: Double, y: Double, z: Double) {
    def length = math.sqrt(x * x + y * y + z * z)
    def *(s: Double) = Vector(x * s, y * s, z * s)
    def +(v: Vector) = Vector(x + v.x, y + v.y, z + v.z)
    def -(v: Vector) = Vector(x - v.x, y - v.y, z - v.z)
  }

  trait Obj {
    def center: Vector
    def intersect(orig: Vector, dir: Vector): (Vector, Vector)
    def color: Color
    def reflection: Double
  }

  case class Sphere(center: Vector, radius: Double) extends Obj {
    def intersect(x0: Vector, x1: Vector): (Vector, Vector) = {
      val dx = x0.x - center.x
      val dy = x0.y - center.y
      val dz = x0.z - center.z
      val a = x1.x * x1.x + x1.y * x1.y + x1.z * x1.z
      val b = 2 * (dx + dy + dz)
      val c = dx * dx + dy * dy + dz * dz - radius * radius

      val discrim = b * b - 4 * a * c
      if (discrim < 0) null
      else {
        val ipoint = {
          val t1 = (-b - math.sqrt(discrim)) / (2 * a)
          val t2 = (-b + math.sqrt(discrim)) / (2 * a)
          val p1 = Vector(x0.x + x1.x * t1, x0.y + x1.y * t1, x0.z + x1.z * t1)
          val p2 = Vector(x0.x + x1.x * t2, x0.y + x1.y * t2, x0.z + x1.z * t2)
          val d1 = (p1.x - x0.x) * (p1.x - x0.x) + (p1.y - x0.y) * (p1.y - x0.y) + (p1.z - x0.z) * (p1.z - x0.z)
          val d2 = (p2.x - x0.x) * (p2.x - x0.x) + (p2.y - x0.y) * (p2.y - x0.y) + (p2.z - x0.z) * (p2.z - x0.z)
          if (d1 < d2) p1 else p2
        }
        val normal = Vector(ipoint.x - center.x, ipoint.y - center.y, ipoint.z - center.z)
        val n0 = normal * (1 / normal.length)
        val reflected = x1 - (n0 * (2 * (x1.x * n0.x + x1.y * n0.y + x1.z * n0.z)))
        (ipoint, reflected)
      }
    }
    def color = Color(0xbb, 0, 0)
    def reflection = 0.2
  }

  class Color(val col: Int) extends AnyVal {
    def r = (col >> 16) & 0xff
    def g = (col >> 8) & 0xff
    def b = (col >> 0) & 0xff
    def +(other: Color) = Color(
      math.min(255, r + other.r),
      math.min(255, g + other.g),
      math.min(255, b + other.b)
    )
    def *(factor: Double) = Color((r * factor).toInt, (g * factor).toInt, (b * factor).toInt)
  }

  object Color {
    def apply(r: Int, g: Int, b: Int) = new Color((r << 16) + (g << 8) + b)
  }

  val lights = Seq(
    Vector(10.0, 10.0, 10.0),
    Vector(5.0, 5.0, 15.0),
    Vector(-5.0, 5.0, -10.0)
  )

  val objects = for (i <- 0 until 1000) yield Sphere(Vector(99.5 + util.Random.nextDouble() / 4, 99.5 + util.Random.nextDouble() / 4, -20.0 + util.Random.nextDouble() * 20.0), 0.1 + util.Random.nextDouble() / 4)

  import collection.mutable.ArrayBuffer

  class ObjectGrid(val size: Int) {
    val array = new Array[ArrayBuffer[Obj]](size * size)
    for (x <- 0 until size; y <- 0 until size) array(y * size + x) = new ArrayBuffer[Obj]()

    def add(x: Int, y: Int, obj: Obj) = array(y * size + x) += obj
    def sector(x: Int, y: Int): Seq[Obj] = array(y * size + x)
  }

  val objectGrid = new ObjectGrid(100)
  for (obj <- objects) objectGrid.add(obj.center.x.toInt, obj.center.y.toInt, obj)

  def compute(x: Double, y: Double, threshold: Int): Int = {
    var finalcolor = Color(0, 0, 0)
    var raystart: Vector = null
    var raydir: Vector = null
    var depth = 0
    var reflection_factor = 1.0
    while (depth < threshold && reflection_factor > 0.0) {
      // prune
      val considered = if (depth == 0) objectGrid.sector(x.toInt, y.toInt) else objects
      
      if (considered.isEmpty) depth = threshold else {
        if (raystart == null) raystart = Vector(x, y, 0.0)
        if (raydir == null) raydir = Vector(x, y, -10.0)
  
        // find closest
        val solution = considered.foldLeft(null: (Obj, (Vector, Vector))) {
          (acc, obj) =>
          val x = obj.intersect(raystart, raydir)
          if (x == null) acc
          else if (acc == null) (obj, x)
          else if ((x._1 - raystart).length < (acc._2._1 - raystart).length) (obj, x)
          else acc
        }

        if (solution == null) depth = threshold
        else {
          val (closest, (intersection, direction)) = solution

          // light contributions
          var computedColor = Color(0, 0, 0)
          for (light <- lights) {
            val dirlight = light - intersection
            if (!considered.exists(obj => obj.intersect(intersection, dirlight) != null)) computedColor = computedColor + closest.color * 0.8
          }

          finalcolor = finalcolor + (computedColor * reflection_factor)
  
          raystart = intersection
          raydir = direction
          reflection_factor = reflection_factor * closest.reflection
  
          // increase depth
          depth += 1
        }
      }
    }

    finalcolor.col
  }

}

object RaytracingSpecific extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val threshold = sys.props("threshold").toInt
  val image = new Array[Int](size * size)

  def run() {
    val range = new ParRange(0 until (size * size), Workstealing.DefaultConfig)

    for (idx <- range) {
      val x = idx % size
      val y = idx / size
      val x0 = 100.0 * x / size
      val y0 = 100.0 * y / size

      image(idx) = Raytracing.compute(x0, y0, threshold)
    }
  }

}

object RaytracingPC extends StatisticsBenchmark {

  val size = sys.props("size").toInt
  val threshold = sys.props("threshold").toInt
  val image = new Array[Int](size * size)

  val parlevel = sys.props("par").toInt
  val fj = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parlevel))

  def run() {
    val range = (0 until (size * size)).par
    range.tasksupport = fj

    for (idx <- range) {
      val x = idx % size
      val y = idx / size
      val x0 = 100.0 * x / size
      val y0 = 100.0 * y / size

      image(idx) = Raytracing.compute(x0, y0, threshold)
    }
  }

}


/* vector addition */

object ScalarProductSpecific extends StatisticsBenchmark {

 class Vector(val size: Int) {
    val array = new Array[Float](size)
    def scalar(that: Vector) {
      val range = new ParRange(0 until size, Workstealing.DefaultConfig)
      val a = this.array
      val b = that.array
      range.aggregate(0.0f)(_ + _) {
        (sum, i) => sum + a(i) * b(i)
      }
    }
  }

  val size = sys.props("size").toInt
  val a = new Vector(size)
  val b = new Vector(size)

  def run() {
    a.scalar(b)
  }

}


















