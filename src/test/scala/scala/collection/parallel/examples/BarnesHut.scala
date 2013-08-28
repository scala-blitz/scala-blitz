package scala.collection.parallel.examples



import scala.collection.parallel._
import scala.collection.parallel.Par._
import scala.collection.parallel.workstealing.Ops._
import scala.collection.mutable.HashSet
import java.awt._
import java.awt.event._
import javax.swing._
import javax.swing.event._



object BarnesHut {

  var bodies: Array[Body] = _
  var scheduler: workstealing.WorkstealingTreeScheduler = _
  var boundaries: Boundaries = _

  case class Body(val id: Int) {
    var x: Double = _
    var y: Double = _
    var xspeed: Double = _
    var yspeed: Double = _
    var mass: Double = _
  }

  sealed trait Quad {
    def massX: Double
    def massY: Double
    def mass: Double
  }

  object Quad {
    case class Fork(centerX: Double, centerY: Double, size: Double, nw: Quad, ne: Quad, sw: Quad, se: Quad)
    extends Quad {
      var massX: Double = _
      var massY: Double = _
      var mass: Double = _
    }
    case class Leaf(body: Body)
    extends Quad {
      def massX = body.x
      def massY = body.y
      def mass = body.mass
    }
  }

  val debug = new java.util.concurrent.ConcurrentLinkedQueue[Body]

  class Boundaries extends Accumulator[Body, Boundaries] {
    var minX = Double.MaxValue
    var minY = Double.MaxValue
    var maxX = Double.MinValue
    var maxY = Double.MinValue

    def width = maxX - minX

    def height = maxY - minY

    def merge(that: Boundaries) = if (this eq that) this else {
      val res = new Boundaries
      res.minX = math.min(this.minX, that.minX)
      res.minY = math.min(this.minY, that.minY)
      res.maxX = math.max(this.maxX, that.maxX)
      res.maxY = math.max(this.maxY, that.maxY)
      res
    }

    def +=(b: Body) = {
      minX = math.min(b.x, minX)
      minY = math.min(b.y, minY)
      maxX = math.max(b.x, maxX)
      maxY = math.max(b.y, maxY)
      this
    }

    def clear() {
      minX = Double.MaxValue
      minY = Double.MaxValue
      maxX = Double.MinValue
      maxY = Double.MinValue
    }

    def result = this

    def centerX = (minX + maxX) / 2

    def centerY = (minY + maxY) / 2

    override def toString = s"Boundaries($minX, $minY, $maxX, $maxY)"
  }

  def parallelism = Runtime.getRuntime.availableProcessors

  def totalBodies = 2000000

  def sectorPrecision = 4

  def init() {
    initScheduler()
    initBodies()
  }

  def initBodies() {
    bodies = new Array(totalBodies)
    for (i <- 0 until bodies.length) {
      val b = new Body(i)
      b.x = math.random * 1000
      b.y = math.random * 1000
      b.xspeed = math.random - 0.5
      b.yspeed = math.random - 0.5
      b.mass = 0.1 + math.random
      bodies(i) = b
    }
  }

  def initScheduler() {
    val conf = new workstealing.WorkstealingTreeScheduler.Config.Default(parallelism)
    scheduler = new workstealing.WorkstealingTreeScheduler.ForkJoin(conf)
  }

  def step()(implicit s: workstealing.WorkstealingTreeScheduler) {
    def constructTree(): Quad = {
      // compute center and boundaries
      boundaries = bodies.toPar.accumulate(new Boundaries)
      val centerX = boundaries.centerX
      val centerY = boundaries.centerY

      // create a tree for each sector

      null
    }

    def updatePositions(quadtree: Quad) {
    }

    val startTime = System.nanoTime
    val quadtree = constructTree()
    updatePositions(quadtree)
    val endTime = System.nanoTime
    val totalTime = endTime - startTime
    println(totalTime / 1000000.0)
  }

  class BarnesHutFrame extends JFrame("Barnes-Hut") {
    setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
    setSize(1024, 600)
    setLayout(new BorderLayout)
    val rightpanel = new JPanel
    rightpanel.setBorder(BorderFactory.createEtchedBorder(border.EtchedBorder.LOWERED))
    add(rightpanel, BorderLayout.EAST)
    val controls = new JPanel
    rightpanel.add(controls, BorderLayout.NORTH)
    val stepbutton = new JButton("Step")
    stepbutton.addActionListener(new ActionListener {
      def actionPerformed(e: ActionEvent) {
        step()(scheduler)
        repaint()
      }
    })
    controls.add(stepbutton)
    val canvas = new JComponent {
      val pixels = new Array[Int](4000 * 4000)
      override def paintComponent(g: Graphics) {
        super.paintComponent(g)
        val width = getWidth
        val height = getHeight
        if (boundaries != null) {
          val img = new image.BufferedImage(width, height, image.BufferedImage.TYPE_INT_ARGB)
          for (x <- 0 until width; y <- 0 until height) pixels(y * width + x) = 0
          for (b <- bodies) {
            val px = ((b.x - boundaries.minX) / boundaries.width * width).toInt
            val py = ((b.y - boundaries.minY) / boundaries.height * height).toInt
            pixels(py * width + px) += 1
          }
          for (x <- 0 until width; y <- 0 until height) {
            val factor = 1.0 * bodies.length / (width * height)
            val intensity = pixels(y * width + x) / factor * 40
            val bound = math.min(255, intensity.toInt)
            val color = (255 << 24) | (bound << 16) | (bound << 8) | bound
            img.setRGB(x, y, color)
          }
          g.drawImage(img, 0, 0, null)
        }
      }
    }
    add(canvas)
    setVisible(true)
  }

  def main(args: Array[String]) {
    val frame = new BarnesHutFrame
    init()
  }

}




