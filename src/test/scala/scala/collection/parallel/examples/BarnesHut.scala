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

  class Boundaries {
    var minX = Double.NaN
    var minY = Double.NaN
    var maxX = Double.NaN
    var maxY = Double.NaN

    def width = maxX - minX

    def height = maxY - minY

    private def isNaN(v: Double) = java.lang.Double.isNaN(v)

    def combine(that: Boundaries) = {
      def min(x: Double, y: Double) = {
        if (isNaN(x)) y
        else if (isNaN(y)) x
        else math.min(x, y)
      }
      def max(x: Double, y: Double) = {
        if (isNaN(x)) y
        else if (isNaN(y)) x
        else math.max(x, y)
      }
      val res = new Boundaries
      res.minX = min(this.minX, that.minX)
      res.minY = min(this.minY, that.minY)
      res.maxX = max(this.maxX, that.maxX)
      res.maxY = max(this.maxY, that.maxY)
      res
    }

    def update(b: Body) = {
      minX = if (isNaN(minX)) b.x else math.min(b.x, minX)
      minY = if (isNaN(minY)) b.y else math.min(b.y, minY)
      maxX = if (isNaN(maxX)) b.x else math.max(b.x, maxX)
      maxY = if (isNaN(maxY)) b.y else math.max(b.y, maxY)
      this
    }

    def centerX = (minX + maxX) / 2

    def centerY = (minY + maxY) / 2

    override def toString = s"Boundaries($minX, $minY, $maxX, $maxY)"
  }

  def parallelism = Runtime.getRuntime.availableProcessors

  def totalBodies = 2000000

  def sectorPrecision = 16

  def width = 1000

  def height = 1000

  def init() {
    updateScheduler()
    updateBodies()
  }

  def updateBodies() {
    bodies = new Array(totalBodies)
    for (i <- 0 until bodies.length) {
      val b = new Body(i)
      b.x = math.random * width
      b.y = math.random * height
      b.xspeed = math.random - 0.5
      b.yspeed = math.random - 0.5
      b.mass = 0.1 + math.random
      bodies(i) = b
    }
  }

  def updateScheduler() {
    var conf = new workstealing.WorkstealingTreeScheduler.Config.Default(parallelism)
    scheduler = new workstealing.WorkstealingTreeScheduler.ForkJoin(conf)
  }

  def step()(implicit s: workstealing.WorkstealingTreeScheduler) {
    def constructTree(): Quad = {
      // compute center and boundaries
      boundaries = bodies.toPar.aggregate(new Boundaries)(_ combine _) {
        (bs, b) => bs.update(b)
      }
      val centerX = boundaries.centerX
      val centerY = boundaries.centerY

      // group bodies into sectors
      //val sectors = new Array[Body](sectorPrecision * sectorPrecision)

      // create a tree for each sector

      // merge quad tree

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




