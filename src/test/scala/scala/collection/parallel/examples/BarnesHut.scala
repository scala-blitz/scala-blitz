package scala.collection.parallel.examples



import scala.collection.parallel._
import scala.collection.parallel.Par._
import scala.collection.parallel.workstealing.Ops._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import java.awt._
import java.awt.event._
import javax.swing._
import javax.swing.event._



object BarnesHut {
  self =>

  var bodies: Array[Quad.Body] = _
  var scheduler: workstealing.WorkstealingTreeScheduler = _
  var initialBoundaries: Boundaries = _
  var boundaries: Boundaries = _
  var quadtree: Quad = _

  sealed trait Quad {
    def massX: Float
    def massY: Float
    def mass: Float
    def total: Int

    def update(fromx: Float, fromy: Float, sz: Float, b: Quad.Body): Quad

    def distance(fromx: Float, fromy: Float): Float = {
      math.sqrt((fromx - massX) * (fromx - massX) + (fromy - massY) * (fromy - massY)).toFloat
    }

    def force(m: Float, dist: Float) = gee * m * mass / (dist * dist)
  }

  object Quad {
    case class Body(val id: Int) extends Quad {
      var x: Float = _
      var y: Float = _
      var xspeed: Float = _
      var yspeed: Float = _
      var mass: Float = _
  
      def massX = x
      def massY = y
      def total = 1
  
      def update(fromx: Float, fromy: Float, sz: Float, b: Body) = {
        val cx = fromx + sz / 2
        val cy = fromy + sz / 2
        val fork = new Fork(cx, cy, sz)(Empty, Empty, Empty, Empty)
        fork.update(fromx, fromy, sz, this).update(fromx, fromy, sz, b)
      }

      def updatePosition(quad: Quad) {
        var netforcex = 0.0f
        var netforcey = 0.0f

        def traverse(quad: Quad): Unit = quad match {
          case Empty =>
            // no force
          case _ =>
            // see if node is far enough, or recursion is needed
            val dist = quad.distance(x, y)
            quad match {
              case f @ Fork(cx, cy, sz) if f.size / dist >= theta =>
                traverse(f)
              case _ =>
                val dforce = quad.force(mass, dist)
                val xn = (quad.massX - x) / dist
                val yn = (quad.massY - y) / dist
                val dforcex = dforce * xn
                val dforcey = dforce * yn
                netforcex += dforcex
                netforcey += dforcey
            }
        }

        println(netforcex, netforcey)
        xspeed += netforcex / mass * delta
        yspeed += netforcey / mass * delta
        x += xspeed * delta
        y += yspeed * delta
      }
    }

    case object Empty extends Quad {
      def massX = 0.0f
      def massY = 0.0f
      def mass = 0.0f
      def total = 0

      def update(fromx: Float, fromy: Float, sz: Float, b: Body) = b
    }
  
    case class Fork(val centerX: Float, val centerY: Float, val size: Float)(var nw: Quad, var ne: Quad, var sw: Quad, var se: Quad)
    extends Quad {
      var massX: Float = _
      var massY: Float = _
      var mass: Float = _

      def total = nw.total + ne.total + sw.total + se.total

      def update(fromx: Float, fromy: Float, sz: Float, b: Body) = {
        val hsz = sz / 2
        if (b.x < centerX) {
          if (b.y < centerY) nw = nw.update(fromx, fromy, hsz, b)
          else sw = sw.update(fromx, centerY, hsz, b)
        } else {
          if (b.y < centerY) ne = ne.update(centerX, fromy, hsz, b)
          else se = se.update(centerX, centerY, hsz, b)
        }

        updateStats()

        this
      }

      def updateStats() {
        mass = nw.mass + sw.mass + ne.mass + se.mass
        massX = (nw.mass * nw.massX + sw.mass * sw.massX + ne.mass * ne.massX + se.mass * se.massX) / mass
        massY = (nw.mass * nw.massY + sw.mass * sw.massY + ne.mass * ne.massY + se.mass * se.massY) / mass
      }
    }
  }

  val debug = new java.util.concurrent.ConcurrentLinkedQueue[Quad.Body]

  class Boundaries extends Accumulator[Quad.Body, Boundaries] {
    var minX = Float.MaxValue
    var minY = Float.MaxValue
    var maxX = Float.MinValue
    var maxY = Float.MinValue

    def width = maxX - minX

    def height = maxY - minY

    def size = math.max(width, height)

    def merge(that: Boundaries) = if (this eq that) this else {
      val res = new Boundaries
      res.minX = math.min(this.minX, that.minX)
      res.minY = math.min(this.minY, that.minY)
      res.maxX = math.max(this.maxX, that.maxX)
      res.maxY = math.max(this.maxY, that.maxY)
      res
    }

    def +=(b: Quad.Body) = {
      minX = math.min(b.x, minX)
      minY = math.min(b.y, minY)
      maxX = math.max(b.x, maxX)
      maxY = math.max(b.y, maxY)
      this
    }

    def clear() {
      minX = Float.MaxValue
      minY = Float.MaxValue
      maxX = Float.MinValue
      maxY = Float.MinValue
    }

    override def toString = s"Boundaries($minX, $minY, $maxX, $maxY)"
  }

  object Sectors {
    def apply[T <: AnyRef: ClassTag](b: Boundaries)(init: () => T)(comb: (T, T) => T)(op: (T, Quad.Body) => Unit) = {
      val s = new Sectors(b)(comb)(op)
      for (x <- 0 until sectorPrecision; y <- 0 until sectorPrecision) s.matrix(y * sectorPrecision + x) = init()
      s
    }
  }

  class Sectors[T <: AnyRef: ClassTag](val boundaries: Boundaries)(val comb: (T, T) => T)(val op: (T, Quad.Body) => Unit)
  extends Accumulator[Quad.Body, Sectors[T]] {
    var matrix = new Array[T](sectorPrecision * sectorPrecision)

    def merge(that: Sectors[T]) = {
      val res = new Sectors(boundaries)(comb)(op)
      for (x <- 0 until sectorPrecision; y <- 0 until sectorPrecision) {
        val sid = y * sectorPrecision + x
        res.matrix(sid) = comb(this.matrix(sid), that.matrix(sid))
      }
      res
    }

    def +=(b: Quad.Body) = {
      val sx = math.min(sectorPrecision - 1, ((b.x - boundaries.minX) / (boundaries.width / sectorPrecision)).toInt)
      val sy = math.min(sectorPrecision - 1, ((b.y - boundaries.minY) / (boundaries.height / sectorPrecision)).toInt)
      val accum = matrix(sy * sectorPrecision + sx)
      op(accum, b)
      this
    }

    def clear() {
      matrix = new Array(sectorPrecision * sectorPrecision)
    }

    override def toString = s"Sectors(${matrix.mkString(", ")})"
  }

  def toQuad(sectors: Sectors[Conc.Buffer[Quad.Body]])(implicit ctx: workstealing.WorkstealingTreeScheduler): Quad = {
    val quads = sectors.matrix.zipWithIndex.toPar.map(bi => sectorToQuad(sectors.boundaries, bi._1, bi._2))
    
    // bind into a quad tree
    var level = sectorPrecision
    while (level > 1) {
      val nextlevel = level / 2
      for (qy <- 0 until nextlevel; qx <- 0 until nextlevel) {
        val rx = qx * 2
        val ry = qy * 2
        val nw = quads.seq((ry + 0) * level + (rx + 0))
        val ne = quads.seq((ry + 0) * level + (rx + 1))
        val sw = quads.seq((ry + 1) * level + (rx + 0))
        val se = quads.seq((ry + 1) * level + (rx + 1))
        val size = boundaries.size / nextlevel
        val centerX = boundaries.minX + size * (qx + 0.5f)
        val centerY = boundaries.minY + size * (qy + 0.5f)
        val fork = new Quad.Fork(centerX, centerY, size)(nw, ne, sw, se)
        fork.updateStats()
        quads.seq(qy * nextlevel + qx) = fork
      }
      level = nextlevel
    }

    quads.seq(0)
  }

  def sectorToQuad(boundaries: Boundaries, bs: Conc.Buffer[Quad.Body], sid: Int): Quad = {
    val sx = sid % sectorPrecision
    val sy = sid / sectorPrecision
    val size = boundaries.size
    val sectorSize = size / sectorPrecision
    val fromX = boundaries.minX + sx * sectorSize
    val fromY = boundaries.minY + sy * sectorSize
    var quad: Quad = Quad.Empty

    for (b <- bs.result) {
      quad = quad.update(fromX, fromY, sectorSize, b)
    }

    quad
  }

  def parallelism = Runtime.getRuntime.availableProcessors

  def totalBodies = 20

  def sectorPrecision = 4

  def delta = 2.0f
  
  def theta = 0.5f

  def gee = 1000000.0f

  def init() {
    initScheduler()
    initBodies()
  }

  def initBodies() {
    bodies = new Array(totalBodies)
    for (i <- 0 until bodies.length) {
      val b = new Quad.Body(i)
      b.x = math.random.toFloat * 1000
      b.y = math.random.toFloat * 1000
      b.xspeed = 0.0f
      b.yspeed = 0.0f
      b.mass = 0.1f + math.random.toFloat
      bodies(i) = b
    }

    // compute center and boundaries
    initialBoundaries = bodies.toPar.accumulate(new Boundaries)(scheduler)
    boundaries = initialBoundaries
  }

  def initScheduler() {
    val conf = new workstealing.WorkstealingTreeScheduler.Config.Default(parallelism)
    scheduler = new workstealing.WorkstealingTreeScheduler.ForkJoin(conf)
  }

  def step()(implicit s: workstealing.WorkstealingTreeScheduler): Unit = self.synchronized {
    def constructTree(): Quad = {
      // construct sectors
      val sectors = bodies.toPar.accumulate(Sectors(boundaries)(() => new Conc.Buffer[Quad.Body])(_ merge _)({
        _ += _
      }))

      // construct a quad tree for each sector
      toQuad(sectors)
    }

    def updatePositions(quadtree: Quad) {
      for (b <- bodies.toPar) {
        b.updatePosition(quadtree)
      }

      // recompute center and boundaries
      boundaries = bodies.toPar.accumulate(new Boundaries)
    }

    val startTime = System.nanoTime
    quadtree = constructTree()
    updatePositions(quadtree)
    val endTime = System.nanoTime
    val totalTime = endTime - startTime
    println(totalTime / 1000000.0)
  }

  class BarnesHutFrame extends JFrame("Barnes-Hut") {
    setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
    setSize(800, 600)
    setLayout(new BorderLayout)
    val rightpanel = new JPanel
    rightpanel.setBorder(BorderFactory.createEtchedBorder(border.EtchedBorder.LOWERED))
    add(rightpanel, BorderLayout.EAST)
    val controls = new JPanel
    controls.setLayout(new GridLayout(0, 1))
    rightpanel.add(controls, BorderLayout.NORTH)
    val quadcheckbox = new JCheckBox("Show quad")
    quadcheckbox.addActionListener(new ActionListener {
      def actionPerformed(e: ActionEvent) {
        repaint()
      }
    })
    controls.add(quadcheckbox)
    val animationPanel = new JPanel
    animationPanel.setLayout(new GridLayout(1, 0))
    controls.add(animationPanel)
    val stepbutton = new JButton("Step")
    stepbutton.addActionListener(new ActionListener {
      def actionPerformed(e: ActionEvent) {
        step()(scheduler)
        repaint()
      }
    })
    animationPanel.add(stepbutton)
    val startButton = new JCheckBox("Start")
    val startTimer = new javax.swing.Timer(0, new ActionListener {
      def actionPerformed(e: ActionEvent) {
        step()(scheduler)
        repaint()
      }
    })
    startButton.addActionListener(new ActionListener {
      def actionPerformed(e: ActionEvent) {
        if (startButton.isSelected) startTimer.start()
        else startTimer.stop()
      }
    })
    animationPanel.add(startButton)
    val canvas = new JComponent {
      val pixels = new Array[Int](4000 * 4000)
      override def paintComponent(g: Graphics) = self.synchronized {
        super.paintComponent(g)
        val width = getWidth
        val height = getHeight
        if (initialBoundaries != null) {
          val img = new image.BufferedImage(width, height, image.BufferedImage.TYPE_INT_ARGB)
          for (x <- 0 until width; y <- 0 until height) pixels(y * width + x) = 0
          for (b <- bodies) {
            val px = ((b.x - initialBoundaries.minX) / initialBoundaries.width * width).toInt
            val py = ((b.y - initialBoundaries.minY) / initialBoundaries.height * height).toInt
            if (px >= 0 && px < width && py >= 0 && py < width) pixels(py * width + px) += 1
          }
          for (x <- 0 until width; y <- 0 until height) {
            val factor = 1.0 * bodies.length / (width * height)
            val intensity = pixels(y * width + x) / factor * 40
            val bound = math.min(255, intensity.toInt)
            val color = (255 << 24) | (bound << 16) | (bound << 8) | bound
            img.setRGB(x, y, color)
          }
          if (quadcheckbox.isSelected && quadtree != null) {
            val g = img.getGraphics.asInstanceOf[Graphics2D]
            g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)
            g.setColor(new Color(0, 225, 80, 150))
            def drawQuad(level: Int, quad: Quad): Unit = if (level < 6) quad match {
              case f @ Quad.Fork(cx, cy, sz) =>
                def drawRect(fx: Float, fy: Float, fsz: Float, q: Quad) {
                  val x = ((fx - initialBoundaries.minX) / initialBoundaries.width * width).toInt
                  val y = ((fy - initialBoundaries.minY) / initialBoundaries.height * height).toInt
                  val w = ((fx + fsz - initialBoundaries.minX) / initialBoundaries.width * width).toInt - x
                  val h = ((fy + fsz - initialBoundaries.minY) / initialBoundaries.height * height).toInt - y
                  g.drawRect(x, y, w, h)
                  if (level <= 3) g.drawString("#:" + q.total, x + w / 2, y + h / 2)
                }
                drawRect(cx - sz / 2, cy - sz / 2, sz / 2, f.nw)
                drawRect(cx - sz / 2, cy, sz / 2, f.sw)
                drawRect(cx, cy - sz / 2, sz / 2, f.ne)
                drawRect(cx, cy, sz / 2, f.se)
                drawQuad(level + 1, f.nw)
                drawQuad(level + 1, f.ne)
                drawQuad(level + 1, f.sw)
                drawQuad(level + 1, f.se)
              case Quad.Empty | Quad.Body(_) =>
                // done
            }
            drawQuad(0, quadtree)
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




