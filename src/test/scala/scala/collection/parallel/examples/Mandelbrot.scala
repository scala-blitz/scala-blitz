package scala.collection.parallel.examples



import scala.collection.parallel._
import scala.collection.parallel.Par._
import scala.collection.parallel.workstealing.Ops._
import java.awt._
import java.awt.event._
import javax.swing._
import javax.swing.event._



object Mandelbrot {

  class MandelCanvas(frame: MandelFrame) extends JComponent {
    val pixels = new Array[Int](4000 * 4000)

    def parallelism = {
      val selidx = frame.parcombo.getSelectedIndex
      frame.parcombo.getItemAt(selidx).toInt
    }
    def threshold = frame.threshold.getText.toInt
    def zoom = frame.zoomlevel.getValue.asInstanceOf[Int] / 10.0 * 500.0
    var xoff = 0.0
    var yoff = 0.0
    var xlast = -1
    var ylast = -1
    def xlo = xoff - getWidth / zoom
    def ylo = yoff - getHeight / zoom
    def xhi = xoff + getWidth / zoom
    def yhi = yoff + getHeight / zoom

    addMouseMotionListener(new MouseMotionAdapter {
      override def mouseDragged(e: MouseEvent) {
        val xcurr = e.getX
        val ycurr = e.getY
        if (xlast != -1) {
          val xd = xcurr - xlast
          val yd = ycurr - ylast
          xoff -= xd / zoom
          yoff -= yd / zoom
        }
        xlast = xcurr
        ylast = ycurr
        repaint()
      }
    })

    addMouseListener(new MouseAdapter {
      override def mousePressed(e: MouseEvent) {
        xlast = -1
        ylast = -1
      }
    })

    addMouseWheelListener(new MouseAdapter {
      override def mouseWheelMoved(e: MouseWheelEvent) {
        val prev = frame.zoomlevel.getValue.asInstanceOf[Int]
        val next = prev + (prev * -0.1 * e.getWheelRotation - e.getWheelRotation)
        frame.zoomlevel.setValue(math.max(1, next.toInt))
      }
    })

    private def compute(xc: Double, yc: Double, threshold: Int): Int = {
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
  

    private def fill(pixels: Array[Int], wdt: Int, hgt: Int) {
      val selected = frame.implcombo.getSelectedItem
      if (selected == "Workstealing tree") {
        fillWsTree(pixels, wdt, hgt)
      } else {
        fillClassic(pixels, wdt, hgt)
      }
    }

    private def fillClassic(pixels: Array[Int], wdt: Int, hgt: Int) {
      val fj = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parallelism))
      val range = 0 until (wdt * hgt)
      val pr = range.par
      pr.tasksupport = fj

      for (idx <- pr) {
        val x = idx % wdt
        val y = idx / wdt
        val xc = xlo + (xhi - xlo) * x / wdt
        val yc = ylo + (yhi - ylo) * y / hgt

        val iters = compute(xc, yc, threshold)
        val a = 255 << 24
        val r = math.min(255, 1.0 * iters / threshold * 255).toInt << 16
        val g = math.min(255, 2.0 * iters / threshold * 255).toInt << 8
        val b = math.min(255, 3.0 * iters / threshold * 255).toInt << 0
        pixels(idx) = a | r | g | b
      }
    }

    private def fillWsTree(pixels: Array[Int], wdt: Int, hgt: Int) {
      val range = 0 until (wdt * hgt)
      val conf = new workstealing.WorkstealingTreeScheduler.Config.Default(parallelism)
      implicit val s = new workstealing.WorkstealingTreeScheduler.ForkJoin(conf)
  
      for (idx <- range.toPar) {
        val x = idx % wdt
        val y = idx / wdt
        val xc = xlo + (xhi - xlo) * x / wdt
        val yc = ylo + (yhi - ylo) * y / hgt

        val iters = compute(xc, yc, threshold)
        val a = 255 << 24
        val r = math.min(255, 1.0 * iters / threshold * 255).toInt << 16
        val g = math.min(255, 2.0 * iters / threshold * 255).toInt << 8
        val b = math.min(255, 3.0 * iters / threshold * 255).toInt << 0
        pixels(idx) = a | r | g | b
      }
    }

    override def paintComponent(g: Graphics) {
      super.paintComponent(g)

      val start = System.nanoTime
      fill(pixels, getWidth, getHeight)
      val end = System.nanoTime
      val time = (end - start) / 1000000.0
      val stats = "size: " + getWidth + "x" + getHeight + ", parallelism: " + parallelism + ", time: " + time + " ms"
      println("Rendering: " + stats)
      frame.setTitle("Mandelbrot: " + stats)

      val img = new image.BufferedImage(getWidth, getHeight, image.BufferedImage.TYPE_INT_ARGB)
      for (x <- 0 until getWidth; y <- 0 until getHeight) {
        val color = pixels(y * getWidth + x)
        img.setRGB(x, y, color)
      }
      g.drawImage(img, 0, 0, null)
      javax.imageio.ImageIO.write(img, "png", new java.io.File("mandelbrot.png"))
    }
  }

  class MandelFrame extends JFrame("Mandelbrot") {
    setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
    setSize(1024, 600)
    setLayout(new BorderLayout)
    val canvas = new MandelCanvas(this)
    add(canvas, BorderLayout.CENTER)
    val right = new JPanel
    right.setBorder(BorderFactory.createEtchedBorder(border.EtchedBorder.LOWERED))
    right.setLayout(new BorderLayout)
    val panel = new JPanel
    panel.setLayout(new GridLayout(0, 1))
    val controls = new JPanel
    controls.setLayout(new GridLayout(0, 2))
    controls.add(new JLabel("Implementation"))
    val implcombo = new JComboBox[String](Array("Workstealing tree", "Classic parallel collections"))
    implcombo.addActionListener(new ActionListener {
      def actionPerformed(e: ActionEvent) {
        canvas.repaint()
      }
    })
    controls.add(implcombo)
    controls.add(new JLabel("Parallelism"))
    val items = 1 to Runtime.getRuntime.availableProcessors map { _.toString } toArray
    val parcombo = new JComboBox[String](items)
    parcombo.setSelectedIndex(items.length - 1)
    parcombo.addActionListener(new ActionListener {
      def actionPerformed(e: ActionEvent) {
        canvas.repaint()
      }
    })
    controls.add(parcombo)
    controls.add(new JLabel("Zoom"))
    val zoomlevel = new JSpinner
    zoomlevel.setValue(10)
    zoomlevel.addChangeListener(new ChangeListener {
      def stateChanged(e: ChangeEvent) {
        canvas.repaint()
      }
    })
    controls.add(zoomlevel)
    controls.add(new JLabel("Threshold"))
    val threshold = new JTextField("1000")
    threshold.addActionListener(new ActionListener {
      def actionPerformed(e: ActionEvent) {
        canvas.repaint()
      }
    })
    controls.add(threshold)
    panel.add(controls)
    panel.add(new JLabel("Drag canvas to scroll, move wheel to zoom."))
    val renderbutton = new JButton("Render")
    renderbutton.addActionListener(new ActionListener {
      def actionPerformed(e: ActionEvent) {
        canvas.repaint()
      }
    })
    panel.add(renderbutton)
    right.add(panel, BorderLayout.NORTH)
    add(right, BorderLayout.EAST)
    setVisible(true)
  }

  def main(args: Array[String]) {
    val frame = new MandelFrame
  }

}