package scala.collection.parallel.examples



import scala.collection.parallel._
import scala.collection.parallel.Par._
import scala.collection.parallel.workstealing.Ops._
import java.awt._
import javax.swing._



object Mandelbrot {

  class MandelCanvas(frame: MandelFrame) extends JComponent {
    val pixels = new Array[Int](4000 * 4000)

    def parallelism = 1
    def threshold = 1000
    def xlo = -1.00
    def ylo = +0.40
    def xhi = -0.60
    def yhi = +0.80

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
  
    private def fill(pixels: Array[Int], size: Int)(implicit ws: workstealing.WorkstealingTreeScheduler) {
      val range = 0 until (size * size)
  
      for (idx <- range.toPar) {
        val x = idx % size
        val y = idx / size
        val xc = xlo + (xhi - xlo) * x / size
        val yc = ylo + (yhi - ylo) * y / size
  
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

      val size = math.min(getWidth, getHeight)
      val conf = new workstealing.WorkstealingTreeScheduler.Config.Default(parallelism)
      implicit val s = new workstealing.WorkstealingTreeScheduler.ForkJoin(conf)
      val start = System.nanoTime
      fill(pixels, size)
      val end = System.nanoTime
      val time = (end - start) / 1000000.0
      val stats = "size: " + size + "x" + size + ", parallelism: " + parallelism + ", time: " + time + " ms"
      println("Rendering: " + stats)
      frame.setTitle("Mandelbrot: " + stats)

      val img = new image.BufferedImage(size, size, image.BufferedImage.TYPE_INT_ARGB)
      //val raster = img.getData().asInstanceOf[image.WritableRaster]
      //raster.setPixels(0, 0, size, size, pixels)
      for (x <- 0 until size; y <- 0 until size) {
        val color = pixels(y * size + x)
        img.setRGB(x, y, color)
      }
      g.drawImage(img, 0, 0, null)
      javax.imageio.ImageIO.write(img, "png", new java.io.File("mandelbrot.png"))
    }
  }

  class MandelFrame extends JFrame("Mandelbrot") {
    setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
    setSize(800, 600)
    setLayout(new BorderLayout)
    val canvas = new MandelCanvas(this)
    add(canvas, BorderLayout.CENTER)
    setVisible(true)
  }

  def main(args: Array[String]) {
    val frame = new MandelFrame
  }

}