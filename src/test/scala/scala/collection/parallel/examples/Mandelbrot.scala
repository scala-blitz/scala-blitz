package scala.collection.parallel.examples



import scala.collection.parallel._
import scala.collection.parallel.Par._
import scala.collection.parallel.workstealing.Ops._
import java.awt._
import javax.swing._



object Mandelbrot {

  class MandelCanvas(frame: MandelFrame) extends JComponent {
    val pixels = new Array[Int](4000 * 4000)

    def parallelism = 8

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
      val xlo = -2.0
      val ylo = -2.0
      val xhi = +2.0
      val yhi = +2.0
      val threshold = 10000
  
      for (idx <- range.toPar) {
        val x = idx % size
        val y = idx / size
        val xc = xlo + (xhi - xlo) * x / size
        val yc = ylo + (yhi - ylo) * y / size
  
        pixels(idx) = compute(xc, yc, threshold)
      }
    }

    override def paintComponent(g: Graphics) {
      super.paintComponent(g)

      val size = math.min(getHeight, getWidth)
      val conf = new workstealing.WorkstealingTreeScheduler.Config.Default(parallelism)
      implicit val s = new workstealing.WorkstealingTreeScheduler.ForkJoin(conf)
      val start = System.nanoTime
      fill(pixels, size)
      val end = System.nanoTime
      val time = (end - start) / 1000000.0
      val stats = "size: " + size + "x" + size + ", parallelism: " + parallelism + ", time: " + time + " ms"
      println("Rendering: " + stats)
      frame.setTitle("Mandelbrot: " + stats)


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