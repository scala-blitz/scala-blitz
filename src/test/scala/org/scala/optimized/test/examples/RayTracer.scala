package org.scala.optimized.test.examples


import scala.collection.parallel._
import scala.collection.par._

import Vector._
import javax.swing.JFrame._
import scala.collection.immutable.VectorBuilder
import javax.swing.JPanel
import java.awt.Graphics2D
import java.awt.Graphics
import java.awt.Color
import java.awt.Dimension
import java.awt.Insets
import java.util.Random
import java.awt.image.BufferedImage
import javax.imageio.ImageIO
import java.io.File
import javax.swing.ImageIcon
import javax.swing.JLabel
import javax.swing.JFrame
import scala.collection.immutable.Map
import java.io.FileWriter
import util.Random.nextInt
import javax.swing.JComboBox
import java.awt.BorderLayout
import javax.swing.BorderFactory
import java.awt.GridLayout
import java.awt.event.ActionListener
import java.awt.event.ActionEvent
import javax.swing.JSpinner
import javax.swing.event.ChangeListener
import javax.swing.JTextField
import javax.swing.event.ChangeEvent
import javax.swing.JButton
import java.awt.event.MouseMotionAdapter
import java.awt.event.MouseAdapter
import java.awt.event.MouseEvent
import java.awt.event.MouseWheelEvent
import javax.swing.JCheckBox
import scala.collection.mutable.MutableList
import java.awt.event.KeyListener
import java.awt.event.KeyEvent

object ParallelismType extends Enumeration {
  type parType = Value
  val parOld, parScalaBlitz = Value
}

object RayTracer extends JFrame("Raytracing")  {
  var settingIndex: Integer = 0
  val settingArray: Array[String] = Array("IceCream","PlanetMoon", "NiceLight", "Pool", "SpherePyramid", "TrianglesAndSpheres", "Random")
  
  val r1 = new Random  
  def computeRandomMaterial() = new Material(new Vector3(r1.nextDouble(), r1.nextDouble(), r1.nextDouble()),r1.nextDouble())
  
  val poolSphereList = MutableList[Sphere]()
  for (x <- (0 to 6)){
    for (y <- (0 to 6)){
      poolSphereList+= new Sphere(new Vector3(x*20, y*20, (x+y)*200), 10, computeRandomMaterial)
    }
  }
  
  val pyramidSphereList = MutableList[Sphere]()  
  for (y<- (1 to 4)){
    for (x <- (0 to y)){
      for (z <- (0 to y)){
        pyramidSphereList+= new Sphere(new Vector3(300 + x*20, 200 + y*20, -500+ z*20), 10, computeRandomMaterial)
      }
    }
  }
  
  val triangleList = MutableList[RenderObject]()  
  val p1 = new Vector3(-100,0,0)
  val p2 = new Vector3(100,0,0)
  val p3 = new Vector3(0,100,0)
  val p4 = new Vector3(0,0,100)
  
  triangleList += new Triangle(p1,p2,p3,new Material(new Vector3(0,0,0.8),0.1))
  triangleList += new Triangle(p1,p4,p2,new Material(new Vector3(0.8,0,0),0.1))
  triangleList += new Triangle(p4,p3,p1,new Material(new Vector3(0.8,0,0.8),0.1))
  triangleList += new Triangle(p4,p2,p3,new Material(new Vector3(0.3,0.3,0),0.9))
  triangleList += new Sphere(new Vector3(233, 290, 0), 100, new Material(new Vector3(1,0,0),0.99))
  triangleList += new Sphere(p3, 10, new Material(new Vector3(1,0,0),0.1))
  triangleList += new Sphere(p2, 10, new Material(new Vector3(0,1,0),0.1))
  triangleList += new Sphere(p1, 10, new Material(new Vector3(0,0,1),0.1))
  triangleList += new Sphere(p4, 10, new Material(new Vector3(1,0.5,0.5),0.1))
  
  val iceCreamSetting = Vector(
    new Sphere(new Vector3(233, 290, 0), 100, new Material(new Vector3(1,1,0),0.1)),
    new Sphere(new Vector3(407, 290, 0), 100, new Material(new Vector3(0,1,1),0.1)),
    new Sphere(new Vector3(320, 140, 0), 100, new Material(new Vector3(1,0,1),0.1)))
  val iceCreamLighting = Vector(
    new Light(new Vector3(500, 240, -100),new Vector3(0,0,125)),
    new Light(new Vector3(640, 600, -1000),new Vector3(255,125,125)), 
    new Light(new Vector3(640, -600, 1000),new Vector3(255,125,125)))
  
  val planetMoonSetting = Vector(
    new Sphere(new Vector3(380, 310, -150), 13, new Material(new Vector3(0.9,0.9,0.9),0.7)),
    new Sphere(new Vector3(50, 50, 0), 50, new Material(new Vector3(1,1,0),0.6)),
    new Sphere(new Vector3(450, 320, 0), 100, new Material(new Vector3(0,1,1),0.1)))
  val planetMoonLighting = Vector(
    new Light(new Vector3(-100, -200, -2000),new Vector3(255,125,125)),
    new Light(new Vector3(-100, -200, 2001),new Vector3(255,125,125)))
  
  val niceLightSetting = Vector(
    new Sphere(new Vector3(50, 50, 0), 50, new Material(new Vector3(1,1,0),0.6)),
    new Sphere(new Vector3(250, 250, 100), 30, new Material(new Vector3(1,1,1),0.9)),
    new Sphere(new Vector3(450, 390, 0), 100, new Material(new Vector3(0,1,1),0.1)))
  val niceLightLighting = Vector(
    new Light(new Vector3(0, 0, -1000),new Vector3(255,0,255)),
    new Light(new Vector3(250, 500, -1000),new Vector3(55,55,55)), 
    new Light(new Vector3(1000, 20000, 2001),new Vector3(255,125,124)))
  
  val poolSphereLighting = Vector(
    new Light(new Vector3(-100, 140, 1000),new Vector3(255,255,255)),
    new Light(new Vector3(640, 240, -1000),new Vector3(255,125,125)))
  val pyramidSphereLighting = Vector(
    new Light(new Vector3(-200, 240, 50),new Vector3(125,0,125)),
    new Light(new Vector3(0, 240, -100),new Vector3(0,0,125)),
    new Light(new Vector3(300, 800, 200),new Vector3(125,125,125)),
    new Light(new Vector3(640, 240, -1000),new Vector3(255,125,125)))
  val triangleLighting = Vector(
    new Light(new Vector3(-500, -500, 500),new Vector3(255,255,255)),
    new Light(new Vector3(-640, 240, -1000),new Vector3(255,255,255)),
    new Light(new Vector3(640, 240, 500),new Vector3(255,255,255)))
  
  def  computeNewrandomSetting(): Vector[RenderObject] = {
    val p1 = new Vector3(-r1.nextDouble()*640,0,0)
    val p2 = new Vector3(r1.nextDouble()*640,0,0)
    val p3 = new Vector3(0,r1.nextDouble()*640,0)
    val p4 = new Vector3(0,0,r1.nextDouble()*640)    
    
    Vector(
      new Sphere(new Vector3(r1.nextDouble()*640, r1.nextDouble() * 480, 0), 100, computeRandomMaterial),
      new Sphere(new Vector3(r1.nextDouble()*640, r1.nextDouble() * 480, 0), 50, computeRandomMaterial),
      new Sphere(new Vector3(r1.nextDouble()*640, r1.nextDouble() * 480, 0), 100, computeRandomMaterial),
      new Sphere(new Vector3(r1.nextDouble()*500, r1.nextDouble() * 300, 0), 100, computeRandomMaterial),
      new Sphere(new Vector3(r1.nextDouble()*300, r1.nextDouble() * 200, 0), 100, computeRandomMaterial),
      new Triangle(p1,p2,p3,computeRandomMaterial),
      new Triangle(p1,p4,p2,computeRandomMaterial),
      new Triangle(p4,p3,p1,computeRandomMaterial),
      new Triangle(p4,p2,p3,computeRandomMaterial)     
    )
  }
  val randomLighting = Vector(
    new Light(new Vector3(0, 240, 1000),new Vector3(125,125,125)),
    new Light(new Vector3(640, 240, -1000.0),new Vector3(255,125,125)))
  
  var objectSettings: Vector[Vector[RenderObject]] = Vector(
    iceCreamSetting, planetMoonSetting, niceLightSetting, 
    poolSphereList.toVector, pyramidSphereList.toVector, triangleList.toVector, computeNewrandomSetting)
  val lightSettings: Vector[Vector[Light]] = Vector(
    iceCreamLighting, planetMoonLighting, niceLightLighting,  
    poolSphereLighting, pyramidSphereLighting, triangleLighting, triangleLighting)
    
  val computationLabel = new JLabel()
  
  def start(): Unit = {
    setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
    setSize(1100, 600)
    setLayout(new BorderLayout)
    var label = new RenderImage()
    label.initialize(objectSettings.apply(settingIndex), lightSettings.apply(settingIndex), computationLabel)
    
    label.updateImage()
    label.setVisible(true)
    
    add(label, BorderLayout.CENTER)
    val right = new JPanel()
    right.setLayout(new BorderLayout)
    val panel = new JPanel()
    panel.setLayout(new GridLayout(0, 1))
    val controls = new JPanel()
    controls.setLayout(new GridLayout(0, 2))
    controls.add(new JLabel("Implementation"))
    val implcombo = new JComboBox[String](Array("Classic Parallel Collections", "ScalaBlitz"))
    implcombo.addActionListener(new ActionListener {
      def actionPerformed(e: ActionEvent) {
        if (implcombo.getSelectedItem() == "Classic Parallel Collections"){
          label.changeParallelismType(ParallelismType.parOld)         
        }
        if (implcombo.getSelectedItem() == "ScalaBlitz"){
          label.changeParallelismType(ParallelismType.parScalaBlitz)
        }
        label.updateImage()
      }
    })    
    controls.add(implcombo)
    
    controls.add(new JLabel("Parallelism"))
    val items = (1 to Runtime.getRuntime.availableProcessors map { _.toString }).toArray
    val parcombo = new JComboBox[String](items)
    parcombo.setSelectedIndex(items.length - 1)
    parcombo.addActionListener(new ActionListener {
    def actionPerformed(e: ActionEvent) {
      label.parallelism = parcombo.getSelectedIndex() + 1
      label.updateImage()
      }
    })
    controls.add(parcombo)
    
    controls.add(new JLabel("Configuration"))
    val settingsCombo = new JComboBox[String]()
    settingsCombo.addActionListener(new ActionListener {
      (0 to objectSettings.size -1).foreach(a => settingsCombo.addItem(settingArray.apply(a)))
      settingsCombo.setSelectedIndex(objectSettings.size -1)      
      settingsCombo.setSelectedIndex(0)
      def actionPerformed(e: ActionEvent) {        
        settingIndex = settingsCombo.getSelectedIndex()
        if (settingIndex == objectSettings.size -1){
          //update the random setting
          objectSettings = objectSettings.updated(objectSettings.size-1, computeNewrandomSetting)
        }
        label.initialize(objectSettings.apply(settingIndex), lightSettings.apply(settingIndex), computationLabel)
        label.updateImage()
      }
    })
    
    controls.add(settingsCombo)
    
    val reflectionBox = new JCheckBox()
    reflectionBox.addChangeListener(new ChangeListener{
      
      def stateChanged(e: ChangeEvent) {      
        if (label.reflectionRate == 0 && reflectionBox.isSelected()){
          label.reflectionRate = 2
          label.updateImage()
        
        }
        else if (label.reflectionRate == 2 && !reflectionBox.isSelected()) {
          label.reflectionRate = 0
          label.updateImage()        
        }
      }
    })
    controls.add(new JLabel("Reflection"))
    controls.add(reflectionBox)
    
    panel.add(controls)
    
    panel.add(computationLabel)
    
    panel.add(new JLabel("<html>Drag the image to move in the x/y-plane<br/>Press 'w'/'s' to move on the z-axis<br/>"
      + "Press 'a'/'d' to toggle the yaw angle<br/>Press 'y'/'x' to toggle the pitch angle<br/>"
      + "Scroll with the mouse wheel to zoom.<br/>Press 'p' to print the current camera-setting.</html"))   
    
    right.add(panel, BorderLayout.NORTH)
    add(right, BorderLayout.EAST)
    setVisible(true)
  }
      
  def main(args: Array[String]) { 
    start()    
  }
}

class RenderImage extends JLabel {  
  
  
  val Witdth = 640
  val Height = 480
  val CamZ = -1000
  val MaxLengthRay = 20000
  
  var parallelismType  = ParallelismType.parOld
  var parallelism = Runtime.getRuntime.availableProcessors
  var timeLabel: JLabel = null
  var xlast = 0.0
  var ylast = 0.0
  var xOff = 0.0
  var yOff = 0.0
  var zOff = 0.0
  var pitch =0.0
  var sinPitch = 0.0
  var cosPitch = 0.0
  var yaw = 0.0
  var sinYaw = 0.0
  var cosYaw = 0.0
  var zoom = 30.0
  var zoomFactor = 0.0
  var reflectionRate = 0
  val aspectratio: Double = Witdth.toDouble / Height.toDouble
  
  var renderObjects: Vector[RenderObject] = null
  var lights: Vector[Light] = null  
  
  //needed for the old scheduler
  var fj = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parallelism))
  
  //needed for the scala blitz scheduler
  var conf = new Scheduler.Config.Default(parallelism)
  implicit var s = new Scheduler.ForkJoin(conf)    

  def changeParallelismType(parType : ParallelismType.Value) = {
    parallelismType = parType
    if (parallelismType== ParallelismType.parOld) {
      s.pool.shutdown()
      fj = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parallelism))
    }
    else {
      fj.environment.shutdown()		   
      conf = new Scheduler.Config.Default(parallelism)
      s = new Scheduler.ForkJoin(conf)
    }    
  }
  
  def initialize(objects: Vector[RenderObject], lights: Vector[Light], timeLabel: JLabel) = {
    xlast = 0
    ylast = 0
    xOff = 0
    yOff = 0
    zOff = 0
    pitch =0.0
    yaw = 0.0
    zoom = 30.0
    
    this.renderObjects = objects
    this.lights = lights
    this.timeLabel = timeLabel    
  }
      
  def updateImage()  = {
    zoomFactor = Math.tan(Math.PI * 0.5 * zoom / 180)
    sinPitch = Math.sin(pitch)
    sinYaw = Math.sin(yaw)
    cosPitch = Math.cos(pitch)
    cosYaw = Math.cos(yaw)
    
    val time1 = System.currentTimeMillis()
    if (parallelismType== ParallelismType.parOld) {     
      setIcon(new ImageIcon((drawParallelOld())))
    }
    else if (parallelismType == ParallelismType.parScalaBlitz){
      setIcon(new ImageIcon((drawScalaBlitz())))		     
    }
    if (timeLabel!=null) {
       timeLabel.setText((System.currentTimeMillis()-time1) + " ms")
    }
  }
    
  addMouseMotionListener(new MouseMotionAdapter {
    override def mouseDragged(e: MouseEvent) {
    val xcurr = e.getX
    val ycurr = e.getY
    if (xlast != -1) {
      val calculation = (xcurr - xlast)*zoom/10
      
      //for x change
      xOff -= calculation*cosYaw
      zOff -= calculation*sinYaw
    }
    if (ylast != -1) {
      val calculation = (ycurr - ylast)*zoom/10      
      
      //for y change
      xOff += calculation*sinPitch*sinYaw 
      yOff += calculation*cosPitch
      zOff -= calculation*sinPitch*cosYaw    
      }
    xlast = xcurr
    ylast = ycurr
    updateImage()    
    }
  })
  setFocusable(true)
  addMouseListener(new MouseAdapter {
    
    override def mousePressed(e: MouseEvent) {
    requestFocus()
    xlast = -1
    ylast = -1
    }
    })
    
  addMouseWheelListener(new MouseAdapter {
    override def mouseWheelMoved(e: MouseWheelEvent) {
      val tempZoom = Math.abs(zoom  - (zoom * 0.005 * e.getWheelRotation - e.getWheelRotation))% 90
      if (0<tempZoom && tempZoom < 90){
        zoom = tempZoom
        updateImage()        
      }
    }
  })
  
  addKeyListener(new KeyListener{
    override def keyPressed(k: KeyEvent){
      if (k.getKeyChar() == 'y'){
        pitch += 0.1
        updateImage()
      }
      if (k.getKeyChar() == 'x'){
        pitch -= 0.1
        updateImage()
      }
      if (k.getKeyChar() == 'a'){
        yaw += 0.1 
        updateImage()
      }
      if (k.getKeyChar() == 'd'){
        yaw -= 0.1 
        updateImage()
      }
      if (k.getKeyChar() == 's'){
        xOff += 30*cosPitch*sinYaw
        yOff -= 30*sinPitch 
        zOff -= 30*cosPitch*cosYaw
        updateImage()
      }
      if (k.getKeyChar() == 'w'){
        xOff -= 30*cosPitch*sinYaw
        yOff += 30*sinPitch 
        zOff += 30*cosPitch*cosYaw
        updateImage()
      }
      if (k.getKeyChar() == 'p'){
        println("pitch: " + pitch)
        println("yaw: " + yaw)
        println("x: " +(Witdth/2 + xOff))
        println("y: " + (Height/2 +yOff))
        println("z: " + (CamZ +zOff))
        println("zoom: "+ zoom)
        
        println("M: " + (-cosPitch*sinYaw) +","+sinPitch+"," + cosPitch*cosYaw)
        println("NX: " + (cosYaw) +","+(0)+"," + (sinYaw))
        println("NY: " + (-cosPitch*sinYaw) +","+sinPitch+"," + cosPitch*cosYaw)
      
      }
      }
    override def keyTyped(e: KeyEvent){      
    }
    override def keyReleased(e: KeyEvent){    
    }
  }) 
  
  def drawParallelOld(): BufferedImage = {    
    val img = new BufferedImage(Witdth, Height, BufferedImage.TYPE_INT_ARGB)
    
    if(fj.parallelismLevel != parallelism){
      fj.environment.shutdown()
      fj = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parallelism))
    }
    val range = 0 until (Witdth * Height)
    val pr = range.par
    pr.tasksupport = fj
    for (pixel<- pr)  {
      val y  = (pixel / Witdth)
      val x  = pixel % Witdth
      img.setRGB(x, y, drawPixel(x, y))    
    }    
    img
  } 
  def drawScalaBlitz(): BufferedImage = {  
    val img = new BufferedImage(Witdth, Height, BufferedImage.TYPE_INT_ARGB)
    
    if(conf.parallelismLevel != parallelism){
      s.pool.shutdown()
      conf = new Scheduler.Config.Default(parallelism)
      s = new Scheduler.ForkJoin(conf)    
    }

    for (pixel<- ((0 to Witdth*Height -1).toPar))  {
      val y  = (pixel / Witdth)
      val x  = pixel % Witdth
      img.setRGB(x, y, drawPixel(x, y))
    }
    img
  }
  
  def drawPixel(x: Double, y:Double) = {
    
    var red, blue, green: Double = 0.0
      
    val xx = (2 * (x / Witdth.toDouble) - 1) *aspectratio * zoomFactor    
    val yy = (1 - 2 * (y / Height.toDouble))* zoomFactor 
      
    val xDir = -cosPitch*sinYaw + xx*cosYaw + yy*sinPitch*sinYaw
    val yDir = sinPitch +yy*cosPitch 
    val zDir = cosPitch*cosYaw  + xx*sinYaw - yy*sinPitch*cosYaw    
      
    val currentViewRay = new Ray(new Vector3(Witdth/2 + xOff, Height/2 + yOff, CamZ + zOff), (new Vector3(xDir, yDir, zDir).normalize), MaxLengthRay)
      
    computeColorContribution(currentViewRay, 0, 1.0)    
      
    def computeColorContribution(currentViewRay: Ray, reflectionLevel: Int, reflectionCoefficient: Double): Unit  = {      
      var currentRenderObject: RenderObject = null
      
      //search the closest intersection
      for (renderObject <- renderObjects){ 
        if (renderObject.intersect(currentViewRay)){                
          currentRenderObject = renderObject                
        }
      }            
      if (currentRenderObject != null){ //if we found an intersection        
        val  intersectionPoint = new Vector3(currentViewRay.start.x+currentViewRay.dir.x*currentViewRay.t,
          currentViewRay.start.y+currentViewRay.dir.y*currentViewRay.t, 
          currentViewRay.start.z +currentViewRay.dir.z*currentViewRay.t)
        val n = currentRenderObject.normalVectorToSurfaceAtPoint(intersectionPoint) // compute the normal vector, normalize it
        for (currentLight <- lights){
          val fromInteresectionToLight = currentLight.pos.subtractVector(intersectionPoint)
          
          //This is to check if the lightsource is not on the other side of the sphere. 
          //If it is, that light does not contribute at all (currentSphere is the reason of the shadow for this light)
          if (fromInteresectionToLight.squareNorm >0.0){
           
            // we normalize fromInteresectionToLight and keep track of the t that we found, 
            //so that we just have to find intersections between the light source and intersectPoint
            val newT = Math.sqrt(fromInteresectionToLight.squareNorm)
            
            //now we create a lightRay for the current light which has as direction the normalized fromInteresectionToLight 
            val lightRay = new Ray(intersectionPoint, 
              new Vector3(fromInteresectionToLight.x/newT, fromInteresectionToLight.y/newT, fromInteresectionToLight.z/newT), newT) 
            
            // we check if it illuminates the intersecting object: is it in the shadow of some sphere?
            val inShadow = renderObjects.exists(o => o.intersect(lightRay))
            if (!inShadow){ 
              val lambertValue = Math.abs(lightRay.dir.scalarProdWith(n) * reflectionCoefficient)              
              red += lambertValue * currentLight.color.x*currentRenderObject.material.color.x
              green += lambertValue * currentLight.color.y*currentRenderObject.material.color.y
              blue += lambertValue * currentLight.color.z*currentRenderObject.material.color.z            
              }                               
            }                  
          }
          //We prepare the next reflection: "The reflection sees what this part of the sphere would see"
          if (reflectionLevel < reflectionRate && reflectionCoefficient > 0){ //if we have to do a reflection 
            val reflection =  2*currentViewRay.dir.scalarProdWith(n)
            val reflectionRayDirection = currentViewRay.dir.subtractVector(new Vector3(n.x*reflection, n.y*reflection, n.z*reflection))
            
            computeColorContribution(new Ray(intersectionPoint, reflectionRayDirection, MaxLengthRay),
              reflectionLevel+1, 
              reflectionCoefficient*currentRenderObject.material.reflection)          
          }           
        }          
      }
            
    red = math.min(red, 255).toInt << 16
    green = math.min(green, 255).toInt << 8
    blue = math.min(blue, 255).toInt << 0
    val a = 255 << 24
    a.toInt| red.toInt | green.toInt | blue.toInt //return the resulting color    
  } 
}
  
class Vector3(xt: Double, yt: Double, zt: Double){
  var x: Double = xt
  var y: Double= yt
  var z: Double= zt
  val squareNorm = scalarProdWith(this)
 
  def subtractVector(v2: Vector3): Vector3 = new Vector3(x -v2.x, y -v2.y, z -v2.z)  
  def scalarProdWith(v2: Vector3): Double = x*v2.x + y*v2.y + z*v2.z  
  def crossProdWith(v2: Vector3): Vector3 = new Vector3(y*v2.z-z*v2.y, z*v2.x - x*v2.z, x*v2.y-y*v2.x)    
  def normalize(): Vector3 = {    
    if (squareNorm > 0){    
      x = x/Math.sqrt(squareNorm)
      y = y/Math.sqrt(squareNorm)
      z = z/Math.sqrt(squareNorm)      
      this
    }
    else {
      new Vector3(0, 0, 0)
    }
  }
}
  
abstract class RenderObject(mat: Material){
  val EPSILON = 0.000001
  val material = mat
  def intersect(r: Ray): Boolean
  def normalVectorToSurfaceAtPoint(point: Vector3): Vector3
}


class Triangle(xT: Vector3, yT: Vector3, zT: Vector3, mat: Material) extends RenderObject(mat: Material){
  val x = xT
  val y = yT
  val z = zT
  
  val e1 = this.y.subtractVector(this.x)
  val e2 = this.z.subtractVector(this.x)
  
  def normalVectorToSurfaceAtPoint(point: Vector3): Vector3 = e1.crossProdWith(e2).normalize  
  def intersect(ray: Ray): Boolean = {    

   //The Vector3 methods subVector, crossProdWith and scalaProdWith are not used here as they slow the code down by recreating another Vector3-object every time.
   //In the other portions of the code, this is not an issue, but here, in the most
   //used code section, they are not necessary and make the code run a little bit slower.

    val px = ray.dir.y*e2.z-ray.dir.z*e2.y
    val py = ray.dir.z*e2.x - ray.dir.x*e2.z
    val pz = ray.dir.x*e2.y-ray.dir.y*e2.x 
        
    //if the determinant is near zero, then the ray lies in the plane of the triangle
   
    val det = e1.x*px + e1.y*py + e1.z*pz 
    if (det > -EPSILON && det < EPSILON){
      return false
    } 
    val inv_det = 1 / det    
   
    val tx = ray.start.x -this.x.x     
    val ty = ray.start.y -this.x.y     
    val tz = ray.start.z -this.x.z     

    val u = (tx*px + ty*py + tz*pz) * inv_det
   
    //The intersection lies outside of the triangle
    if (u < 0 || u > 1){    
      return false
    }    
  
    val qx = ty*e1.z-tz*e1.y
    val qy = tz*e1.x - tx*e1.z
    val qz = tx*e1.y-ty*e1.x 
        
    val v = (ray.dir.x*qx + ray.dir.y*qy + ray.dir.z*qz) * inv_det
    //The intersection lies outside of the triangle
    if (v < 0 || u + v  > 1) {    
      return false
    }    
   
    val t = (e2.x*qx + e2.y*qy + e2.z*qz) * inv_det
    
    if (t <= ray.t && t >EPSILON) {  //return ture only if the interestion is closer than any of the previous one's we found  
      ray.t = t    
      return true
    } 
    return false    
  }
}
  
class Sphere(sPos: Vector3, sSize: Double, mat: Material) extends RenderObject(mat: Material) {
  val center: Vector3 = sPos
  val radius: Double = sSize
  
  def normalVectorToSurfaceAtPoint(point: Vector3): Vector3 = point.subtractVector(this.center).normalize()
  
  def intersect(r: Ray): Boolean = {
    //The Vector3 methods subVector and scalaProdWith are not used here as they slow the code down by recreating another Vector3-object every time.
    //In the other portions of the code, this is not an issue, but here, in the most
    //used code section, they are not necessary and make the code run a little bit slower.
    val A = r.dir.x*r.dir.x + r.dir.y*r.dir.y + r.dir.z*r.dir.z
    val B = 2 * (r.dir.x * (r.start.x - this.center.x) + r.dir.y * (r.start.y - this.center.y) + r.dir.z * (r.start.z - this.center.z))
    val c1 = r.start.x - this.center.x
    val c2 = r.start.y - this.center.y
    val c3 = r.start.z - this.center.z
    val C = c1*c1 + c2*c2 + c3*c3 - this.radius*this.radius
    val delta = B*B - 4* A*C 
    
    if (delta <0){
      return false
    }
    val t0 = (-B - Math.sqrt(delta))/(2*A)
    
    if (t0 <= r.t && t0 > EPSILON){ //take the first intersection with the sphere that is closer than any of the previous one's we found
      r.t = t0
      return true
    }
    val t1 = (-B + Math.sqrt(delta))/(2*A) //compute t1 only if necessary, as we allways have t0<t1
    if (t1 <= r.t && t1 > EPSILON){    
      r.t= t1
      return true
    }
    return false    
  }
}
  
class Material(colorS: Vector3, reflectionS: Double){
  val color: Vector3 = colorS //Represents the color of that material on a scale from 0 to 1 for each entry
  val reflection: Double = reflectionS
}

class Light(lPos: Vector3, lcolor: Vector3) {
  val pos: Vector3 = lPos
  val color = lcolor

}

class Ray(rStart: Vector3, rDir: Vector3, rT: Double) {
  var start: Vector3 = rStart
  var dir = rDir
  var t: Double = rT //represents the limit until where on the halfline starting at start we test for intersections.
}
