package org.scala.optimized.test.examples

import scala.collection.parallel._
import scala.collection.par._

import scala.reflect.ClassTag
import scala.util.Random
import scala.concurrent.Lock
import scala.collection
import scala.collection.GenSeq
import javax.swing.JPanel
import java.awt.Polygon
import javax.swing
import javax.swing.JFrame
import javax.swing.JPanel
import javax.swing.JComboBox
import javax.swing.JSlider
import javax.swing.event.ChangeListener
import javax.swing.event.ChangeEvent
import javax.swing.JLabel
import javax.swing.JCheckBox
import javax.swing.ButtonGroup
import javax.swing.JRadioButton
import javax.swing.JButton
import javax.swing.JTextArea
import javax.swing.JFormattedTextField

import java.awt.Graphics2D
import java.awt.Color
import java.awt.Polygon
import java.awt.event.ActionListener
import java.awt.GridLayout
import java.awt.BorderLayout
import java.awt.event.ActionEvent
import java.awt.Dimension

class Bird (var x: Int, var y: Int, var alignmentX: Double,  var alignmentY: Double){
  var polygon = new Polygon();

  def getDeepCopy(): Bird = {
    val newBird = new Bird(x, y, alignmentX, alignmentY)
    newBird.polygon = this.polygon
    newBird
  }
}


class Quadtree (var birds: GenSeq[Bird], val x1: Int, val y1: Int, val x2: Int, val y2: Int, val minSize: Int) {
  val midX = (x2 + x1) / 2
  val midY = (y2 + y1) / 2
  var isEnd = false
  var SE: Quadtree = null
  var SW: Quadtree = null
  var NE: Quadtree = null
  var NW: Quadtree = null
  
  // We check if we need to partition another time : if the bird is alone is the quadtree or if the quadtree is only one pixel
  if (birds.size != 1 && (x2-x1) > minSize && (y2-y1) > minSize ) {
    var listSE, listSW, listNE, listNW = List[Bird]() //To have the same data structure
    var i = 0
    while (i < birds.size) {
    if (birds(i).x <= midX && birds(i).y <= midY) 
      listNW = birds(i) +: listNW
    else if (birds(i).x > midX && birds(i).y <= midY)
      listNE = birds(i) +: listNE
    else if (birds(i).x <= midX && birds(i).y > midY)
      listSW = birds(i) +: listSW
    else
      listSE = birds(i) +: listSE
      i += 1
    }
    
    if (listSE.size != 0)
      SE = new Quadtree(listSE, midX, midY, x2, y2, minSize)
    if (listSW.size != 0)
      SW = new Quadtree(listSW, x1, midY, midX, y2, minSize)
    if (listNE.size != 0)
      NE = new Quadtree(listNE, midX, y1, x2, midY, minSize)
    if (listNW.size != 0)
      NW = new Quadtree(listNW, x1, y1, midX, midY, minSize)
    
  }
  else{
    isEnd = true
  }
  
  //Function to get all the fllocs that are on a certain intervall
  def getBirdsIn(x1In: Int, y1In: Int, x2In: Int, y2In: Int): List[Bird] = {
    if (isEnd)
        birds.toList
    else{
      if ((y2In - y1In) < minSize || (x2In - x1In) < minSize) {
        return this.birds.toList
      }
   
      var birdsReturn = List[Bird]()
      //We search in each of the four quadtrees
      //NW : x <= midX y <= midY
      if (x1In <= midX && y1In <= midY && NW != null){ // If a piece of the square is in NW
        var boundNWX = x2In //bound on the right
        if (boundNWX > this.midX)
          boundNWX = this.midX
        var boundNWY = y2In //Bound on the down
        if (boundNWY > this.midY)
          boundNWY = this.midY
        birdsReturn = this.NW.getBirdsIn(x1In, y1In, boundNWX, boundNWY) ::: (birdsReturn)
      }
      //NE : x > midX y <= midY
      if (x2In > midX && y1In <= midY && NE != null){ //If a piece of the square is in NE
        var boundNEX = x1In // bound on the left
        if (boundNEX <= this.midX)
          boundNEX = this.midX + 1
        var boundNEY = y2In //Bound on the down
        if (boundNEY > this.midY)
          boundNEY = this.midY
        birdsReturn = this.NE.getBirdsIn(boundNEX, y1In, x2In, boundNEY) ::: (birdsReturn)
      }
      //SW : x <= midX, y > midY
      if (x1In <= midX && y2In > midY && SW != null){ //If a piece of the square is in SW
        var boundSWX = x2In //bound on the right
        if (boundSWX > this.midX)
          boundSWX = this.midX
        var boundSWY = y1In //bount on the top
        if (boundSWY <= this.midY)
          boundSWY = this.midY + 1
      birdsReturn = this.SW.getBirdsIn(x1In, boundSWY, boundSWX, y2In) ::: (birdsReturn)  
      }  
      //SE : x > midX y > midY
      if (x2In > midX && y2In > midY && SE != null){
        var boundSEX = x1In
        if (boundSEX <= this.midX)
          boundSEX = this.midX + 1
        var boundSEY = y1In
        if (boundSEY <= this.midY)
          boundSEY = this.midY + 1
        birdsReturn = this.SE.getBirdsIn(this.midX, this.midY, x2In, y2In) ::: (birdsReturn)
      }
      birdsReturn.toList
    }
  }
}




object Graphics extends JPanel{
  val heightPanel = 700
  val widthPanel = 600
  val sizeButtons = 480 //Size of the buttons interface
  val backGroundColor = new Color(185, 251, 243)
  val jFrame = new JFrame("Flocking algorithm")
  
  
  //Controllers
  //Execution control
  val playBox = new JCheckBox("Play", Flocking.play)
  val nextStepButton = new JButton("Next step");
  val restartButton = new JButton("Restart");
  val info = new JTextArea("")
  
  
  //Initial values
  val notparRadio = new JRadioButton("Normal Mode", ! Flocking.usePar)
  val parRadio = new JRadioButton("Parrallel Mode", Flocking.usePar)
  val toParRadio = new JRadioButton("Workstealing tree mode", Flocking.useToPar)
  val processorsAvailable = 1 to (Runtime.getRuntime.availableProcessors) map { _.toString } toArray //numberOfProcess
  val numberOfProcessorsComboBox = new JComboBox(processorsAvailable)
  numberOfProcessorsComboBox.setSelectedIndex(Flocking.numberOfProcessorsUsed - 1)
  
  
  val jsliderNumberBirds = new JSlider(1, 10000, Flocking.numberOfBirds)
  val textFieldNumberBirds = new JFormattedTextField(Flocking.numberOfBirds)
  
  jsliderNumberBirds.setPaintTicks(true); 
  jsliderNumberBirds.setPaintLabels(true); 
  
  //Range
  val diminutionOfJslider = 120
  val jsliderShortRange = new JSlider(1, 20, Flocking.shortRange)
  val jsliderShortRangeRate = new JSlider(1, 80, (Flocking.coeffShort * (-10)).asInstanceOf[Int])
  val jsliderLongRange = new JSlider(1, widthPanel, Flocking.longRange)
  val jsliderLongRangeRate = new JSlider(1, 100, (Flocking.coeffLong * 100).asInstanceOf[Int])
 
  val textFieldShortRange = new JFormattedTextField(Flocking.shortRange)
  val textFieldShortRangeRate = new JFormattedTextField((Flocking.coeffShort * (-10).asInstanceOf[Int]))
  val textFieldLongRange = new JFormattedTextField(Flocking.longRange)
  val textFieldLongRangeRate = new JFormattedTextField((Flocking.coeffLong * 100).asInstanceOf[Int])
  
  val d = jsliderShortRange.getPreferredSize()
  jsliderShortRange.setPreferredSize(new Dimension(d.width - diminutionOfJslider, d.height))
  jsliderShortRangeRate.setPreferredSize(new Dimension(d.width - diminutionOfJslider, d.height))
  jsliderLongRange.setPreferredSize(new Dimension(d.width - diminutionOfJslider, d.height))
  jsliderLongRangeRate.setPreferredSize(new Dimension(d.width - diminutionOfJslider, d.height))
  
  //Miscellaneous 
  val jsliderMovingForward = new JSlider(1, 50, (Flocking.coefMovingForward * 10).asInstanceOf[Int])
  val jsliderCoefficientTurn = new JSlider(1, 99, (Flocking.coeffTurn * 100).asInstanceOf[Int])
  val jsliderSpeed = new JSlider(1, 200, 200 - Flocking.minSpeed.asInstanceOf[Int])
  
  val textFieldMovingForward = new JFormattedTextField((Flocking.coefMovingForward * 10).asInstanceOf[Int])
  val textFieldCoefficientTurn = new JFormattedTextField((Flocking.coeffTurn * 100).asInstanceOf[Int])
  val textFieldSpeed = new JFormattedTextField(200 - Flocking.minSpeed.asInstanceOf[Int])
  
  val groupModeBox = new JCheckBox("Group Mode", Flocking.groupMode)
  val wallsBox = new JCheckBox("Walls", Flocking.walls)

  //Quadtree
  val useQuadtreeBox = new JCheckBox("Use", Flocking.useQuadtree)
  val quadtreeVisibleBox = new JCheckBox("Visible", Flocking.drawQuadTree)
  val jsliderQuadTreeMinSize = new JSlider(1, widthPanel, Flocking.minSizeQuadTree)
  val textFieldQuadTreeMinSize = new JFormattedTextField(Flocking.minSizeQuadTree)
  jsliderQuadTreeMinSize.setPreferredSize(new Dimension(d.width - diminutionOfJslider, d.height))
  
  var lock = new Lock()
  var quadtree: Quadtree = null
  
  def cosPI4 = Math.cos(Math.PI / 4)
  def sinPI4 = Math.sin(Math.PI / 4)
  def cosMinusPI4 = Math.cos(-Math.PI / 4)
  def sinMinusPI4 = Math.sin(-Math.PI / 4)
  
  var arrayBirds: GenSeq[Bird] = Vector[Bird]()
  def init(){
    //First we define the main frame
    jFrame.setSize(heightPanel + sizeButtons, widthPanel + 40)
    this.setSize(heightPanel, widthPanel)
      jFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)

      //Now the buttons
      val jpanelButtons = new JPanel()
    
    //The listeners to link our commands to our variables
    //Execution control
    playBox.addActionListener(
        new ActionListener {
          def actionPerformed(e: ActionEvent) {
            Flocking.play = e.getSource().asInstanceOf[JCheckBox].isSelected()
          }
        }
    )
    nextStepButton.addActionListener(new ActionListener {
      def actionPerformed(e: ActionEvent) {
          Flocking.justOnePlay = true
        }
      }
    )
    restartButton.addActionListener(new ActionListener {
      def actionPerformed(e: ActionEvent) {
          Flocking.numberOfBirdsChanged = true
          Flocking.timeSincePlay = System.currentTimeMillis()
        }
      }
    )
    //Initial values
    jsliderNumberBirds.addChangeListener(
      new ChangeListener {
        def stateChanged(e: ChangeEvent) {
          Flocking.numberOfBirds = e.getSource().asInstanceOf[JSlider].getValue()
          textFieldNumberBirds.setValue(e.getSource().asInstanceOf[JSlider].getValue())
          Flocking.numberOfBirdsChanged = true   
        }
      }
    )
      
    textFieldNumberBirds.addActionListener(
      new ActionListener {
        def actionPerformed(e: ActionEvent) {
          val value = textFieldNumberBirds.getText().toInt
          if (value >= jsliderNumberBirds.getMinimum() && value <= jsliderNumberBirds.getMaximum()){
            Flocking.numberOfBirds = value
            jsliderNumberBirds.setValue(value)
            Flocking.numberOfBirdsChanged = true
          }
        }
      }
    )  
      
      
      notparRadio.addActionListener(
        new ActionListener {
          def actionPerformed(e: ActionEvent) {
            Flocking.usePar = ! e.getSource().asInstanceOf[JRadioButton].isSelected()
            Flocking.useToPar = Flocking.usePar
            Flocking.parChanged = true
          }
        }
      )

      parRadio.addActionListener(
        new ActionListener {
          def actionPerformed(e: ActionEvent) {
            Flocking.usePar = e.getSource().asInstanceOf[JRadioButton].isSelected()
            Flocking.useToPar = ! Flocking.usePar
            Flocking.parChanged = true
          }
        }
      )

      toParRadio.addActionListener(
        new ActionListener {
          def actionPerformed(e: ActionEvent) {
            Flocking.useToPar = e.getSource().asInstanceOf[JRadioButton].isSelected()
            Flocking.usePar = ! Flocking.useToPar
            Flocking.parChanged = true
          }
        }
      )

      numberOfProcessorsComboBox.addActionListener(new ActionListener {
      def actionPerformed(e: ActionEvent) {
          Flocking.numberOfProcessorsUsed = Graphics.numberOfProcessorsComboBox.getItemAt(Graphics.numberOfProcessorsComboBox.getSelectedIndex).toInt + 1
          Flocking.parChanged = true
        }
      }
    )
    
    
    //Ranges
    jsliderShortRange.addChangeListener(
      new ChangeListener {
        def stateChanged(e: ChangeEvent) {
          Flocking.shortRange = e.getSource().asInstanceOf[JSlider].getValue()
          textFieldShortRange.setValue(e.getSource().asInstanceOf[JSlider].getValue())
        }
      }
    )

    jsliderShortRangeRate.addChangeListener(
      new ChangeListener {
        def stateChanged(e: ChangeEvent) {
          Flocking.coeffShort = e.getSource().asInstanceOf[JSlider].getValue().asInstanceOf[Double] / -10.0
          textFieldShortRangeRate.setValue(e.getSource().asInstanceOf[JSlider].getValue())
        }
      }
    )
      
      
    jsliderLongRange.addChangeListener(
      new ChangeListener {
        def stateChanged(e: ChangeEvent) {
          Flocking.longRange = e.getSource().asInstanceOf[JSlider].getValue()
          textFieldLongRange.setValue(e.getSource().asInstanceOf[JSlider].getValue())
        }
      }
    )
      
    jsliderLongRangeRate.addChangeListener(
      new ChangeListener {
        def stateChanged(e: ChangeEvent) {
          Flocking.coeffLong = e.getSource().asInstanceOf[JSlider].getValue().asInstanceOf[Double] / 100.0
          textFieldLongRangeRate.setValue(e.getSource().asInstanceOf[JSlider].getValue())
        }
      }
    )
      
    textFieldShortRange.addActionListener(
      new ActionListener {
        def actionPerformed(e: ActionEvent) {
          val value = textFieldShortRange.getText().toInt
          if (value >= jsliderShortRange.getMinimum() && value <= jsliderShortRange.getMaximum()){
            Flocking.shortRange = value
            jsliderShortRange.setValue(value)
          }
        }
      }
    )  
      
    textFieldShortRangeRate.addActionListener(
      new ActionListener {
        def actionPerformed(e: ActionEvent) {
          val value = textFieldShortRangeRate.getText().toInt
          if (value >= jsliderShortRangeRate.getMinimum() && value <= jsliderShortRangeRate.getMaximum()){
            Flocking.coeffShort = value / -10
            jsliderShortRangeRate.setValue(value)
          }
        }
      }
    )  
      
    textFieldLongRange.addActionListener(
      new ActionListener {
        def actionPerformed(e: ActionEvent) {
          val value = textFieldLongRange.getText().toInt
          println(value)
          if (value >= jsliderLongRange.getMinimum() && value <= jsliderLongRange.getMaximum()){
            Flocking.longRange = value
            jsliderLongRange.setValue(value)
          }
        }
      }
    )
  
    textFieldLongRangeRate.addActionListener(
      new ActionListener {
        def actionPerformed(e: ActionEvent) {
          val value = textFieldLongRangeRate.getText().toInt
          if (value >= jsliderLongRangeRate.getMinimum() && value <= jsliderLongRangeRate.getMaximum()){
            Flocking.coeffLong = value / 100.0
            jsliderLongRangeRate.setValue(value)
          }
        }
      }
    )
      
    //Miscellaneous 
    jsliderMovingForward.addChangeListener(
      new ChangeListener {
        def stateChanged(e: ChangeEvent) {
          Flocking.coefMovingForward = e.getSource().asInstanceOf[JSlider].getValue().asInstanceOf[Double] / 10.0
          textFieldMovingForward.setValue(e.getSource().asInstanceOf[JSlider].getValue().asInstanceOf[Double])
        }
      }
    )
      
      
    jsliderCoefficientTurn.addChangeListener(
      new ChangeListener {
        def stateChanged(e: ChangeEvent) {
          Flocking.coeffTurn = e.getSource().asInstanceOf[JSlider].getValue().asInstanceOf[Double] / 100.0
          textFieldCoefficientTurn.setValue(e.getSource().asInstanceOf[JSlider].getValue().asInstanceOf[Double])
        }
      }
    )
      
    jsliderSpeed.addChangeListener(
      new ChangeListener {
        def stateChanged(e: ChangeEvent) {
          Flocking.minSpeed = 200 - e.getSource().asInstanceOf[JSlider].getValue().asInstanceOf[Long]
          textFieldSpeed.setValue(e.getSource().asInstanceOf[JSlider].getValue().asInstanceOf[Long])
        }
      }
    )
      
    textFieldMovingForward.addActionListener(
      new ActionListener {
        def actionPerformed(e: ActionEvent) {
          val value = textFieldMovingForward.getText().toInt
          if (value >= jsliderMovingForward.getMinimum() && value <= jsliderMovingForward.getMaximum()){
            Flocking.coefMovingForward = value / 10.0
            jsliderMovingForward.setValue(value)
          }
        }
      }
    )
      
    textFieldCoefficientTurn.addActionListener(
      new ActionListener {
        def actionPerformed(e: ActionEvent) {
          val value = textFieldCoefficientTurn.getText().toInt
          if (value >= jsliderCoefficientTurn.getMinimum() && value <= jsliderCoefficientTurn.getMaximum()){
            Flocking.coeffTurn = value / 100.0
            jsliderCoefficientTurn.setValue(value)
          }
        }
      }
    )
      
    textFieldSpeed.addActionListener(
      new ActionListener {
        def actionPerformed(e: ActionEvent) {
          val value = textFieldSpeed.getText().toInt
          if (value >= jsliderSpeed.getMinimum() && value <= jsliderSpeed.getMaximum()){
            Flocking.minSpeed = 200 - value
            jsliderSpeed.setValue(value)
          }
        }
      }
    )
      
      
    groupModeBox.addActionListener(
      new ActionListener {
        def actionPerformed(e: ActionEvent){
          Flocking.groupMode = e.getSource().asInstanceOf[JCheckBox].isSelected()    
        } 
      }
    )
      
    wallsBox.addActionListener(
      new ActionListener {
        def actionPerformed(e: ActionEvent){
          Flocking.walls = e.getSource().asInstanceOf[JCheckBox].isSelected()
        }
      }
    )
      
    //Quadtree
    useQuadtreeBox.addActionListener(
      new ActionListener {
        def actionPerformed(e: ActionEvent) {
          Flocking.useQuadtree = e.getSource().asInstanceOf[JCheckBox].isSelected()
        }
      }
    )
    
    quadtreeVisibleBox.addActionListener(
      new ActionListener {
        def actionPerformed(e: ActionEvent) {
          Flocking.drawQuadTree = e.getSource().asInstanceOf[JCheckBox].isSelected()
        }
      }
    )
      
    jsliderQuadTreeMinSize.addChangeListener(
      new ChangeListener {
        def stateChanged(e: ChangeEvent) {
          Flocking.minSizeQuadTree = e.getSource().asInstanceOf[JSlider].getValue()
          textFieldQuadTreeMinSize.setValue(e.getSource().asInstanceOf[JSlider].getValue())
        }
      }
    )
       
    textFieldQuadTreeMinSize.addActionListener(
      new ActionListener {
        def actionPerformed(e: ActionEvent) {
          val value = textFieldQuadTreeMinSize.getText().toInt
          if (value >= jsliderQuadTreeMinSize.getMinimum() && value <= jsliderQuadTreeMinSize.getMaximum()){
            Flocking.minSizeQuadTree = value
            jsliderNumberBirds.setValue(value)
          }
        }
      }
    )
      
    //Now we add the buttons to the graphic interface
    jpanelButtons.setSize(sizeButtons, heightPanel)
    jpanelButtons.setLayout(new GridLayout(0, 1))
    
    //Buttons
    //Execution control
    val jPanelExecution = new JPanel
    jPanelExecution.add(playBox)
    jPanelExecution.add(nextStepButton)
    jPanelExecution.add(restartButton)
      
    //Information
    val jPanelInformation = new JPanel
    jPanelInformation.add(info)
      
    //Initial values
    val jPanelPar = new JPanel
    jPanelPar.setLayout(new GridLayout(0, 1))
    val jPanelParMode = new JPanel
    val parrallelsComboBoxes = new ButtonGroup
    parrallelsComboBoxes.add(parRadio)
    parrallelsComboBoxes.add(toParRadio)
    parrallelsComboBoxes.add(notparRadio)
    jPanelParMode.add(notparRadio)
    jPanelParMode.add(parRadio)
    jPanelParMode.add(toParRadio)
      
    val JPanelNumberOfProcessors = new JPanel
    JPanelNumberOfProcessors.add(new JLabel("Number of processors used : "))
    JPanelNumberOfProcessors.add(numberOfProcessorsComboBox)
      
    jPanelPar.add(jPanelParMode)
    jPanelPar.add(JPanelNumberOfProcessors)
      
    val jPanelNumberBirds = new JPanel
    jPanelNumberBirds.add(new JLabel("Number of birds"))
    jPanelNumberBirds.add(jsliderNumberBirds)
    jPanelNumberBirds.add(textFieldNumberBirds)
      
    //Range
    val jPanelRange = new JPanel
    jPanelRange.setLayout(new GridLayout(3, 5))
    jPanelRange.add(new JLabel(""))
    jPanelRange.add(new JLabel(""))
    jPanelRange.add(new JLabel("Range"))
    jPanelRange.add(new JLabel(""))
    jPanelRange.add(new JLabel("Coefficient"))
    jPanelRange.add(new JLabel("Short range"))
    jPanelRange.add(textFieldShortRange)
    jPanelRange.add(jsliderShortRange)
    jsliderShortRange.setPreferredSize(new Dimension(10,70))
    jPanelRange.add(textFieldShortRangeRate)
    jPanelRange.add(jsliderShortRangeRate)
    jPanelRange.add(new JLabel("Long range"))
    jPanelRange.add(textFieldLongRange)
    jPanelRange.add(jsliderLongRange)
    jPanelRange.add(textFieldLongRangeRate)
    jPanelRange.add(jsliderLongRangeRate)
      
    //Miscellaneous
    val jPanelMovingForward = new JPanel
    jPanelMovingForward.add(new JLabel("Coefficient of moving foward"))
    jPanelMovingForward.add(jsliderMovingForward)
    jPanelMovingForward.add(textFieldMovingForward)
      
    val jPanelCoeffTurn = new JPanel
    jPanelCoeffTurn.add(new JLabel("Coefficient of turning"))
    jPanelCoeffTurn.add(jsliderCoefficientTurn)
    jPanelCoeffTurn.add(textFieldCoefficientTurn)
      
    val jPanelSpeed = new JPanel
    jPanelSpeed.add(new JLabel("Maximum Speed"))
    jPanelSpeed.add(jsliderSpeed)
    jPanelSpeed.add(textFieldSpeed)
      
    val jPanelMode = new JPanel
    jPanelMode.add(groupModeBox)
    jPanelMode.add(wallsBox)
      
    //Quadtree
    val jPanelQuadtree = new JPanel
    jPanelQuadtree.add(new JLabel("Quadtree : "))
    jPanelQuadtree.add(useQuadtreeBox)
    jPanelQuadtree.add(quadtreeVisibleBox)
    jPanelQuadtree.add(new JLabel(" MinSize"))
    jPanelQuadtree.add(jsliderQuadTreeMinSize)
    jPanelQuadtree.add(textFieldQuadTreeMinSize)  
      

    jpanelButtons.add(jPanelExecution)
    jpanelButtons.add(jPanelInformation)
    jpanelButtons.add(jPanelPar)
    jpanelButtons.add(jPanelNumberBirds)
    jpanelButtons.add(jPanelRange)
    jpanelButtons.add(jPanelMovingForward)
    jpanelButtons.add(jPanelCoeffTurn)
    jpanelButtons.add(jPanelSpeed)
    jpanelButtons.add(jPanelMode)
    jpanelButtons.add(jPanelQuadtree)
      
    jFrame.add(jpanelButtons, BorderLayout.EAST)
      
    jFrame.add(Graphics, BorderLayout.CENTER)
    this.setBackground(Color.WHITE)
    jFrame.repaint()
    jFrame.setVisible(true)
  }
  
  import java.awt.Graphics
  //Draw the polygon representing the birds
  def drawPolygons(g: Graphics, bird: Bird): Bird = {
    g.drawPolygon(bird.polygon)
    bird
  }
  
  //Redraw the birds
  def updatePolygons(g: Graphics, bird: Bird): Bird = {
    val newBird: Bird = bird.getDeepCopy()
    val birdDrawed = new Polygon()

    //pointTop
    val xPointTop = (bird.x + Flocking.sizeLong * bird.alignmentX).toInt
    val yPointTop = (bird.y + Flocking.sizeLong * bird.alignmentY).toInt
    birdDrawed.addPoint(xPointTop, yPointTop)
    
    //pointLeft
    //We use rotations matrices
    val xPoint1 = (bird.x + Flocking.sizeBase * (bird.alignmentX * cosPI4 - bird.alignmentY * sinPI4)).toInt
    val yPoint1 = (bird.y + Flocking.sizeBase * (bird.alignmentX * sinPI4 + bird.alignmentY * cosPI4)).toInt
    birdDrawed.addPoint(xPoint1, yPoint1)

    //pointRight
    val xPoint2 = (bird.x + Flocking.sizeBase * (bird.alignmentX * cosMinusPI4 - bird.alignmentY * sinMinusPI4)).toInt
    val yPoint2 = (bird.y + Flocking.sizeBase * (bird.alignmentX * sinMinusPI4 + bird.alignmentY * cosMinusPI4)).toInt
    birdDrawed.addPoint(xPoint2, yPoint2)
    
    newBird.polygon = birdDrawed
    newBird
  }
  
  def drawQuadtree(g: Graphics, quadtree: Quadtree){
    if (quadtree != null){
      g.drawRect(quadtree.x1, quadtree.y1, quadtree.x2 - quadtree.x1, quadtree.y2 - quadtree.y1)
      drawQuadtree(g, quadtree.NE)
      drawQuadtree(g, quadtree.NW)
      drawQuadtree(g, quadtree.SE)
      drawQuadtree(g, quadtree.SW)
    }
  }
  
  override def paintComponent(g: Graphics) {
    super.paintComponent(g)

    //We erase old triangles
    g.setColor(backGroundColor)
    g.fillRect(0,0, heightPanel, widthPanel)
    implicit val sch = Flocking.schedulerForToPar

    //we update the triangles
    if (Flocking.useToPar)
      arrayBirds = arrayBirds.toArray.toPar.map(updatePolygons(g, _)).seq
    else
      arrayBirds = arrayBirds.map(updatePolygons(g, _))
    
    //we redraw the triangles
    g.setColor(Color.RED);
    arrayBirds.seq.map(drawPolygons(g, _))
    
    if (Flocking.useQuadtree && Flocking.drawQuadTree){
      g.setColor(Color.BLACK)
      drawQuadtree(g,quadtree)
      g.setColor(Color.RED)
    }
    
  }
}

object Flocking {
  //Parameters
  var shortRange = 10
  var longRange = 80
  var coeffShort = -6.0
  var coeffLong = 0.2
  var coefMovingForward = 1.5
  var groupMode = false //The birds  stay in the group (true) or just travel around the area (false)
  var walls = false // Does the window have walls 
  var coeffTurn = 0.6 //If softTurn=true, what coefficient does the bird turns
  var useQuadtree = true //Use or not quadtree data structures for our birds
  var drawQuadTree = false
  var minSizeQuadTree = 1
  var numberOfBirds = 1600
    
  var usePar = false
  var useToPar = false
  var numberOfProcessorsUsed = Runtime.getRuntime.availableProcessors
  var minSpeed: Long = 1 // The minimum value for a calculation
    
  var numberOfBirdsChanged = true
  var parChanged = true
    
  var sizeBase = 3
  var sizeLong = 13
    
    
    
  //Variables to measure time
  var play = false
  var timeSincePlay: Long = 0
  var justOnePlay = true
  var timeSpentSinceBegining: Long =  0
  var iterations = 0  
  var startLoop: Long = 0 
  var iterationStartTime: Long = 0
  var iterationEndTime: Long = 0
  var timeSpent: Long = 0
    
    
  //Environment utilitaries
  val randomNumbers = new Random(System.currentTimeMillis())
  var schedulerForToPar: Scheduler = _
  var tasksupportForPar: TaskSupport = _
  var quadtree: Quadtree = null
  var arrayBirds: GenSeq[Bird] = null
    
  var dumb = false
  
  //Tell us if bird1 and bird2 are separated by less than distance
  def isInThatDistance(bird1: Bird, bird2: Bird, distance: Int): Boolean =
     math.sqrt(math.pow(math.abs(bird1.x.toDouble - bird2.x.toDouble), 2.0) + math.pow(math.abs(bird1.y.toDouble - bird2.y.toDouble), 2.0) ) < distance
  
  //Moves a bird to a position with a speed depending on the coefficient and how far the bird is of this point
  def moveThatBirdTo(positionX: Int, positionY: Int, coefficient: Double, bird: Bird) = {
    if (positionX > bird.x)
      bird.x += (coefficient * (positionX - bird.x) * bird.x / (positionX + 1.0)).toInt
    else if (positionX < bird.x)
      bird.x += (coefficient * (positionX - bird.x) * positionX / (bird.x + 1.0)).toInt
    if (positionY > bird.y)
      bird.y += (coefficient * (positionY - bird.y) * bird.y / (positionY + 1.0)).toInt
    else if (positionY < bird.y)
      bird.y += (coefficient * (positionY - bird.y) * positionY / (bird.y + 1.0)).toInt
  }
  
  def updateBorders(bird: Bird): Bird = {
    
    //We determine if the bird too much on one side
    val tooMuchLeft = bird.x <= Flocking.sizeLong
    val tooMuchRight = bird.x >= Graphics.heightPanel - Flocking.sizeLong
    val tooMuchTop = bird.y <= Flocking.sizeLong
    val tooMuchDown = bird.y >= Graphics.widthPanel - Flocking.sizeLong
  
    if (tooMuchTop){
      if (walls) {
        bird.alignmentY *= -1
      bird.y = Flocking.sizeLong + 1 + (Flocking.sizeLong - bird.y)
      }
      else {
        bird.y = Graphics.widthPanel-Flocking.sizeLong - 1 - (Flocking.sizeLong - bird.y)
      }
    }
    else if (tooMuchDown){
      if (walls) {
        bird.alignmentY *= -1
        bird.y = Graphics.widthPanel - Flocking.sizeLong - 1 - (bird.y - (Graphics.widthPanel - Flocking.sizeLong))
      }
      else
        bird.y = Flocking.sizeLong + 1 + (bird.y - (Graphics.widthPanel - Flocking.sizeLong))
    }
    else if (tooMuchLeft){
      if (walls) {
        bird.alignmentX *= -1
        bird.x = Flocking.sizeLong + 1 + (Flocking.sizeLong - bird.x)
      }
      else
        bird.x = Graphics.heightPanel - Flocking.sizeLong - 1  - (Flocking.sizeLong - bird.x)
    }
    else if (tooMuchRight){
      if (walls){
        bird.alignmentX *= -1
        bird.x = Graphics.heightPanel - Flocking.sizeLong - 1 - (bird.x - (Graphics.heightPanel - Flocking.sizeLong))
      }
      else
        bird.x = Flocking.sizeLong + 1 + (bird.x - (Graphics.heightPanel - Flocking.sizeLong))
      }
    bird
  }
  
  def updateOrientation(bird: Bird, averageAlignmentX: Double,  averageAlignmentY: Double): Bird = {
    def oldAlignmentX = bird.alignmentX
    def oldAlignmentY = bird.alignmentY
    
    bird.alignmentX = bird.alignmentX - (bird.alignmentX - averageAlignmentX) * coeffTurn
    bird.alignmentY = bird.alignmentY - (bird.alignmentY - averageAlignmentY) * coeffTurn
    
    //We normalise the vector
    def norm = Math.sqrt((bird.alignmentX * bird.alignmentX) + (bird.alignmentY * bird.alignmentY))
    bird.alignmentX /= norm
    bird.alignmentY /= norm
    bird
  }
  
  def randomOneOrMinusOne(random: Random): Double = {
    if (random.nextBoolean)
      1.0
    else
      -1.0
  }
  
  def cycleForOneBird(birds: GenSeq[Bird], quadtree: Quadtree, bird:Bird)(implicit ctx: Scheduler): Bird = {
    val newBird = bird.getDeepCopy()
    
    var sumOfXShort = 0
    var sumOfYShort = 0
    var numberOfShort = 0
    var sumOfXLong = 0
    var sumOfYLong = 0
    var numberOfLong = 0
    var i = 0
    
    var sumXAngles = 0.0
    var sumYAngles = 0.0
    val oldX = newBird.x
    val oldY = newBird.y
    
    var interestingBirds = birds
    if (useQuadtree && quadtree != null)
      interestingBirds = quadtree.getBirdsIn(bird.x - longRange - 1, bird.y - longRange - 1, bird.x + longRange + 1, bird.y + longRange + 1).seq
    
    for (currentBirdOnList <- interestingBirds.seq){
      // We first test if the bird is in short range
      if (isInThatDistance(currentBirdOnList, newBird, shortRange)) {
        sumOfXShort += currentBirdOnList.x
        sumOfYShort += currentBirdOnList.y
        sumXAngles += currentBirdOnList.alignmentX
        sumYAngles += currentBirdOnList.alignmentY
        numberOfShort += 1
      }
      //Then we test for the long range
      else if (isInThatDistance(currentBirdOnList, newBird, longRange)) {
        sumOfXLong += currentBirdOnList.x
      sumOfYLong += currentBirdOnList.y
      numberOfLong += 1
    }
      
    //We avoid multiple birds at the same place
    if (currentBirdOnList.x == newBird.x && currentBirdOnList.y == newBird.y){
      val randomValue = Flocking.randomNumbers.nextInt % 4
      if (randomValue == 0)
        newBird.y += 1
      else if (randomValue == 1)
        newBird.y -= 1
      else if (randomValue == 2)
        newBird.x += 1
      else
        newBird.x -= 1
      }
    }
    
    //We first make the bird move to his normal direction
    val xPointTop = (newBird.x + (Flocking.sizeLong / 2) * newBird.alignmentX).toInt
    val yPointTop = (newBird.y + (Flocking.sizeLong / 2) * newBird.alignmentY).toInt
    moveThatBirdTo(xPointTop, yPointTop, coefMovingForward, newBird)   
    
    //We update the short range
    if (numberOfShort != 0 && numberOfLong != 0){ //Should never happen but happens sometimes when the input values change a lot
      val moyOfXShort = sumOfXShort / numberOfShort
      val moyOfYShort = sumOfYShort / numberOfShort
      val moyOfXLong = sumOfXLong / numberOfLong
      val moyOfYLong = sumOfYLong / numberOfLong
      if (groupMode){
        //If we are in groups mode, we orient the bird to the position of the short range, probably the position where the group is
        val differenceX = moyOfXShort - oldX
        val differenceY = moyOfYShort - oldY 
        updateOrientation(newBird, differenceY, differenceY)
      }
      moveThatBirdTo(moyOfXShort, moyOfYShort, coeffShort, newBird)
      moveThatBirdTo(moyOfXLong, moyOfYLong, coeffLong, newBird)
    
      /* We make it change his orientation moving towards the average orientation size of his neighbours,
      that we compute with the sums of sinuses and cosinuses */
      var averageAlignmentX, averageAlignmentY: Double = 0.0
      
      //We normalise it
      def norm = Math.sqrt((sumXAngles * sumXAngles) + (sumYAngles * sumYAngles))
      if (norm != 0.0)
        averageAlignmentX = sumXAngles / norm
      if (norm != 0.0)
        averageAlignmentY = sumYAngles / norm
      if (averageAlignmentX != 0.0 || averageAlignmentY != 0.0)
        updateOrientation(newBird, averageAlignmentX, averageAlignmentY)
    }
    //If it reach a border, we make it change his way
    return updateBorders(newBird)
  }

  def makePerformanceText(): String = { 
    "Last iteration took : " + (iterationEndTime - iterationStartTime) + "ms \n" + "Average since the last change of parallelism mode : " + {timeSpentSinceBegining/iterations} + "ms \n Time playing : " + (System.currentTimeMillis() - timeSincePlay).toDouble / 1000.0 + "s"
  }

  def launch()(implicit sch: Scheduler){
    if (useToPar){
      arrayBirds = ((arrayBirds.toArray.toPar).map(cycleForOneBird(arrayBirds, quadtree, _))).seq
    }
    else{
      arrayBirds = arrayBirds.map(cycleForOneBird(arrayBirds, quadtree, _))
      if (usePar && !parChanged)
        arrayBirds.asInstanceOf[ParSeq[Bird]].tasksupport = tasksupportForPar
    }
  }
  
  def main(args: Array[String]) {
    Graphics.init()

          
    while(true){
      if (!play){
        timeSincePlay = System.currentTimeMillis()
      }
      if (play || justOnePlay){
        //If the initial values changed
        if (numberOfBirdsChanged){
          var i = 0
        arrayBirds = Array[Bird]()
        numberOfBirdsChanged = false
        parChanged = true
        while (i < numberOfBirds){  
          val randomNumbersForBirds = new Random(i * 57)
          arrayBirds = arrayBirds :+ new Bird((sizeLong + randomNumbersForBirds.nextDouble * (Graphics.heightPanel - 2 * sizeLong)).toInt, (sizeLong + randomNumbersForBirds.nextDouble *  (Graphics.widthPanel - 2 * sizeLong)).toInt, randomNumbersForBirds.nextDouble * randomOneOrMinusOne(randomNumbersForBirds) , randomNumbersForBirds.nextDouble * randomOneOrMinusOne(randomNumbersForBirds))
          i += 1
        }
        Graphics.arrayBirds = arrayBirds
        Graphics.repaint()
      }
          
      if (parChanged){
        parChanged = false
        if (usePar){
          tasksupportForPar = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(numberOfProcessorsUsed+1))
          arrayBirds = arrayBirds.par
          arrayBirds.asInstanceOf[ParSeq[Bird]].tasksupport = new collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(numberOfProcessorsUsed+1))
        }
        if (!usePar){
          arrayBirds = arrayBirds.seq
          if (useToPar)
            schedulerForToPar = new Scheduler.ForkJoin(new Scheduler.Config.Default(numberOfProcessorsUsed))
          }
          timeSpentSinceBegining = 0
          iterations = 0
          startLoop = 0
          timeSpent = 0
        }
        
        justOnePlay = false
        iterations = iterations + 1
        iterationStartTime = System.currentTimeMillis()

        launch()(schedulerForToPar)
        if (useQuadtree){
          quadtree = new Quadtree(arrayBirds.seq, 0, 0, Graphics.heightPanel,Graphics.widthPanel, minSizeQuadTree)
          Graphics.quadtree = quadtree
        }
        Graphics.arrayBirds = arrayBirds
        Graphics.repaint()
          
        iterationEndTime = System.currentTimeMillis()
        timeSpentSinceBegining += (iterationEndTime - iterationStartTime)
        println(s"iteration took ${iterationEndTime - iterationStartTime}, mean time ${timeSpentSinceBegining/iterations}")
        println("--------------------------")
        
        //Now the maximum speed control
        timeSpent = System.currentTimeMillis() - iterationStartTime
        if (timeSpent < minSpeed)
          Thread.sleep(minSpeed - timeSpent)
        Graphics.info.setText(makePerformanceText())
      }
      else
        Thread.sleep(10)
    } 
  }
}
