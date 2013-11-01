
import sbt._
import Keys._
import Process._
import java.io.File



object BuildSettings {
  val buildSettings = Defaults.defaultSettings ++ Seq (
    name := "workstealing",
    version := "0.1",
    scalaVersion := "2.10.2",
    scalacOptions ++= Seq("-deprecation", "-optimise"),
    resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % "2.10.2"
      , "org.scalatest" %% "scalatest" % "1.9.1" % "test"
      , "com.github.axel22" %% "scalameter" % "0.4"
    ),
    testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework"),
    logBuffered := false
  )
}


object WorkstealingBuild extends Build {
  
  def quote(s: Any) = {
    if (scala.util.Properties.isWin) "\"" + s.toString + "\""
    else s.toString
  }

  /* projects */

  //lazy val scalameter = RootProject(uri("git://github.com/axel22/scalameter.git"))
  
  lazy val root = Project(
    "root",
    file("."),
    settings = BuildSettings.buildSettings
  ) dependsOn ()

}










