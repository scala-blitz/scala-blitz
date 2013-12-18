
import sbt._
import Keys._
import java.io.File



object BuildSettings {
  val buildSettings = Defaults.defaultSettings ++ Seq (
    name := "scala-blitz",
    organization := "com.github.scala-blitz",
    version := "1.0-SNAPSHOT",
    scalaVersion := "2.11.0-M7",
    scalacOptions ++= Seq("-deprecation", "-optimise"),
    resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % "2.11.0-M7"
      , "com.github.axel22" %% "scalameter" % "0.5-M1"
//      , "org.scalatest" %% "scalatest" % "2.0.1-SNAP4" % "test"
    ),
    testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework"),
    logBuffered := false,
//    addCompilerPlugin("org.scala-lang.plugins" % "macro-paradise" % "2.0.0-SNAPSHOT" cross CrossVersion.full),
    initialCommands in console := "import scala.collection.optimizer._"
  )
}


object WorkstealingBuild extends Build {
  
  def quote(s: Any) = {
    if (scala.util.Properties.isWin) "\"" + s.toString + "\""
    else s.toString
  }


  /* tasks and settings */
   
  val javaCommand = TaskKey[String](
    "java-command",
    "Creates a java vm command for launching a process."
  )
  
  val javaCommandSetting = javaCommand <<= (
    dependencyClasspath in Compile,
    artifactPath in (Compile, packageBin),
    artifactPath in (Test, packageBin),
    packageBin in Compile,
    packageBin in Test
  ) map {
    (dp, jar, testjar, pbc, pbt) => // -XX:+UseConcMarkSweepGC  -XX:-DoEscapeAnalysis -XX:MaxTenuringThreshold=12 -verbose:gc -XX:+PrintGCDetails 
    val sep = java.io.File.pathSeparator
    val javacommand = "java -Xmx4096m -Xms4096m -XX:+UseCondCardMark -server -cp %s%s%s%s%s".format(
      dp.map(x => quote(x.data)).mkString(sep),
      sep,
      quote(jar),
      sep,
      quote(testjar)
    )
    javacommand
  }
  
  val benchTask = InputKey[Unit](
    "bench",
    "Runs a specified benchmark."
  ) <<= inputTask {
    (argTask: TaskKey[Seq[String]]) =>
    (argTask, javaCommand) map {
      (args, jc) =>
      val javacommand = jc
      val comm = javacommand + " " + args.mkString(" ")
      println("Executing: " + comm)
      comm!
    }
  }
  
  val benchVerboseTask = InputKey[Unit](
    "vbench",
    "Runs a specified benchmark in a verbose mode."
  ) <<= inputTask {
    (argTask: TaskKey[Seq[String]]) =>
    (argTask, javaCommand) map {
      (args, jc) =>
      val javacommand = jc
      val verboseopts = "-XX:+UnlockDiagnosticVMOptions -XX:+PrintCompilation -XX:+PrintAssembly -XX:PrintAssemblyOptions=hsdis-print-bytes -Xbatch -verbose:gc -Xprof"
      val comm = javacommand + " " + verboseopts + " " + args.mkString(" ")
      println("Executing: " + comm)
      comm!
    }
  }
  /* projects */

  //lazy val scalameter = RootProject(uri("git://github.com/axel22/scalameter.git"))
  
  lazy val root = Project(
    "root",
    file("."),
    settings = BuildSettings.buildSettings ++ Seq(benchTask, javaCommandSetting, benchVerboseTask)
  ) dependsOn ()

}










