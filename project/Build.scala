
import sbt._
import Keys._
import Process._
import java.io.File



object WorkstealingBuild extends Build {
  
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
    (dp, jar, testjar, pbc, pbt) => // -XX:+UseConcMarkSweepGC  -XX:-DoEscapeAnalysis -XX:MaxTenuringThreshold=12
    val javacommand = "java -Xmx2048m -Xms2048m -XX:+UseCondCardMark -verbose:gc -XX:+PrintGCDetails -server -cp %s:%s:%s".format(
      dp.map(_.data).mkString(":"),
      jar,
      testjar
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
  
  lazy val storm = Project(
    "workstealing",
    file("."),
    settings = Defaults.defaultSettings ++ Seq(benchTask, javaCommandSetting, benchVerboseTask)
  ) dependsOn (
  )
  
}


