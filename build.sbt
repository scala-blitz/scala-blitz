

publishMavenStyle := true

publishTo <<= version { (v: String) =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := true

pomIncludeRepository := { _ => false }

pomExtra := (
  <url>http://scala-blitz.github.com</url>
  <licenses>
    <license>
      <name>Apache 2</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:scala-blitz/scala-blitz.git</url>
    <connection>scm:git:git@github.com:scala-blitz/scala-blitz.git</connection>
  </scm>
  <developers>
    <developer>
      <id>axel22</id>
      <name>Aleksandar Prokopec</name>
      <url>http://axel22.github.com/</url>
    </developer>
    <developer>
      <id>DarkDimius</id>
      <name>Dmitry Petrashko</name>
      <url>http://people.epfl.ch/dmitry.petrashko</url>
    </developer>
  </developers>)


useGpg := true

usePgpKeyHex("23079096")
