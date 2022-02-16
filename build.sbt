
lazy val extraMavenRepo = sys.env.get("EXTRA_MAVEN_REPO").toSeq.map { repo =>
    resolvers += "Delta" at repo
}

lazy val root = project
  .in(file("."))
  .settings(
    name := "scala-delta",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := "2.12.13",

    libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test",
    libraryDependencies += "io.delta" %% "delta-core" % "1.1.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0",
    extraMavenRepo
  )
