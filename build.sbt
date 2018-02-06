import Dependencies._

lazy val commonSettings = Seq(
  version := "0.1.0",
  scalaVersion := "2.10.6",
  scalaBinaryVersion := "2.10"
)

lazy val root = (project in file("."))
  .settings(
    name := "movielens",
    commonSettings,
    mainClass in Compile := Some("org.aeb.uk.movielens.Driver"),
    libraryDependencies ++= commonDependencies
  )

assemblyMergeStrategy in assembly := {
  case PathList("spark.properties") => MergeStrategy.discard
  case PathList("application.properties") => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
