lazy val root = (project in file(".")).
  settings(
    name := "movielens",
    version := "1.0",
    scalaVersion := "2.10.6",
    mainClass in Compile := Some("org.aeb.uk.movielens.driver.Main")
  )

assemblyMergeStrategy in assembly := {
  case PathList("spark.properties") => MergeStrategy.discard
  case PathList("application.properties") => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

lazy val sparkVersion = "1.6.3"

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"

)