name := "spark-streaming-demo"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.1"

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.3.0" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.0",
  "org.apache.kafka" %% "kafka" % "0.8.2.1"
)

import sbtassembly.MergeStrategy
// Fix the duplicate problem
assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs@_*) => MergeStrategy.last
  case PathList("com", "google", xs@_*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
    