name := "directoryWatcher"

version := "0.1"

scalaVersion := "2.12.3"

lazy val akkaVersion = "2.5.3"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.7.2",
  "org.apache.hadoop" % "hadoop-client" % "2.7.2",
  "org.apache.hadoop" % "hadoop-common" % "2.7.2"
)