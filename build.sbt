name := "scala-play-sbt"

version := "1.0"

scalaVersion := "2.11.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.0",
  "org.apache.spark" %% "spark-streaming" % "1.2.0",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.2.0",
  "org.mortbay.jetty" % "servlet-api" % "3.0.20100224")
    