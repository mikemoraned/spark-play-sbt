name := "scala-play-sbt"

version := "1.0"

scalaVersion := "2.11.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.0",
  "org.apache.spark" %% "spark-sql" % "1.2.0",
  "org.apache.spark" %% "spark-streaming" % "1.2.0",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.2.0",
  "com.github.nscala-time" %% "nscala-time" % "1.6.0")

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-log4j12" % "1.7.5",
  "org.mortbay.jetty" % "servlet-api" % "3.0.20100224",
  "org.eclipse.jetty" % "jetty-project" % "9.2.6.v20141205" )

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test")