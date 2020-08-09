name := "scala-belegarbeit"

version := "0.1"

scalaVersion := "2.11.8"

resolvers ++= Seq(
  "Typesafe Repository" at "https://repo1.maven.org/maven2/"
)

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  // Webserver
  "com.typesafe.akka" %% "akka-actor" % "2.3.16",
  "com.typesafe.akka" %% "akka-http" % "10.1.5",
  "com.typesafe.akka" %% "akka-stream" % "2.5.31"
)