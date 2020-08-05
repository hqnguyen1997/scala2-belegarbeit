name := "scala-belegarbeit"

version := "0.1"

scalaVersion := "2.11.8"

resolvers ++= Seq(
  "Typesafe Repository" at "https://repo1.maven.org/maven2/"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"