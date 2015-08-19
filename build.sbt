name := "FlightDelay"

version := "0.1"

scalaVersion := "2.11.6"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.4.1"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.4.1"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.4.0"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
