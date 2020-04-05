name := "SparkTest"

version := "1.0"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"
libraryDependencies += "com.typesafe" % "config" % "1.4.0"
libraryDependencies += "com.github.pureconfig" %% "pureconfig" % "0.12.3"
libraryDependencies += "org.typelevel" %% "cats-core" % "2.1.1"
libraryDependencies += "org.typelevel" %% "cats-effect" % "2.1.2"