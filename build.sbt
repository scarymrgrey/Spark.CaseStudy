name := "SparkTest"

version := "1.0"

scalaVersion := "2.12.10"


libraryDependencies ++= List(
  "org.apache.spark" %% "spark-sql" % "2.4.5",
  "com.typesafe" % "config" % "1.4.0",
  "com.github.pureconfig" %% "pureconfig" % "0.12.3",
  "org.typelevel" %% "cats-core" % "2.1.1",
  "org.typelevel" %% "cats-effect" % "2.1.2",
  "com.holdenkarau" %% "spark-testing-base" % "2.4.5_0.14.0" % "test"
)