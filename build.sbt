name := "SparkTest"

version := "1.0"

scalaVersion := "2.12.10"

libraryDependencies ++= List(
  "org.apache.spark" %% "spark-sql" % "2.4.5",
  "com.typesafe" % "config" % "1.4.0",
  "com.github.pureconfig" %% "pureconfig" % "0.12.3",
  "org.typelevel" %% "cats-core" % "2.1.1",
  "org.typelevel" %% "cats-effect" % "2.1.2",
  "com.holdenkarau" %% "spark-testing-base" % "2.4.5_0.14.0" % "test",
  "org.scalactic" %% "scalactic" % "3.1.1",
  "org.scalatest" %% "scalatest" % "3.1.1" % "test"
)

mainClass in assembly := Some("com.task.MarketingAnalysisDriver")

assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*)       => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*)        => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*)         => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*)     => MergeStrategy.last
  case PathList("org", "apache", xs @ _*)           => MergeStrategy.last
  case PathList("com", "google", xs @ _*)           => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*)         => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*)           => MergeStrategy.last
  case "about.html"                 => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA"      => MergeStrategy.last
  case "META-INF/mailcap"           => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties"          => MergeStrategy.last
  case "git.properties"             => MergeStrategy.last
  case "log4j.properties"           => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}