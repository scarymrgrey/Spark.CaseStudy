name := "SparkTest"

version := "1.0"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"
libraryDependencies += "com.typesafe" % "config" % "1.4.0"
libraryDependencies += "com.github.pureconfig" %% "pureconfig" % "0.12.3"
libraryDependencies += "org.typelevel" %% "cats-core" % "2.1.1"
libraryDependencies += "org.typelevel" %% "cats-effect" % "2.1.2"

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