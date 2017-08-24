name := "fun-spark"

version in ThisBuild := "0.1"

scalaVersion in ThisBuild := "2.11.11"


libraryDependencies in ThisBuild ++= Seq(

  "org.apache.spark" % "spark-core_2.11" % "2.2.0" , // % "provided",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0" , // % "provided",
  "org.apache.spark" % "spark-streaming_2.11" % "2.2.0" % "provided",
  "org.jsoup" % "jsoup" % "1.10.3"
)

lazy val bbc_comedy = Project(
  id = "bbc_comedy",
  base = file("bbc_comedy")
)
lazy val root = Project(
  id = "root",
  base = file(".")
).aggregate(bbc_comedy)

  assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList("org", "apache", xs@_*) => MergeStrategy.last
  case "application.conf" => MergeStrategy.concat
  case "unwanted.txt" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
