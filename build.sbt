import sbt.Keys.organization

lazy val root = (project in file("."))
  .settings(
    name := "kaggle_covid_24",
    version := "1.0.0-00",
    scalaVersion := "2.11.12",
    mainClass in Compile := Some("com.github.marino_serna.kaggle_covid.KaggleCovidMain"),

    organization := "com.github.marino-serna",
    homepage := Some(url("https://github.com/marino-serna/KaggleCovid")),
    scmInfo := Some(ScmInfo(url("https://github.com/marino-serna/KaggleCovid"),
      "git@github.com:marino-serna/KaggleCovid.git")),
    developers := List(Developer("marino-serna",
      "Marino Serna",
      "marinosersan@gmail.com",
      url("https://github.com/marino-serna"))),
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
  )

val sparkVersion = "2.4.7"

// disable using the Scala version in output paths and artifacts
crossPaths := false

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.scalatest" %% "scalatest-funsuite" % "3.2.2" % Test,
  "org.scalatest" %% "scalatest-shouldmatchers" % "3.2.2" % Test,
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "2.6.3",
  "org.apache.spark" %% "spark-mllib" % "2.4.7" % "provided",
  "com.databricks" %% "dbutils-api" % "0.0.4",
  "commons-httpclient" % "commons-httpclient" % "3.1",
  "graphframes" % "graphframes" % "0.8.1-spark2.4-s_2.11"
//  "org.elasticsearch" %% "elasticsearch-spark-20" % "7.9.3"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

addArtifact(artifact in (Compile, assembly), assembly)

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"