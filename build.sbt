name := "SparkScalaCourse"

version := "0.1"

scalaVersion := "2.11.0"

val sparkVersion = "2.0.1"

val twitterVersion = "4.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  "org.apache.spark" %% "spark-mllib" % sparkVersion,

  "org.apache.spark" %% "spark-streaming" % sparkVersion,

  "org.apache.bahir" %% "spark-streaming-twitter" % sparkVersion,
  "org.twitter4j" % "twitter4j-core" % twitterVersion,
  "org.twitter4j" % "twitter4j-stream" % twitterVersion,

  "commons-logging" % "commons-logging-api" % "1.1"

)