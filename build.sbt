name := "SparkSolutions"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "MavenRepository" at "https://mvnrepository.com/"

libraryDependencies ++= Seq(
/*  "org.apache.spark" % "spark-streaming_2.10" % "2.1.0" % "provided",
  "org.apache.kafka" % "kafka_2.10" % "0.8.2.0",*/
  "com.googlecode.json-simple" % "json-simple" % "1.1.1",
// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib_2.10
  "org.apache.spark" % "spark-mllib_2.11" % "2.1.0",
  // https://mvnrepository.com/artifact/joda-time/joda-time
  "joda-time" % "joda-time" % "2.8.1",
// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11
  "org.apache.spark" % "spark-core_2.11" % "2.1.0",
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
  "org.apache.spark" % "spark-sql_2.11" % "2.1.0",
// https://mvnrepository.com/artifact/org.apache.poi/poi
  "org.apache.poi" % "poi" % "3.16",
// https://mvnrepository.com/artifact/org.apache.poi/poi-ooxml
  "org.apache.poi" % "poi-ooxml" % "3.16",
  "org.apache.poi" % "poi-ooxml-schemas" % "3.16",
//spark testing base
  "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.6.0" % "test",
  "io.netty" % "netty" % "3.6.2.Final",
  // https://mvnrepository.com/artifact/com.crealytics/spark-excel_2.11
  "com.crealytics" % "spark-excel_2.11" % "0.8.3"
)
