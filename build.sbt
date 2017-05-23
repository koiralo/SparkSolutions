name := "SparkSolutions"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "MavenRepository" at "https://mvnrepository.com/"

libraryDependencies ++= Seq(
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
  "io.netty" % "netty" % "3.6.2.Final"
)
