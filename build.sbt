name := "SparkSolutions"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "MavenRepository" at "https://mvnrepository.com/"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0"

// https://mvnrepository.com/artifact/org.apache.poi/poi
libraryDependencies += "org.apache.poi" % "poi" % "3.16"

//spark testing base
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.6.0" % "test"

libraryDependencies += "io.netty" % "netty" % "3.6.2.Final"



