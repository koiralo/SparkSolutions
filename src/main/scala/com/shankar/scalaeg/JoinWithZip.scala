package com.shankar.scalaeg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by sakoirala on 5/25/17.
  */
object JoinWithZip extends App{

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("ParquetAppendMode")
    .getOrCreate()

  import spark.implicits._


  val data1 = spark.read.json("/home/sakoirala/IdeaProjects/SparkSolutions/src/main/resources/explode.json")

  val result = data1.withColumn("path", explode($"path"))

//  result.select("id", "name", "age", "path.x", "path.y").show()


  val data = spark.sparkContext.parallelize(10000 to 10010).toDF("salary")

//  data.agg(max("salary").alias("max"), min($"salary").alias("min")).show()

  val df  = Seq(("foo", Seq(Some(2), Some(3))), ("bar", Seq(None))).toDF("k", "v")

  df.show()

  df.filter(array_contains($"v", None)).show()



}
