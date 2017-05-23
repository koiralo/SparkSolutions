package com.shankar.scalaeg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by sakoirala on 5/22/17.
  */
object ReadParquetFromHDFS extends App{

  val spark =
    SparkSession.builder().master("local").appName("test").getOrCreate()

  import spark.implicits._

  val data = spark.read.parquet("hdfs://localhost:9000/tmp/data/facts/weather-api/0")

  println("*****************" + data.count())

  data.show(100)

  data.select("*").where(data("name") === "UPLOAD-2").show()


}
