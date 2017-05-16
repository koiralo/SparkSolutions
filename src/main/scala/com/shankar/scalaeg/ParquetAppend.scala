package com.shankar.scalaeg

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by sakoirala on 5/16/17.
  */
object ParquetAppend extends App{

  val spark = SparkSession.builder().master("local").appName("ParquetAppendMode").getOrCreate()

  val outputDir = "/home/sakoirala/IdeaProjects/SparkSolutions/src/main/resources/parquetExample/"

  import spark.implicits._

  spark.read.parquet(outputDir).show()

  val data = spark.sparkContext.parallelize(Seq(
    (1, "ram", "Kathmandu"),
    (2, "shyam", "Dharan"),
    (3, "hari", "Biratnagar"),
    (4, "Sita", "Pokhara")
  ))
    .toDF("id", "name", "address")

  val updatedData = data.withColumn("timestamp", lit(unix_timestamp) )

  updatedData.show()


  updatedData.write.mode(SaveMode.Append)
    .parquet(outputDir)

/*source : http://stackoverflow.com/questions/43332479/add-data-to-spark-parquet-data-stored-on-disk?noredirect=1&lq=1*/

}
