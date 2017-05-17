package com.shankar.scalaeg

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._


/**
  * Created by sakoirala on 5/16/17.
  */
object ParquetAppend extends App{

  val spark = SparkSession.builder().master("local")
      .config("spark.sql.parquet.cacheMetadata", false)
    .appName("ParquetAppendMode").getOrCreate()

  val outputDir = "/home/sakoirala/IdeaProjects/SparkSolutions/src/main/resources/parquetExample"

  import spark.implicits._

  val file = new File(outputDir)

  val data = spark.sparkContext.parallelize(Seq(
    (1, "ram", "Kathmandu"),
    (2, "shyam", "Dharan"),
    (3, "hari", "Biratnagar"),
    (4, "Sita", "Pokhara")
  ))
    .toDF("id", "name", "address")

  val updatedData = data.withColumn("timestamp", lit(unix_timestamp) )


    if (file.listFiles().length > 0){
      println("previous Data ")
      val previousData = spark.read.parquet(outputDir)
      previousData.show(100)
      previousData.union(updatedData).write.mode(SaveMode.Overwrite)
        .parquet(outputDir + "_temp")

      FileUtils.deleteDirectory(new File(outputDir))
      FileUtils.moveDirectory(new File(outputDir + "_temp"), new File(outputDir))
    }
  else {
    println(" no previous Data ")
    updatedData.write.mode(SaveMode.Overwrite)
      .parquet(outputDir)
    updatedData.show()
  }


/*source : http://stackoverflow.com/questions/43332479/add-data-to-spark-parquet-data-stored-on-disk?noredirect=1&lq=1*/

}
