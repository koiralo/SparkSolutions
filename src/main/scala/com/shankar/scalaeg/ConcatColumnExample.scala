package com.shankar.scalaeg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by sakoirala on 5/26/17.
  */
object ConcatColumnExample extends App{
  val spark =
    SparkSession.builder().master("local").appName("test").getOrCreate()
  import spark.implicits._
  val data = spark.sparkContext.parallelize(
    Seq(
      ("qwertyuiop", 0, 0, 16102.0, 0)
    )).toDF("agentName","original_dt","parsed_dt","user","text")


  val result = data.withColumn("newCol", split(concat_ws(";",  data.schema.fieldNames.map(c=> col(c)):_*), ";"))
  result.show()


  data.withColumn("newCol",
    struct(data.columns.head, data.columns.tail: _*))
//    show(false)





}
