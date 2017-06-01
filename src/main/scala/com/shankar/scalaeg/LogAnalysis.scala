package com.shankar.scalaeg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Created by sakoirala on 5/23/17.
  */
object LogAnalysis extends App {

  val spark =
    SparkSession.builder().master("local").appName("test").getOrCreate()

  import spark.implicits._

/*  val customSchema = StructType(
    Array(StructField("column0", StringType, true),
          StructField("column1", StringType, true),
          StructField("column2", StringType, true)))

  val data = spark.read
    .schema(schema = customSchema)
    .csv(
      "tempoutput.txt")

  data.show()

  data
    .withColumn("column0", split($"column0", " "))
    .withColumn("column1", split($"column2", " "))
    .withColumn("column2", split($"column2", " "))
    .select(
      $"column0".getItem(0).as("column0"),
      $"column1".getItem(3).as("column1"),
      $"column2".getItem(5).as("column2")
    )
    .show()*/
  val row_df = spark.sparkContext.parallelize(Seq(0.1, 0.3, 0.9 ,0.7, 0.6)).toDF("x")

  row_df.withColumn("x", (row_df("x") * 10).cast(IntegerType)).show()

}
