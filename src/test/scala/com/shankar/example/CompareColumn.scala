package com.shankar.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by sakoirala on 6/20/17.
  */
object CompareColumn extends App{

  val spark = SparkSession.builder().master("local").getOrCreate()

  import spark.implicits._

  val df1 = spark.sparkContext.parallelize(Seq(
    ("id1", "id2"),
    ("id1","id3"),
    ("id2","id3")
  )).toDF("idA", "idB")

  val df2 = spark.sparkContext.parallelize(Seq(
    ("id1", "blue", "m"),
    ("id2", "red", "s"),
    ("id3", "blue", "s")
  )).toDF("id", "color", "size")

  val firstJoin = df1.join(df2, df1("idA") === df2("id"), "inner")
    .withColumnRenamed("color", "colorA")
    .withColumnRenamed("size", "sizeA")
    .withColumnRenamed("id", "idx")

  val secondJoin = firstJoin.join(df2, firstJoin("idB") === df2("id"), "inner")

  val check = udf((v1: String, v2:String ) => {
    if (v1.equalsIgnoreCase(v2)) 1 else 0
  })

  val result = secondJoin
    .withColumn("color", check(col("colorA"), col("color")))
    .withColumn("size", check(col("sizeA"), col("size")))

  val finalResult = result.select("idA", "idB", "color", "size")

}
