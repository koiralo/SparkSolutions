package com.shankar.scalaeg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by sakoirala on 5/30/17.
  */
object ExplodeRDD extends App {

  val spark =
    SparkSession.builder().master("local").appName("test").getOrCreate()

  import spark.implicits._

  val data = spark.sparkContext
    .parallelize(
      Seq(
        ("079562193",
         "EA",
         "0810",
         "STORE",
         Array("SELLABLE", "HELD"),
         Array("2015-10-09T00:55:23.6345Z", "2015-10-09T00:55:23.6345Z"))
      ))

/*  val result = data
    .map(row => (row._1, row._2, row._3, row._4, (row._5.zip(row._6).toMap)))
    .map(r => {
      r._5.map(v => (r._1, r._2, r._3, r._4, v._1, v._2))
    })
    .collect()
    .foreach(println)*/

  //create Dataframe 1
val df1 = spark.sparkContext.parallelize(Seq(
  ("2016-01-01", 1, "abcd", "F"),
  ("2016-01-01", 2, "efg", "F"),
  ("2016-01-01", 3, "hij", "F"),
  ("2016-01-01", 4, "klm", "F")
)).toDF("date","id","value", "label")

  //Create Dataframe 2
  val df2 = spark.sparkContext.parallelize(Seq(
    ("2016-01-01", 1, "abcd"),
    ("2016-01-01", 3, "hij")
  )).toDF("date1","id1","value1")

  val condition = $"date" === $"date1" && $"id" === $"id1" && $"value" === $"value1"

  //Join two dataframe with above condition
  val result = df1.join(df2, condition, "left")

//  check wather both fields contain same value and drop columns
  val finalResult = result.withColumn("label", condition)
    .drop("date1","id1","value1")
//Update column label from true false to T or F
  finalResult.withColumn("label", when(col("label") === true, "T").otherwise("F")).show


  case class TimeValue(value:Int, time: Int)
  case class RangeValue(value:Int, from: Int, to: Int)

/*  val timeValueDS = List(TimeValue(1,3),TimeValue(2,7),TimeValue(3,10),TimeValue(4,13)).toDS()
  val w = org.apache.spark.sql.expressions.Window.orderBy("time")
  val rangeValueDS = timeValueDS.withColumn("from",lag("time",1,0).over(w))
    .withColumnRenamed("time","to").as[RangeValue].show*/


}
