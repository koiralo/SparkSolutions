package com.shankar.scalaeg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by sakoirala on 5/22/17.
  */
object RowBetweenDate extends App{

  val spark =
    SparkSession.builder().master("local").appName("test").getOrCreate()

  import spark.implicits._
  /*
    val data = spark.sparkContext.parallelize(Seq(
      (1,  "person1", 4),
        (2,  "person4", 5),
        (3,  "person3", 7))
    ).toDF("id", "name", "age")

    data.withColumn("age", col("age") + 20).show()



    val date = Seq("01-01-2012", "01-01-2013", "01-01-2014")*/



  val data = spark.sparkContext.parallelize(Seq(
    (123/*, Array("10.100.1.25", "10.100.164.36"), "10.100.164.32"*/),
    (456/*, Array("10.100.1.25", "10.100.164.3"), "10.100.164.32"*/),
    (45/*, Array("10.100.1.25", "10.100.164.36"), "10.100.164.32"*/),
    (46/*, Array("10.100.1.25", "10.100.164.367"), "10.100.164.32"*/)
  )).toDF("current_time")


  data.select("*").where(data("*") > 45).show

  data.select("*").where(array_contains(data("destinationips"), "10.100.164.36")).show





}
