package com.shankar.scalaeg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by sakoirala on 5/22/17.
  */
object GroupByExample extends App {

  val spark =
    SparkSession.builder().master("local").appName("test").getOrCreate()

  import spark.implicits._

  val data = spark.sparkContext.parallelize(
    Seq(
      (10, "Bob", "Manager", ""),
      (9, "Joe", "", "HQ"),
      (8, "Tim", "", "New York Office"),
      (7, "Joe", "", "New York Office"),
      (6, "Joe", "Head Programmer", ""),
      (5, "Bob", "", "LA Office"),
      (4, "Tim", "Manager", "HQ"),
      (3, "Bob", "", "New York Office"),
      (2, "Bob", "DB Administrator", "HQ"),
      (1, "Joe", "Programmer", "HQ")
    )).toDF("event_id", "name", "job", "location")

  val latest = data.groupBy("name").agg(max(data("event_id")).alias("event_id"))

//  latest.join(data, "event_id").drop("event_id").show


  data.createOrReplaceTempView("data")

    spark.sql("desc data").show

}
