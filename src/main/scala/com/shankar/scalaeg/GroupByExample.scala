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

  /*val data = spark.sparkContext.parallelize(
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

    spark.sql("desc data").show*/

/*  val df = spark.sparkContext.parallelize(Seq(
    ("1", "3", "unknown"),
    ("1", "unknown", "4"),
    ("2", "unknown", "3"),
    ("2", "2", "unknown")
  )).toDF("id", "group_a", "group_b")

  val groupAList = collect_list("group_a").as("group_a")
  val groupBList = collect_list("group_b").as("group_b")

  val grouped = df.groupBy("id").agg(groupAList, groupBList)

  grouped.show


  val removeUnknown = udf((list : Seq[String]) => {
    list.filter(value => !value.equalsIgnoreCase("unknown"))
  })

  grouped.printSchema()


  grouped.withColumn("group_a", removeUnknown($"group_a")).show
  //    withColumn("group_b", removeUnknown($"group_b")).show

  println(List("3", "unknown").filter(value => value.equalsIgnoreCase("unknown")))*/


 /* val data =  spark.sparkContext.parallelize(Seq(
    ("2017-05-21", 1),
  ("2017-05-21", 1),
  ("2017-05-22", 1),
  ("2017-05-22", 1),
  ("2017-05-23", 1),
  ("2017-05-23", 1),
  ("2017-05-23", 1),
  ("2017-05-23", 1))).toDF("time_window", "foo")



  data.na.drop()
//  data.select("*", date_format(data("time_window"),"yyyy-MM-dd hh:mm").alias("time_window"))

  data.withColumn("$time_window", date_format(data("time_window"),"yyyy-MM-dd hh:mm"))
    .groupBy("$time_window")
    .agg(sum("foo")).show*/

//  data.groupBy("date").agg(avg(when(col("foo") === "a", 1).otherwise(0))).show()

//  data.groupBy("date").agg(avg((col("foo") == "a").cast("integer")))



 val data =  spark.sparkContext.parallelize(Seq(
   (Array("1", "2","3"), 1),
 (Array("1", "2","3"), 1),
 (Array("1", "2","3"), 1))).toDF("value", "value1")

  val toInt = udf((value : Seq[String]) => value.map(_.toInt).sum.toString)

  val i = data.withColumn("value", toInt(col("value"))).show




}
