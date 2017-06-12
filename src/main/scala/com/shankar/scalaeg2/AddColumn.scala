package com.shankar.scalaeg2

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

/**
  * Created by sakoirala on 6/9/17.
  */
object AddColumn extends App {

  val spark = SparkSession.builder().master("local")
    .appName("test").getOrCreate()

  import spark.implicits._

  val data = spark.sparkContext.parallelize(Seq(
    (1,"1994-11-21 Xyz"),
    (2,"1994-11-21 00:00:00"),
    (3,"1994-11-21 00:00:00")
  )).toDF("id", "date")

  val check = udf((value: String) => {
    Try(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(value)) match {
      case Success(d) => 1
      case Failure(e) => 0
    }
  })

  println(data.withColumn("badData", check($"date")).rdd.collect()(1))

}
