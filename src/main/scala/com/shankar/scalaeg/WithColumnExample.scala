package com.shankar.scalaeg

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

/**
  * Created by sakoirala on 5/17/17.
  */
object WithColumnExample  extends App{

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("")
    .getOrCreate()

  import spark.implicits._

//Create dfList dataframe
  val dfList = spark.sparkContext
    .parallelize(Seq("19931001","19930404", "19930603", "19930805")).toDF("DATE")


//  dfList.withColumn("DATE", dateToTimeStamp($"DATE")).show()

  val dateToTimeStamp = udf((date: String) => {
    val stringDate = date.substring(0,4)+"/"+date.substring(4,6)+"/"+date.substring(6,8)
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.format(new SimpleDateFormat("yyy/MM/dd").parse(stringDate))
  })


}
