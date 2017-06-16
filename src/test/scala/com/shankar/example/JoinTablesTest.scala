package com.shankar.example

import java.sql.{Date, Timestamp}

import org.joda.time.{DateTime, DateTimeZone, Months}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.joda.time.format.DateTimeFormat
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
  * Created by sakoirala on 6/16/17.
  */
class JoinTablesTest extends FunSuite with BeforeAndAfterEach{

  val spark = SparkSession.builder().master("local").getOrCreate()
  test ("join example"){

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


    finalResult.show()

  }

  test ("mean test "){
    import spark.implicits._

    val data = spark.sparkContext.parallelize(Seq(
      ("2017-04-06 00:00:00,2017-04-05 00:00:00"),
      ("2017-04-05 00:00:00,2017-04-04 00:00:00"),
      ("2017-04-04 00:00:00,2017-04-03 00:00:00"),
      ("2017-04-03 00:00:00,2017-03-31 00:00:00"),
      ("2017-03-31 00:00:00,2017-03-30 00:00:00"),
      ("2017-03-30 00:00:00,2017-03-29 00:00:00"),
      ("2017-03-29 00:00:00,2017-03-28 00:00:00"),
      ("2017-03-28 00:00:00,2017-03-27 00:00:00"),
      ("2017-04-06 00:00:00,2017-04-05 00:00:00"),
      ("2017-04-05 00:00:00,2017-04-04 00:00:00"),
      ("2017-04-04 00:00:00,2017-04-03 00:00:00"),
      ("2017-04-03 00:00:00,2017-03-31 00:00:00"),
      ("2017-03-31 00:00:00,2017-03-30 00:00:00"),
      ("2017-03-30 00:00:00,2017-03-29 00:00:00"),
      ("2017-03-29 00:00:00,2017-03-28 00:00:00"),
      ("2017-03-28 00:00:00,2017-03-27 00:00:00"),
      ("2017-04-06 00:00:00,2017-04-05 00:00:00")
    )).toDF("dateRanges")


    val calculateDate = udf((date: String) => {

      val dtf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

        val from = dtf.parseDateTime(date.split(",")(0)).toDateTime()
        val to   = dtf.parseDateTime(date.split(",")(1)).toDateTime()
        val dates = scala.collection.mutable.MutableList[String]()
        var toDate = to
        while(from.getMillis != toDate.getMillis){
          if (from.getMillis > toDate.getMillis){
            dates += from.toString(dtf)
            toDate = toDate.plusDays(1)
          }
          else {
            dates += from.toString(dtf)
            toDate = toDate.minusDays(1)
          }
        }
      dates
    })

    data.withColumn("newDate", calculateDate(data("dateRanges"))).show(false)


//    data.withColumn("newDate", split($"dateRanges", ",")(0).cast("dateTime")).show(false)
//    data.withColumn("newDate", split($"dateRanges", ",")(0).cast("dateTime")).printSchema()

  }








}
