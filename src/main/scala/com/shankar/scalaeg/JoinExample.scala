package com.shankar.scalaeg

import java.io.File

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.Source

/**
  * Created by sakoirala on 5/18/17.
  */
object JoinExample extends App {

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("ParquetAppendMode")
    .getOrCreate()

  import spark.implicits._

  val df1 = spark.sparkContext
    .parallelize(
      Seq(
        ("500001", "100000001", "100000002", "100000003", "100000004"),
        ("500305", "100000001", "100000002", "100000007", ""),
        ("500303", "100000021", "", "", ""),
        ("500702", "110000045", "", "", ""),
        ("500304", "100000021", "100000051", "120000051", ""),
        ("503001", "540000012", "510000012", "500000002", "510000002"),
        ("503051", "880000045", "", "", "")
      ))
    .toDF("contract_id", "cust1_id", "cust2_id", "cust3_id", "cust4_id")

  val df2 = spark.sparkContext
    .parallelize(
      Seq(
        ("100000001", "1988-11-04"),
        ("100000002", "1955-11-16"),
        ("100000003", "1980-04-14"),
        ("100000004", "1980-09-26"),
        ("100000007", "1942-03-07"),
        ("100000021", "1964-06-22"),
        ("100000051", "1920-03-12"),
        ("120000051", "1973-11-17"),
        ("110000045", "1955-11-16"),
        ("880000045", "1980-04-14"),
        ("540000012", "1980-09-26"),
        ("510000012", "1973-03-15"),
        ("500000002", "1958-08-18"),
        ("510000002", "1942-03-07")
      ))
    .toDF("cust_id", "date_of_birth")
/*
  val finalDF = df1
    .join(df2, df1("cust1_id") === df2("cust_id"), "left")
    .drop("cust_id")
    .withColumnRenamed("date_of_birth", " cust1_dob")
    .join(df2, df1("cust2_id") === df2("cust_id"), "left")
    .drop("cust_id")
    .withColumnRenamed("date_of_birth", " cust2_dob")
    .join(df2, df1("cust3_id") === df2("cust_id"), "left")
    .drop("cust_id")
    .withColumnRenamed("date_of_birth", " cust3_dob")
    .join(df2, df1("cust4_id") === df2("cust_id"), "left")
    .drop("cust_id")
    .withColumnRenamed("date_of_birth", " cust4_dob")

  finalDF.na.fill("").show()*/

  val df = spark.sparkContext.parallelize(Seq((1,"right"),(2,"right"),(3,"right"),(4,"wrong"))).toDF("id","decision")


  val dataframe = spark.sparkContext.parallelize(Seq(
    ("A", "a"),
      ("A", "b"),
      ("B", "b"),
      ("B", "c")
  )).toDF("col", "desc")


  dataframe.groupBy("col").agg(collect_list(struct("desc")).as("desc")).show
dataframe.write.mode(SaveMode.Append)


}

/*
contract_id, cust1_id, cust2_id, cust3_id, cust4_id, cust1_dob, cust2_dob, cust3_dob, cust4_dob
500001,100000001,100000002,100000003,100000004,      1988-11-04,1955-11-16,1980-04-14,1980-09-26
500305,100000001,100000002,100000007,         ,      1988-11-04,1955-11-16,1942-03-07
500303,100000021,         ,         ,         ,      1964-06-22
500702,110000045          ,         ,         ,      1955-11-16
500304,100000021,100000051,120000051,         ,      1964-06-22,1920-03-12,1973-11-17
503001,540000012,510000012,500000002,510000002,      1980-09-26,1973-03-15,1958-08-18,1942-03-07
503051,880000045          ,         ,         ,      1980-04-14


+-----------+---------+---------+---------+---------+----------+----------+----------+----------+
|contract_id| cust1_id| cust2_id| cust3_id| cust4_id| cust1_dob| cust2_dob| cust3_dob| cust4_dob|
+-----------+---------+---------+---------+---------+----------+----------+----------+----------+
|     500001|100000001|100000002|100000003|100000004|1988-11-04|1955-11-16|1980-04-14|1980-09-26|
|     503001|540000012|510000012|500000002|510000002|1980-09-26|1973-03-15|1958-08-18|1942-03-07|
|     500304|100000021|100000051|120000051|         |1964-06-22|1920-03-12|1973-11-17|      null|
|     500305|100000001|100000002|100000007|         |1988-11-04|1955-11-16|1942-03-07|      null|
|     503051|880000045|         |         |         |1980-04-14|      null|      null|      null|
|     500303|100000021|         |         |         |1964-06-22|      null|      null|      null|
|     500702|110000045|         |         |         |1955-11-16|      null|      null|      null|
+-----------+---------+---------+---------+---------+----------+----------+----------+----------+




 */
