package com.shankar.scalaeg

import java.io.{BufferedReader, InputStreamReader}

import org.apache.commons.io.IOUtils
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by sakoirala on 5/10/17.
  */
object ExcelToCsv extends App{



  //Initialise Spark Session
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("ParquetAppendMode")
    .getOrCreate()

  import spark.implicits._

  val dataFrame = spark.sparkContext.parallelize(Seq((1, 123, "shankar", "koirala"))).toDF("id", "age", "fname", "lname")


  val fields = spark.sparkContext.parallelize(dataFrame.schema.map(st => st.dataType))

//  fields.groupBy("fields").show




  val small_df = spark.sparkContext.parallelize(List(("Alice", List("15", "16", "16")), ("Bob", List("20", "20", "30")))).toDF("name", "age")

  val large_df = spark.sparkContext.parallelize(List(("Alice", 30), ("Alice", 30), ("Bob", 45), ("Bob", 40), ("SomeOne", 50))).toDF("name", "age")
//    .registerTempTable("myData")


// spark.sqlContext.sql("select distinct('name') from myData").show

//  large_df.dropDuplicates("name").show



//  val distinct = udf((x: Seq[String]) => x.distinct)

//small_df.withColumn("age", approx_count_distinct($"age")).show
















/*  //Initialise Spark Session
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("ParquetAppendMode")
    .getOrCreate()

  import spark.implicits._

  //Let us suppose you have 2 columns not 10
  //this is how you create a dataframe
  val data = spark.sparkContext.parallelize(Seq(
    (1, "abcd"),
    (2, "efgh"),
    (3, "hijk"),
    (4, "lmno")
  )).toDF("id", "name")

  //if you need to add a new colummn in this dataframe
  //you need to use withColumn

  data.withColumn("date", unix_timestamp()).show()*/




}
