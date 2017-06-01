package com.shankar.excel

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Encoders, SQLContext, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by sakoirala on 5/29/17.
  */
case class Dept(dept_name : String, Salary : Int)
object ExcelToSpark extends App {


  val spark = SparkSession.builder().appName("test excel ").master("local").getOrCreate()
  import spark.implicits._
//  val file = "/home/sakoirala/Documents/eu-historical-price-series_en.xls"
  val file = "/home/sakoirala/IdeaProjects/SparkSolutions/src/main/resources/ip.csv"

  val data = spark.sparkContext.textFile(file).map(_.split(","))

  val recs = data.map(r => Dept(r(0), r(1).toInt )).toDS()


  recs.groupBy($"dept_name").agg(max("Salary").alias("max_solution")).show()


  val ds = spark.read
      .option("header", true)
      .schema(Encoders.product[Dept].schema)
      .csv(file).as[Dept]



  /// /  (max("Salary").alias("max_solution")).show()
/*
  val sc = new SparkContext(new SparkConf().setAppName("Max Salary").setMaster("local[2]"))

  val sq = new SQLContext(sc)

  import sq.implicits._*/










  /* val data = spark.sparkContext.parallelize(Seq(
     (1, Map(1 -> "a")),
     (1, Map(2 -> "b")),
     (1, Map(3 -> "c")),
     (2, Map(1 -> "a")),
     (2, Map(2 -> "b")),
     (2, Map(3 -> "c"))
   )).toDF("id", "maps")


   data.groupBy("id").agg(collect_list(data("maps")).alias("maps")).show(false)*/




}
