package com.shankar.scalaeg

import org.apache.spark.sql.SparkSession

/**
  * Created by sakoirala on 5/17/17.
  */
object ReadTSVFile extends App {

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("ParquetAppendMode")
    .getOrCreate()

  import spark.implicits._

  val data =
    "col1 col2 col3 col4 col5 col6 col7 col8\nval1 val2 val3 val4 val5 val6 val7 val8\nval9 val10 val11 val12 val13 val14 val15 val16\nval17 val18 val19 val20 val21 val22 val23 val24"

  val spited = data.split("\n").map(columns => columns.split(" "))

  spark.sparkContext.parallelize(spited).toDF().show()
}
