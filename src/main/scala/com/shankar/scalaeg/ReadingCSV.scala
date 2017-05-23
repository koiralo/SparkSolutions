package com.shankar.scalaeg

import org.apache.spark.sql.SparkSession

/**
  * Created by sakoirala on 5/22/17.
  */
object ReadingCSV extends App {

  import org.apache.spark.sql.Encoders
  val spark =
    SparkSession.builder().master("local").appName("test").getOrCreate()

  import spark.implicits._

  val titschema = Encoders.product[tit].schema

  val dfList = spark.read.schema(schema = titschema).csv("data.csv").as[tit]

  dfList.show()

  case class tit(Num: Int,
                 Class: String,
                 Survival_Code: Int,
                 Name: String,
                 Age: Double,
                 Province: String,
                 Address: String,
                 Coach_No: String,
                 Coach_ID: String,
                 Floor_No: Int,
                 Gender: String)

}
