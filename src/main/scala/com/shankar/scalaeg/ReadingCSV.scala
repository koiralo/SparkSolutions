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

  val dfList1 = spark.read.option("inferSchema", true)csv("/home/sakoirala/IdeaProjects/SparkSolutions/src/main/resources/data1.csv")

  dfList1.show()

//  val dfList = spark.createDataFrame(dfList1.rdd, titschema)

  dfList1.printSchema()
//  dfList.printSchema()
//
//  dfList.show()

  dfList1.coalesce(1).rdd.saveAsTextFile("/home/sakoirala/IdeaProjects/SparkSolutions/src/main/resources/testfile.csv")

  case class tit(Num: String,
                 Class: String,
                 Survival_Code: Int,
                 Name: String,
                 Age: Int,
                 Province: String,
                 Address: String,
                 Coach_No: String,
                 Coach_ID: String,
                 Floor_No: Int,
                 Gender: String)

}
