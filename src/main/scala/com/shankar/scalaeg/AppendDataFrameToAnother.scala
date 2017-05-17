package com.shankar.scalaeg

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by sakoirala on 5/17/17.
  */
object AppendDataFrameToAnother extends App {

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("ParquetAppendMode")
    .getOrCreate()

  import spark.implicits._

  //Create dfList dataframe
/*  val dfList = spark.sparkContext
    .parallelize(
      Seq(
        (41, "912AEQ",  2016022l, "UJ"),
        (82, "912ARD",  2016022l, "GH"),
        (903, "912AYQ", 2016022l, "KL"),
        (454, "912AKK", 2016022l, "KL"),
        (95, "912AHG",  2016022l, "KH")
      ))
    .toDF("A", "B", "C", "D")*/


  val a = spark.sparkContext.parallelize(Seq(41, 82, 903, 454, 95)).toDF("A")

  val b = spark.sparkContext.parallelize(Seq("912AEQ", "912ARD", "912AYQ", "912AKK", "912AHG")).toDF("B")

  val c = spark.sparkContext.parallelize(Seq(2016022l, 2016022l, 2016022l, 2016022l, 2016022l)).toDF("C")

  val d = spark.sparkContext.parallelize(Seq("UJ", "GH", "KL", "KL", "KH")).toDF("D")

  val list = List(a,b,c,d)

  val dfList = list.map(df => {
    df.rdd.map(r=>r(0)).collect()
  })




/*

  //Create df dataframe
  val df = spark.sparkContext
    .parallelize(
      Seq(
        (11, "AS", 989765498l, "SDAWQ"),
        (12, "GH", 7654998599l, "TRUDR"),
        (13, "IO", 10654998580l, "ABUCK"),
        (14, "1JG", 65499855101l, "KLBCK"),
        (15, "RT", 10265499852l, "BCKKL")
      ))
    .toDF("id", "v1", "v2", "v3")

  val dfListWithIndex = addIndex(dfList) // and index column
  val dfWithIndex = addIndex(df).drop("v1", "v2", "v3") //add index column and remove unnecessary columns

  val newDF = dfWithIndex.join(dfListWithIndex, "index").drop("index") //join two dataframe and drop index column


  dfListWithIndex.show()
  dfWithIndex.show()
  newDF.printSchema()
  newDF.show

*/
  def addIndex(df: DataFrame) = spark.sqlContext.createDataFrame(
    // Add index column
    df.rdd.zipWithIndex.map {
      case (row, index) => Row.fromSeq(row.toSeq :+ index)
    },
    // Create schema for index column
    StructType(df.schema.fields :+ StructField("index", LongType, false))
  )



}
