package com.shankar.scalaeg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by sakoirala on 5/24/17.
  */
object ColumnSubsetExample extends  App{


  val spark =
    SparkSession.builder().master("local").appName("test").getOrCreate()

  import spark.implicits._

  val data = spark.sparkContext.parallelize(
  Seq(
    (1,Array("Adventure","Comedy"),Array("Adventure")),
  (2,Array("Animation","Drama","War"),Array("War","Drama")),
  (3,Array("Adventure","Drama"),Array("Drama","War"))
  )).toDF("movieId1", "genreList1", "genreList2")

  data.show



  val subsetOf = udf((col1: Seq[String], col2: Seq[String]) => {
    if (col2.toSet.subsetOf(col1.toSet)) 1 else 0
  })

  data.withColumn("flag", subsetOf(data("genreList1"), data("genreList2"))).show()

}
