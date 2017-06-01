package com.shankar.scalaeg2

import java.sql.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

/**
  * Created by sakoirala on 5/30/17.
  */
object DataSetNullFill extends App {
  val spark =
    SparkSession.builder().master("local").appName("test").getOrCreate()

  import spark.implicits._

  val data = spark.sparkContext.parallelize(
    Seq((0f,0f, "2016-01-1"),
        (1f,1f, "2016-02-2"),
        (2f,2f, null),
        (Float.NaN,Float.NaN, "2016-04-25"),
        (4f,4f, "2016-05-21"),
        (Float.NaN,Float.NaN, "2016-06-1"),
        (6f,6f, "2016-03-21"))
  ).toDF("id1", "id", "date")

  data.show()

/*  //Cast to date type
  val data1 = data.withColumn("date", $"date".cast(DateType))
  //add 1 month in each row
  data.withColumn("date", add_months($"date", 1)).show
  //add 30 days in each row
  data.withColumn("date", date_add($"date", 30)).show*/

  data.na.fill(0).show
  data.na.fill(null, Seq("blank"))













  /* val df1 = spark.sparkContext.parallelize(Seq(
        (1,1,2,2),
        (1,1,2,3),
        (1,1,2,4),
        (1,1,2,6),
        (1,1,2,8)
      )).toDF("x", "y", "z", "t")

  val df2 = spark.sparkContext
    .parallelize(
      Seq(
        (1,1,2,1),
        (1,1,2,2),
        (1,1,2,3),
        (1,1,2,4),
        (1,1,2,5),
        (1,1,2,6),
        (1,1,2,7)
      )).toDF("x1", "y1", "z1", "t1")

  df2.join(df1,
    df2.col("x1").equalTo(df1.col("x"))
      .and(df2.col("y1").equalTo(df1.col("y")))
      .and(df2.col("z1").equalTo(df1.col("z")))).show()*/

}
