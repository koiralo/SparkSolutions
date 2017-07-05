package com.shankar.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.collection.mutable.ListBuffer

/**
  * Created by sakoirala on 7/5/17.
  */
class SchemaExampleTest extends FunSuite with BeforeAndAfterEach{

  val spark = SparkSession.builder().master("local").getOrCreate()
  test ("schema example ") {

    import spark.implicits._

    val rdd = spark.sparkContext.parallelize(Seq(
      (010, 0.11 ,5),
        (100, 0.22 ,4),
        (001, 0.33 ,3),
        (011, 0.01 ,5),
        (101, 0.005,4),
        (110, 0.11 ,3),
        (000, 0.21 ,5),
        (111, 0.0001 ,4)
    ))

  }

}

