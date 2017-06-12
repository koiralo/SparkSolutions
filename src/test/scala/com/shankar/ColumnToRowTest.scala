package com.shankar

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FunSuite, Outcome}

/**
  * Created by sakoirala on 5/9/17.
  */
class ColumnToRowTest extends FunSuite with BeforeAndAfterEach {

  val spark =
    SparkSession.builder().master("local").appName("test").getOrCreate()
  test("test column to row conversion") {

    import spark.implicits._

    val data = spark.sparkContext
      .parallelize(Seq(
        ("4623784", "John", "Smith", "35", "Engineer"),
        ("24342", "Michael", "Levine", "", "Pilot"),
        ("24342", "Michael", "Levine", "", "Pilot"),
        ("324234", "Charles", "", "54", "Manager")
      ))
      .toDF("UT", "FirstName", "LastName", "Age", "Job")

    data.map(row => row.getAs[String]("UT")).show()
    val columns = data.columns

    val result = data.map(row => {
      columns.map(column => {
        (row.getAs[String](0), column)
      })
    }) /*.toDF("UT", "AttributeName")*/

//    println("************* " + result)
//    spark.sparkContext.parallelize(result)
    result.show(false)
  }

//  override protected def withFixture(test: Any): Outcome = ???

  test("test string") {

    val data = spark.sparkContext.parallelize(
      Seq(
        ("pan;Shinjuku"),
        ("Australia;Melbourne"),
          ("United States of America;New York"),
          ("Australia;Canberra"),
          ("Australia;Sydney"),
          ("Japan;Tokyo")
      ))

    val exRDD = data.cache()
    val result = exRDD.map(
        rec =>
          (rec.split(";")(0),rec.split(";")(1)))

    result.foreach(println)





/*    val result = exRDD.map(
      rec =>{
        val value = rec.split(",")
        BidItem(value(0),value(1),value(2),value(3).toDouble)
      })*/
/*
    result.collect().foreach(println)*/

  }

}
case class BidItem(date: String, curr: String, cont: String, rate: Double)