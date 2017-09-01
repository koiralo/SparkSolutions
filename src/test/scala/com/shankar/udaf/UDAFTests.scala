package com.shankar.udaf

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction, Window}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.apache.spark.sql.Encoders

/**
  * Created by sakoirala on 7/20/17.
  */
class UDAFTests extends FunSuite with BeforeAndAfterEach{

  val schema = StructType(Seq(
    StructField("idx", StringType, false),
    StructField("v1", StringType, false),
    StructField("v2", StringType, false)
  ))

  val bufferSchema = Encoders.product[test].schema


  val spark = SparkSession.builder().appName("test UDAF").master("local").getOrCreate()
  test ("test UDAF "){

    import spark.implicits._



    val df1 = spark.sparkContext.parallelize(Seq(
      ("a", 100),
      ("b", 200)
    )).toDF("idA", "numA")

    val df2 = spark.sparkContext.parallelize(Seq(
      ("a", 500),
      ("a", 600)
    )).toDF("idB", "numB")

    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    df1.join(df2, $"idA" === "idB", "right").show






  }

  class AverageScore extends UserDefinedAggregateFunction{

    override def inputSchema: StructType ={
      println("This is input schema")
      println(schema)
      schema
    }

    override def bufferSchema: StructType = {
      println("This is buffer schema")
      bufferSchema
    }

    override def dataType: DataType = bufferSchema

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      println("This is a init")
      buffer.update(0, Array)
      buffer.update(1, Array)
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      println("*******************")
      val v1 = input.getInt(1)
      val v2 = input.getInt(0)

      val b1 = buffer.getAs[Array[Int]](0)
      val b2 = buffer.getAs[Array[Int]](1)

      buffer.update(0, b1 :+ v1)
      buffer.update(1, b2 :+ v2)

      print("******************")
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      println("%%%%%%%%%%%%%%%%%%%%%")
      println(buffer1.getDouble(0)+"   "  +buffer2.getDouble(0))
      println("%%%%%%%%%%%%%%%%%%%%%")
      buffer1.update(0,buffer2.getAs[Array[Int]](0))
      buffer1.update(0,buffer2.getAs[Array[Int]](0))

    }

    override def evaluate(buffer: Row): Any = {
      println("______________")
      println(buffer.getDouble(0))
      println("_____________")
      buffer.getAs[Array[Int]](0) ++buffer.getAs[Array[Int]](1)
    }
  }


  test("random id "){
    import spark.implicits._
    val df = Seq(1,4,3,2,5,7,3,5,4,18).zipWithIndex.toDF("values", "index")

    df.show

//    val window = Window.partitionBy("index" ).orderBy("index")
    val window1 = Window.orderBy("index")

//    val resultLag = df.withColumn("newCol", lag($"values", 2, 1).over(window))

    val resultRows = df.withColumn("newCol", lag($"values", 2, 1).over(Window.orderBy("index")))
    resultRows.withColumn("index", monotonically_increasing_id())

//    println("result LAG   " + resultLag.rdd.getNumPartitions)
    println("result Rows    " + resultRows.rdd.getNumPartitions)

    println("************  " + resultRows.repartition(100).rdd.getNumPartitions)

    resultRows.show()

    val res = resultRows.as[tets]

    res


  }





}

case class tets (values: Int, index: Int, newCol:Int)

case class test( value : Seq[Seq[Int]])
