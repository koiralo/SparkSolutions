package com.shankar.testing

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, TimestampType}
import org.scalatest.{BeforeAndAfterEach, FunSuite, Outcome}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by sakoirala on 6/14/17.
  */
 class TestJoinTwoDataframe extends FunSuite with BeforeAndAfterEach
{

  val spark = SparkSession.builder().master("local").getOrCreate()

  test("test merge two dataframes "){
    import spark.implicits._

    val df1 = spark.sparkContext.parallelize(Seq(
      (1, "abc"),
      (2, "def"),
      (3, "hij")
    )).toDF("id", "name")

    val df2 = spark.sparkContext.parallelize(Seq(
      (19, "x"),
      (29, "y"),
      (39, "z")
    )).toDF("age", "address")

    val to = df2.columns.map(col(_))



    val from = (1 to to.length).map( i => (s"column$i"))

    df2.select(to.zip(from).map { case (x, y) => x.alias(y) }: _*).show



    val ddf1 = df1.withColumn("row_iddf1", monotonically_increasing_id())
    val ddf2 = df2.withColumn("row_iddf2", monotonically_increasing_id())

    val result = ddf1.join(ddf2, ddf1("row_iddf1") === ddf2("row_iddf2") + 1)

//    result.show()


//    val data = ArrayBuffer(ArrayBuffer(ArrayBuffer(1, "a"), 5), ArrayBuffer(ArrayBuffer(1, "b"), 5))
//
//    data.map(arr => arr.map(value =>
//      value match {
//        case isInstanceOf[ArrayBuffer] => value.toArray
//        case _ => value
//      } ))



    val t1 = spark.sparkContext.parallelize(Seq(142.13, 157.34, 168.45, 170.23)).toDF("c1")
    val t2 = spark.sparkContext.parallelize(Seq(141.16,145.45,155.85,166.76,168.44)).toDF("c2")






    val t11 = t1.withColumn("id", monotonically_increasing_id())
    val t22 = t2.withColumn("id", monotonically_increasing_id())

    val res = t11.join(t22, t11("id") + 1 === t22("id") ).drop("id")

//    res.show


    val parsedData = spark.sparkContext.parallelize(Seq(1.0,1.0,1.0,0.0,0.0,0.0))//.toDF()
//      .withColumn("id", monotonically_increasing_id())

    val resultatOfprediction = spark.sparkContext.parallelize(Seq(
      (0.0,0.0,0.0),
      (0.1,0.1,0.1),
      (0.2,0.2,0.2),
      (9.0,9.0,9.0),
      (9.1,9.1,9.1),
      (9.2,9.2,9.2)
    )).toDF().withColumn("id", monotonically_increasing_id())

   val rdd = Seq((1,2,3), (2,2,4), (3,2,5), (4,2,6),(5,2,7)).toDF("col1", "col2", "col3")
    rdd.show
//    rdd.agg(avg())




//    resultatOfprediction.zip(parsedData) .map(t => t._1.productIterator.toList.map(_.asInstanceOf[Double]) :+ t._2).foreach(println)


//    resultatOfprediction.join(parsedData, "id").drop("id")




//    d1.join(d2)

//      .map(t => (t._1._1, t._1._2, t._1._3, t._2)).foreach(println)


  }

  test ("rdd"){
    import spark.implicits._

    val RDD1 = spark.sparkContext.parallelize(Seq(
      (1, "2017-2-13", "ABX-3354 gsfette"),
      (2, "2017-3-18", "TYET-3423 asdsad"),
      (3, "2017-2-09", "TYET-3423 rewriu"),
      (4, "2017-2-13", "ABX-3354 42324"),
      (5, "2017-4-01", "TYET-3423 aerr")
    ))

    val RDD2 = spark.sparkContext.parallelize(Seq(
      ("mfr1","ABX-3354"),
      ("mfr2","TYET-3423")
    ))

    RDD1.map(r =>{
      (r._3.split(" ")(0), (r._1, r._2, r._3))
    })
      .join(RDD2.map(r => (r._2, r._1)))
      .groupBy(_._1)
      .map(r => (r._1, r._2.toSeq.size))
      .foreach(println)









  }
}
