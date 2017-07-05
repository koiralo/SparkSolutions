package com.shankar.example

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.apache.spark.sql.functions.mean

import scala.util.{Failure, Success, Try}


/**
  * Created by sakoirala on 6/5/17.
  */

case class Person(name: String, age: Int, address: String)

class JoinExampleTest extends FunSuite with BeforeAndAfterEach {

  val spark =
    SparkSession.builder().master("local").appName("test").getOrCreate()
  test("test column to row conversion") {
    import spark.implicits._
    val df1 = spark.sparkContext.parallelize(Seq(
          (1, "user1@test.com", "EN", "US"),
          (2, "user2@test2.com", "EN", "GB"),
          (3, "user3@test3.com", "FR", "FR")
        ))
      .toDF("id", "email", "language", "location")

    val df2 = spark.sparkContext.parallelize(Seq(
          (1, 1, 1, 300, "prod1"),
          (2, 1, 2, 300, "prod1"),
          (3, 1, 2, 300, "prod2"),
          (4, 2, 3, 100, "prod2"),
          (5, 1, 3, 300, "prod3")
        )
      ).toDF("transaction-id", "product-id","user-id", "purchase-amount", "itemdescription")


    val df2Count = df2.groupBy("user-id").count().alias("count_product_id")

    val result = df2Count.join(df1, df2Count("user-id") === df1("id"))

    result.select("user-id", "count", "location").show


  }
  test("test subtract rdd"){

    val df1 = spark.sparkContext.parallelize(Array(
      Person("Mary",28,"New York"),
      Person("Bill",17,"Philadelphia"),
      Person("Craig",34,"Philadelphia"),
      Person("Leah",26,"Rochester")))

    val df2 = spark.sparkContext.parallelize(Array(
      Person("Mary",28,"New York"),
      Person("Bill",17,"Philadelphia"),
      Person("Craig",35,"Philadelphia"),
      Person("Leah",26,"Rochester")
    ))
    println("**********************" + spark.version)
    df1.subtract(df2).collect.foreach(println)
  }

  test("test column and datatypes "){

    import spark.implicits._
    val data = spark.sparkContext.parallelize(Seq(
      (123,"abc", "def", 20, 50, "xyz", 1234),
      (456,"abc", "def", 20, 50, "xyz", 1234)
    )).toDF("id", "firstName", "latName", "Age", "DailyRate", "Dept", "DistanceFromHome")


//    data.agg(min($"id"), max($"id")).show()

    def append = udf((name: String, value:String) => {
      name + value
    })

//    data.withColumn("test", append($"firstName", lit("shankar"))).show()

  val df1 = spark.sparkContext.parallelize(Seq(12, 23, 31,67)).toDF("age")

    val df2 = df1.withColumn("id", lit("a"))

    val window = Window.orderBy("id")

    val result = df2.withColumn("age123", lag($"age", 0).over(window))

    result.show()


    /*

//    val e = d.withColumn("rankInt", d("rank").cast(IntegerType))

    val e = d.withColumn("id", monotonically_increasing_id())

    e.show

    val window = Window.partitionBy("rank").orderBy("id")

    val addCol = udf((value: Float) => {
      if (value == Float.NaN || value == null){
        lag(("rank"), 1)
      }
    })

    e.withColumn("test", coalesce((0 to 90).map(i=>lag(e.col("rank"),i,0).over(window)): _*)).show()

//    e.withColumn("newRank", when(e("rank") === null or e("rank") === Float.NaN, (lag(("rank"), 1))).otherwise(($"rank"))).show

*/




  }

  test("dataset example" ){
    var PRV_RANK = 0f

    import spark.implicits._
    val data = spark.sparkContext.parallelize(Seq(10f, 10f, Float.NaN, Float.NaN, 15f, Float.NaN, 20f, Float.NaN, Float.NaN, 15f, Float.NaN, 10f))
      .toDF("rank")

    val forwardFill = udf((rank: Float) =>
    {
      if (rank == null || rank.equals(Float.NaN)){
        PRV_RANK
      }
      else {
        PRV_RANK = rank
        rank
      }
    })

    data.withColumn("rankNew", forwardFill($"rank")).show()
  }

  test("list test "){
import spark.implicits._
//    val data = spark.sparkContext.parallelize(Seq((3,7), (2,4), (7,3)))

    val rdd1 = spark.sparkContext.parallelize(Seq("a","b")).toDF("name")
    val rdd2 = spark.sparkContext.parallelize(Seq(("a", 3), ("b", 5), ("c",4))).toDF("name1", "id")

//    rdd1.join(rdd2, rdd1("name") === rdd2("name1")).drop("name1").rdd.map(row => (row(0), row(1))).collect().foreach(println)


  val data = spark.sparkContext.parallelize(Seq(
    (1, "A", List(1,2,3)),
    (2, "B", List(3, 5))
  )).toDF("FieldA", "FieldB", "FieldC")


    data.withColumn("ExplodedField", explode($"FieldC")).drop("FieldC")


  }

  test ("null pointer test "){
    import spark.implicits._
//    val data = spark.sparkContext.parallelize(Seq((10.1f, "pqr"), (20.1f, "xyz"), (Float.NaN, "null"), (20.1f, "null"))).toDF("salary", "name")
    val data = spark.read.option("header", true).option("inferSchema", true).format("com.databricks.spark.csv")
      .load("/home/sakoirala/IdeaProjects/SparkSolutions/src/main/resources/nullValue.csv")


    data.na.fill(data.columns.zip(
      data.select(data.columns.map(mean(_)): _*).first.toSeq
    ).toMap).show()

    val map = Map("Name" -> "a", "Place" -> "a2")

    data.na.fill(map).show()

    data.columns.zip(
    data.select(data.columns.map(mean(_)): _*).first().toSeq
    ).toMap.foreach(println)

    var newDF = data
    data.dtypes.foreach { x =>
      val colName = x._1
      val fill = data.agg(max(col(s"`$colName`"))).first()(0).toString
      newDF = newDF.withColumn(colName, when(col(s"`$colName`").isNull , fill).otherwise(col(s"`$colName`")) )
    }
    newDF.show(false)







  }

  test ("partition by example"){
    val rdd=spark.sparkContext.parallelize(List(1,3,2,4,5,6,7,8).zip(List("a", "b", "c","a", "b", "c")),4)

    import spark.implicits._
    val df = rdd.toDF("values", "id").withColumn("csum", sum(col("values")).over(Window.partitionBy("id").orderBy("id")))
    df.show()
    println(s"numPartitions ${df.rdd.getNumPartitions}")
  }

  test ("test mrge "){
    import spark.implicits._
    val df1 = spark.sparkContext.parallelize(Seq(
      (0,"John",3),
    (1,"Paul",4),
    (2,"George",5)
    )).toDF("id", "uid1", "var1")

    val df2 = spark.sparkContext.parallelize(Seq(
      (0,"John",23),
      (1,"Paul",44),
      (2,"George",52)
    )).toDF("id", "uid1", "var2")

    val df3 = spark.sparkContext.parallelize(Seq(
      (0,"John",31),
      (1,"Paul",45),
      (2,"George",53)
    )).toDF("id", "uid1", "var3")

    df1.join(df2, df1("id") === df2("id"), "leftouter").show




    val df = List(df1, df2, df3)

//    df.reduce((a,b) => a.join(b, Seq("id", "uid1"))).show





  }

  test ("test "){
    import spark.implicits._
    val data = spark.sparkContext.parallelize(Seq("8106f510000dc502","8106f510000dc502", "8106f510000dc502")).toDF("info")

    val exec = udf((input : String) => {
      if (input == null || input.trim == "") ""
      else {
        Try{
          val ca = input.toCharArray
          List(3,1,5,7,6,9,10,11,12,13,14,15,16,4,2).map(a=>ca(a-1)).mkString
        } match{
          case Success(data) => data
          case Failure(e)  =>
            println(e.printStackTrace())
            ""
        }
      }
    })

    data.show
    data.withColumn("newInfo", exec(data("info"))).show



  }





}
case class Employee(city: String, name: String)

