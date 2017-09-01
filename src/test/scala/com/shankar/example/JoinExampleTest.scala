package com.shankar.example

import java.sql.Timestamp
import java.util.regex.Pattern

import org.apache.poi.ss.formula.functions.Columns
import org.apache.spark.sql.{Column, Encoders, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.apache.spark.sql.functions.mean

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}


/**
  * Created by sakoirala on 6/5/17.
  */

case class Person(name: String, age: Int, address: String)

class JoinExampleTest extends FunSuite with BeforeAndAfterEach {

  val spark =
    SparkSession.builder.master("local[*]").appName("test").getOrCreate()
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

    def test123(v:String): String ={
      "test"
    }

    spark.sqlContext.udf.register("test", test123 _)

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

  test ("test example"){

    import spark.implicits._

    val input = Array(1,4,3,2,5,7,3,5,4,18).zipWithIndex

    var df=spark.sparkContext.parallelize(input,3).toDF("value2","value1")

    df.show

//    val colSum = df.columns.map(c => sum(c).as(s"${c}_sum"))

//    df.groupBy().agg(colSum.head, colSum.tail:_*).show()

    val sum = udf((cols : Seq[Any]) => {
      cols.map(x => x.asInstanceOf[Int]).reduce(_+_)
    })

//    df.withColumn("total", df.columns.map(col(_)).reduce(_ + _)).show

//    df.withColumn("total", sum(Seq(Array(df.columns.head, df.columns.tail:_*)))).show


  }


  test ("test condition "){

    import spark.implicits._

    import org.apache.spark.sql.functions._
    //create a dummy data
    val df = Seq((35, "M", "Joanna", "F"),
      (25, "S", "Isabelle", "F"),
      (19, "S", "Andy", "M"),
      (70, "M", "Robert", "M")
    ).toDF("age", "maritalStatus", "name", "sex")

    // create a udf to update name according to age and sex
    val append = udf((name: String, maritalStatus:String, sex: String) => {
      println("***********   " + sex.equalsIgnoreCase("F"),  maritalStatus)
      if (sex.equalsIgnoreCase("F") &&  maritalStatus.equalsIgnoreCase("M") ) s"Mrs. ${name}"
      else if (sex.equalsIgnoreCase("F")) s"Ms. ${name}"
      else s"Mr. ${name}"
    })

/*    val append = udf((name: String, age:Int, sex: String) => {
      if (sex.equalsIgnoreCase("F") && age > 30) s"Mrs. ${name}"
      else if (sex.equalsIgnoreCase("F")) s"Ms. ${name}"
      else s"Mr. ${name}"
    })*/



    //add two new columns with using withColumn
/*    val result = df.withColumn("name", append($"name", $"maritalStatus", $"sex"))
      .withColumn("seniorCitizen", when($"age" < 60, "N").otherwise("Y")).show()*/


    val updateName = when(lower($"maritalStatus") === "m" && lower($"sex") === "f", concat(lit("Mrs. "), $"name"))
      .otherwise(when(lower($"maritalStatus") === "s" && lower($"sex") === "f", concat(lit("Ms. "), $"name"))
        .otherwise(when(lower($"sex") === "m", concat(lit("Mr. "), $"name"))))

/*    df.withColumn("name", updateName)
      .withColumn("seniorCitizen", when($"age" < 60, "N").otherwise("Y")).show()*/




    df.withColumn("name",
      when($"sex" === "F", when($"maritalStatus" === "M",  concat(lit("Ms. "), df("name"))).otherwise(concat(lit("Ms. "), df("name"))))
        .otherwise(concat(lit("Ms. "), df("name"))))
      .withColumn("seniorCitizen", when($"age" < 60, "N").otherwise("Y")).show

  }

  test ("cas classes "){
    import spark.implicits._

    import org.apache.spark.sql.functions._
    //create a dummy data

    val input = "/home/sakoirala/IdeaProjects/SparkSolutions/src/main/resources/data1.csv"
    var products = spark.sparkContext.textFile(input).map(r => {
      var p = r.split('|');
      (p(0).toInt, p(1).toInt, p(2), p(3), p(4).toFloat, p(5))
    })
    var productsDF = products.map(r => Products(r._1, r._2, r._3, r._4, r._5, r._6)).toDF()

    productsDF.show

    val productsDFnew = spark.sparkContext.textFile(input).map(_.split(Pattern.quote("|"))).map(p =>
      Products(p(0).trim.toInt, p(1).trim.toInt, p(2), p(3), p(4).trim.toFloat, p(5))
    ).toDF()
    productsDFnew.show()

  }

  test ("test classes for wrapped") {
    import spark.implicits._

    val df = spark.sparkContext.parallelize(Seq(
      ("t1", "7/26/2017 13:56", "[error = RI_VIOLATION, field = user_id, value = null]"),
      ("t2", "7/26/2017 13:58", "[error = NULL_CHECK, field = geo_id, value = null] [error = DATATYPE_CHECK, field = emp_id, value = FIWOERE8]")
    )).toDF("table", "err_timestamp", "err_message")

    df.show(false)

    val splitValue = udf ((value : String ) => {
      "\\[(.*?)\\]".r.findAllMatchIn(value).map(x => x.toString().replaceAll("\\[", "").replaceAll("\\]", "")).toSeq
    })

    val df1 = df.withColumn("err_message", explode(splitValue($"err_message")))
    df1.show(false)

    val splitExpr = split($"err_message", ",")

    val result = df1.withColumn("err_field", split(splitExpr(1), "=")(1))
      .withColumn("err_type", split(splitExpr(0), "=")(1))
      .withColumn("err_value", split(splitExpr(2), "=")(1)).drop("err_message")


    result.select("table", result.columns: _*)
      .withColumnRenamed("err_timestamp", "label").show





  }


/*
*
root
 |-- table: string (nullable = true)
 |-- err_timestamp: string (nullable = true)
 |-- err_message: string (nullable = true)


* */


  test("learning "){

    import spark.implicits._
/*    val rdd1 = spark.sparkContext.parallelize(Seq(
      (1, 5),
      (7, 5),
      (9, 5))).toDF("num1", "num2")

    rdd1.withColumn("result", when(($"num1" / $"num2") < 1, $"num2").otherwise($"num1")).show()*/


//    rdd1.cartesian(rdd1).map(case (x : (Int, Set[String]), y: (Int, Set[String])) => (x._1, y._1, x._2.intersect(y._2)))

    val arr = Array((1,1),(4,2),(3,3),(2,4),(5,5),(7,6),(3,7),(5,8),(4,9),(18,10))
    var df=spark.sparkContext.parallelize(arr).toDF("value","timestamp")


//    println(df.sort($"timestamp", $"timestamp".desc).first())
//    df.sort($"timestamp", $"timestamp".desc).take(1).foreach(println)

//    df.where($"timestamp" === df.count()).show


//    df.where($"timestamp" === max($"timestamp")).show

    val df1 = spark.sqlContext.createDataFrame(
        df.rdd.zipWithIndex.map {
      case (row, index) => Row.fromSeq(row.toSeq :+ index)
    },
    // Create schema for index column
    StructType(df.schema.fields :+ StructField("index", LongType, false)))

//    df.where($"timestamp" === df.count()).drop("index").show


    val df3 = spark.sparkContext.parallelize(Seq(
      ("a","1", "2017-03-10"),
      ("b","12", "2017-03-9"),
      ("b","123", "2015-03-12"),
      ("c","1234", "2015-03-15"),
      ("c","12345", "2015-03-12")
    ))//.toDF("name", "phonenumber", "timestamp")

    df3.sortBy(x => (x._1, x._3)).foreach(println)


//    val df4 = df3.withColumn("datetime", from_unixtime(unix_timestamp($"datetime", "yyyy-MMM-dd")))
//
//    df4.filter($"datetime" >= (lit("2017-07-10").cast(TimestampType))).show
//    df4.filter($"datetime" < (lit("2017-07-10"))).show
//    df4.filter($"datetime" <= (lit("2017-07-10"))).show
//
//
//    df4.sort("datetime", "value").show()

  }

  test ("udf test "){
    import spark.implicits._
//
//    val df = Seq(
//      (Seq(("a", 1.0), ("a",2.0)), null)
//    ).toDF("ColA", "colB")
//
//
//
//    val calc = udf ((colA: Seq[Row], colB: Seq[Row]) => {
//
//
//
//      Seq.concat(colA,  null)
//      Seq.concat(colA,  colB.map(r =>  Row(r.getAs[String](0), r.getAs[Double](1) *2 )))
/*        .groupBy(r => r.getString(0))
        .mapValues(x => x.map(_.getDouble(1)).min).toArray
        .sortBy(_._1)

        .map(r => s"${r._1}, ${r._2}").mkString(", ")*/
//    })





//    val result = df.withColumn("new", calc($"ColA", $"ColB"))
//    result.show(false)


    val df1 = Seq (
      (0, 0),
      (1, 30),
      (2, 18),
      (3, 10),
      (4, 5),
      (5, 1),
      (1, 8),
      (2, 6),
      (3, 9),
      (4, 3),
      (5, 4),
      (1, 2),
      (2, 18),
      (3, 2),
      (4, 1),
      (5, 15)
    ).toDF("a", "b")

//
    df1.withColumn("index", row_number().over(Window.orderBy())).show(false)






  }

  test ("test options"){

    import spark.implicits._

    val df = Seq(
      ("30000", 40000, 50000),
      (null, 20000, 10000)
    ).toDF("v1", "v2", "v3")




    df.withColumn("id", monotonically_increasing_id() + 8)
//      .drop("id")
      .show(false)

/*
    val testUDF=udff((a: Double, b: Double, c: Double) )=> {
      if(a==null && b!=null && c!=null)
        b+c
      else
        a+b+c
    })
*/


//    a: Option[Double], b: Option[Double] c: Option[Double]
//    df.withColumn("v1", $"v1".cast(DoubleType))
//    df.withColumn("checkNull", testUDF(col("v1"),col("v2"),col("v3"))).show(false)



  }

  test ("group to col "){
    import spark.implicits._

    val df = Seq(
      ("da24a375-962a", "2017-06-04 03:00:00.0", "2017-06-04 03:25:00.0", 2),
      ("9f034a1d-02c1", "2017-06-04 03:50:00.0", "2017-06-04 03:00:00.0", 2),
      ("a3fac437-efcc", "2017-06-04 03:10:00.0", "2017-06-04 03:15:00.0", 2),
      ("9f034a1d-02c1", "2017-06-04 03:00:00.0", "2017-06-04 03:05:00.0", 3),
      ("b4590016-1af2", "2017-06-04 03:45:00.0", "2017-06-04 03:50:00.0", 2),
      ("9f034a1d-02c1", "2017-06-04 03:00:00.0", "2017-06-04 03:00:00.0", 2),
      ("e5e4c972-6599", "2017-06-04 03:25:00.0", "2017-06-04 03:30:00.0", 5),
      ("a3fac437-efcc", "2017-06-04 03:00:00.0", "2017-06-04 03:55:00.0", 2),
      ("bedd88f1-3751", "2017-06-04 03:20:00.0", "2017-06-04 03:00:00.0", 2),
      ("a3fac437-efcc", "2017-06-04 03:20:00.0", "2017-06-04 03:25:00.0", 2)
    ).toDF("userid", "start", "end", "total_unique_loc")

    val A = df.withColumn("start", unix_timestamp($"start", "yyyy-MM-dd HH:mm:SS").cast("timestamp"))
      .withColumn("end", unix_timestamp($"end", "yyyy-MM-dd HH:mm:SS").cast("timestamp"))
      .withColumn("window", struct("start", "end"))

    val arrToString = udf((value: Seq[Seq[Int]]) => {
      value.map(x=> x.map(_.toString).mkString(",")).mkString("::")
    })

    val B = Seq(
      ("9f034a1d-02c1", "2017-06-04 03:00:00.0", "0.17218625176420413"),
      ("9f034a1d-02c1", "2017-06-04 03:00:00.0", "0.11145767867097957"),
      ("9f034a1d-02c1", "2017-06-04 03:00:00.0", "0.14064932728588236"),
      ("a3fac437-efcc", "2017-06-04 03:00:00.0", "0.08328915597349452"),
      ("a3fac437-efcc", "2017-06-04 03:00:00.0", "0.07079054693441306")
    ).toDF("userid", "eventtime", "distance")
      .withColumn("eventTime", unix_timestamp($"eventtime", "yyyy-MM-dd HH:mm:SS").cast(TimestampType))
      .withColumn("eventTime", arrToString($"eventtime"))



//    df2.show(false)
//    df2.printSchema()
//
//    df1.select($"window.start").show

    val result = A.join(B, A("userid") === B("userid") &&  A("window.start") === B("eventtime") || A("window.end") === B("eventtime"), "left")



  }

  test ("dataframe difference "){

    import spark.implicits._
    val df1 = Seq(
      ("city 1", "prod 1", "9/29/2017", 358, 975, 193),
      ("city 1", "prod 2", "8/25/2017", 50 , 687, 201),
      ("city 1", "prod 3", "9/9/2017 ", 236, 431, 169),
      ("city 2", "prod 1", "9/28/2017", 358, 975, 193),
      ("city 2", "prod 2", "8/24/2017", 50 , 687, 201),
      ("city 3", "prod 3", "9/8/2017 ", 236, 431, 169)
    ).toDF("city","product", "date", "sale", "exp", "wastage")


    val df2 = Seq(
    ("city 1", "prod 1", "9/29/2017", 358, 975, 193),
    ("city 1", "prod 2", "8/25/2017", 50 , 687, 201),
    ("city 1", "prod 3", "9/9/2017 ", 230, 430, 160),
    ("city 1", "prod 4", "9/27/2017", 350, 90 , 190),
    ("city 2", "prod 2", "8/24/2017", 50 , 687, 201),
    ("city 3", "prod 3", "9/8/2017 ", 236, 431, 169),
    ("city 3", "prod 4", "9/18/2017", 230, 431, 169)
    ).toDF("city","product", "date", "sale", "exp", "wastage")

//    df1.join(df2, Seq("city","product", "date"), "left").show(false)
//    df1.join(df2, Seq("city","product", "date"), "right").show(false)


/*    val d = Seq(
      ("a", 10, Map("Sports" -> -0.2, "Academics" -> -0.1)),
      ("b", 20, Map("Academics" -> -0.1, "Academics" -> -0.1)),
      ("c", 5, Map("Sports" -> -0.2, "Academics" -> 0.5)),
      ("d", 15, Map("Sports" -> -0.2, "Academics" -> 0))
    ).toDF("Name", "Attendence", "Efficiency")*/

    //dummy data
    val d = Seq(
      ("a", 10, Map("Sports" -> -0.2, "Academics" -> 0.1)),
      ("b", 20, Map("Sports" -> -0.1, "Academics" -> -0.1)),
      ("c", 5, Map("Sports" -> -0.2, "Academics" -> 0.5)),
      ("d", 15, Map("Sports" -> -0.2, "Academics" -> 0.0))
    ).toDF("Name", "Attendence", "Efficiency")

    //explode the map and get key value
/*    val result = d.select($"Name", $"Attendence", explode($"Efficiency"))

    //select value less than 0 and show 100
    result.select("*").where($"value".lt(0))
      .sort($"Attendence".desc)
      .show(100)*/


/*    val df5 = spark.createDataFrame(Seq(
      ("Hi I heard about Spark", "Spark"),
      ("I wish Java could use case classes", "Java"),
      ("Logistic regression models are neat", "models")
    )).toDF("sentence", "label")


    val res = df5.withColumn("sentence_without_label", regexp_replace($"sentence" , lit($"label"), lit("" )))

      res.show(false)*/


    val peopleDF = spark.sparkContext.
      textFile("/home/sakoirala/IdeaProjects/SparkSolutions/src/main/resources/person.txt").
      map(_.split(",")).
      map(attributes => Person123(attributes(0), attributes(1).trim.toInt)).
      toDF()

    peopleDF.show()

  }




}
case class Employee(city: String, name: String)
case class Products(productID: Int, productCategory: Int, productName: String, productDescription: String, productPrice: Float, productImage: String)

case class Person123(name: String, age: Long)


case class Record(transactionDate: Timestamp, product: String, price: Int, paymentType: String, name: String, city: String, state: String, country: String,
                  accountCreated: Timestamp, lastLogin: Timestamp, latitude: String, longitude: String)

/*





* */