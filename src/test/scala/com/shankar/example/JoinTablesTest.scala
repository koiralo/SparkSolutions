package com.shankar.example

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.format.DateTimeFormat
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
  * Created by sakoirala on 6/16/17.
  */
class JoinTablesTest extends FunSuite with BeforeAndAfterEach{

  val spark = SparkSession.builder().master("local").getOrCreate()
  test ("join example"){

    import spark.implicits._

    val df1 = spark.sparkContext.parallelize(Seq(
        ("id1", "id2"),
        ("id1","id3"),
        ("id2","id3")
      )).toDF("id", "idB")

    val df2 = spark.sparkContext.parallelize(Seq(
      ("id1", "blue", "m"),
      ("id2", "red", "s"),
      ("id3", "blue", "s")
    )).toDF("id", "color", "size")

    /*val firstJoin = df1.join(df2, df1("idA") === df2("id"), "inner")
      .withColumnRenamed("color", "colorA")
      .withColumnRenamed("size", "sizeA")
      .withColumnRenamed("id", "idx")*/

    df1.join(df2, "id").show()

//    val secondJoin = firstJoin.join(df2, firstJoin("idB") === df2("id"), "inner")

   /* val check = udf((v1: String, v2:String ) => {
      if (v1.equalsIgnoreCase(v2)) 1 else 0
    })

    val result = secondJoin
      .withColumn("color", check(col("colorA"), col("color")))
      .withColumn("size", check(col("sizeA"), col("size")))

    val finalResult = result.select("idA", "idB", "color", "size")


    finalResult.show()*/

  }

  test ("mean test "){
    import spark.implicits._

    val data = spark.sparkContext.parallelize(Seq(
      ("2017-04-06 00:00:00,2017-04-05 00:00:00"),
      ("2017-04-05 00:00:00,2017-04-04 00:00:00"),
      ("2017-04-04 00:00:00,2017-04-03 00:00:00"),
      ("2017-04-03 00:00:00,2017-03-31 00:00:00"),
      ("2017-03-31 00:00:00,2017-03-30 00:00:00"),
      ("2017-03-30 00:00:00,2017-03-29 00:00:00"),
      ("2017-03-29 00:00:00,2017-03-28 00:00:00"),
      ("2017-03-28 00:00:00,2017-03-27 00:00:00"),
      ("2017-04-06 00:00:00,2017-04-05 00:00:00"),
      ("2017-04-05 00:00:00,2017-04-04 00:00:00"),
      ("2017-04-04 00:00:00,2017-04-03 00:00:00"),
      ("2017-04-03 00:00:00,2017-03-31 00:00:00"),
      ("2017-03-31 00:00:00,2017-03-30 00:00:00"),
      ("2017-03-30 00:00:00,2017-03-29 00:00:00"),
      ("2017-03-29 00:00:00,2017-03-28 00:00:00"),
      ("2017-03-28 00:00:00,2017-03-27 00:00:00"),
      ("2017-04-06 00:00:00,2017-04-05 00:00:00")
    )).toDF("dateRanges")


    val calculateDate = udf((date: String) => {

      val dtf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

        val from = dtf.parseDateTime(date.split(",")(0)).toDateTime()
        val to   = dtf.parseDateTime(date.split(",")(1)).toDateTime()
        val dates = scala.collection.mutable.MutableList[String]()
        var toDate = to
        while(from.getMillis != toDate.getMillis){
          if (from.getMillis > toDate.getMillis){
            dates += from.toString(dtf)
            toDate = toDate.plusDays(1)
          }
          else {
            dates += from.toString(dtf)
            toDate = toDate.minusDays(1)
          }
        }
      dates
    })

    data.withColumn("newDate", calculateDate(data("dateRanges"))).show(false)


//    data.withColumn("newDate", split($"dateRanges", ",")(0).cast("dateTime")).show(false)
//    data.withColumn("newDate", split($"dateRanges", ",")(0).cast("dateTime")).printSchema()

  }

  test("time"){

    import spark.implicits._
    val d1 = spark.sparkContext.parallelize(Seq("20170501", "20170401", "20170901", "20170601")).toDF("date")


//    d1.withColumnRenamed("date", "dateNew").show
//    d1.withColumn("datenew", $"date").show()
//
//    d1.select(($"date").alias("newData")).show

    val expr = substring($"date", 5,2) //+ "" +substring($"date", 4,6) + ""+ substring($"date", 6,8)



    val data = d1.withColumn("date", expr)

    data.show

//    data.show()
//    val result = data.withColumn("newDate", to_utc_timestamp($"date", "YYYY-mm-dd").cast("timestamp"))
//
//    result.printSchema()
//    result.show()
//
//    result.withColumn("newDate", date_sub($"newDate", 30)).show


//    result.withColumn("newDate", date_sub($"newDate", 30)).printSchema()






  }

  test ("test array "){

    import spark.implicits._

    val data = spark.sparkContext.parallelize(Seq(123f, 125f, Float.NaN)).toDF("column2")

//    data.withColumn("column3", array(($"column2"))).printSchema()


    val isNaN = udf((value : Float) => {
      if (value.equals(Float.NaN) || value == null) true else false
    })
    val result = data.filter(isNaN(data("column2"))).count()

    println("*********** " + result)

  }


  test ("test values "){
    val rdd=spark.sparkContext.parallelize(0 to 10,1)
    val last=rdd.sortBy(x=>{x},false,1).first()
//    println(last)

//    println(rdd.top(1).toString)
//    rdd.top(1).foreach(println)

    val order = spark.sparkContext.parallelize(Seq(
        (("2014-04-18 00:00:00.0","PENDING"),9),
        (("2014-04-18 00:00:00.0","ON_HOLD"),11),
        (("2013-09-17 00:00:00.0","ON_HOLD"),8),
        (("2014-07-10 00:00:00.0","COMPLETE"),57)
    ))

    val result = order.countByKey().toSeq

    result.sortBy(_._1).foreach(println)

//    List ("hello,i,am,shankar", "hello,i,am,shankar").map(_.split(",")).filter(_(2).equals("am")).foreach(x=>x.foreach(println))

  }


  test ("test cast "){
    import spark.implicits._

    val data = spark.sparkContext.parallelize(Seq(
      ("om", "scaka", "120"),
      ("daniel", "spark", "80"),
      ("3754978", "spark", "1")
    )).toDF("user", "topic", "hits")

//    data.withColumn("hits", $"hits".cast("integer")).printSchema()
//    data.withColumn("hits", data("hits").cast("integer")).show

//    val df2 = data.selectExpr ("user","topic","cast(hits as int) hits")
//    val df1 = data.selectExpr ("user","topic","cast(hits as int) hits")

    val df2 = data.withColumn("hitsTmp",
      data("hits").cast(IntegerType)).drop("hits").
      withColumnRenamed("hitsTmp", "hits")

//    df2.show()
//    df2.printSchema()


    data.createOrReplaceTempView("test")
    val dfNew = spark.sql("select *, cast('hist' as integer) as hist2 from test")
    dfNew.show()
    dfNew.printSchema()

  }



  test ("test abc ") {
    import spark.implicits._

    val data = spark.sparkContext.parallelize(Seq(
      ("o,m", "scaka", "1,2,0"),
      ("da,niel", "spark", "8,0"),
      ("375,4978", "spark", "1,9,9")
    )).toDF("user", "topic", "hits")


//    val tolong = udf((value : String) => value.split(",")map(_.toLong))

    data.withColumn("newCol", split(data("hits"), ",").cast("array<long>")).printSchema()
    data.withColumn("newCol", split(data("hits"), ",").cast("array<long>")).show


  }


  test ("test split"){
    import spark.implicits._
    val data = spark.sparkContext.parallelize(Seq(
      ("awer.ttp.net","Code", 554),
      ("abcd.ttp.net","Code", 747),
      ("asdf.ttp.net","Part", 554),
      ("xyz.ttp.net","Part", 747)
    )).toDF("A","B","C")

//    data.withColumn("D", split($"A", "\\.")(0)).show(false)
//
//    data.selectExpr("SUBSTRING_INDEX(A, '.', 1) as D ").show
//    data.createOrReplaceTempView("tempTable")
//    data.sqlContext.sql("SELECT A, B, C, SUBSTRING_INDEX(A, '.', 1) as D  from tempTable").show

      data.withColumn("key", $"dcId".cast("string"))
          .select(to_json(struct(data.columns.head, data.columns.tail:_*)).as("value")).show()

  }

  test ("test join" ){
    import spark.implicits._
    val a = spark.sparkContext.parallelize(Seq(
      (Some(3), 33),
      (None, 11),
      (Some(2), 22)
    ))//.toDF("id", "value1")

//    a.printSchema()

    val q = spark.sparkContext.parallelize(Seq(
      (Some(3), 33)
    ))//.toDF("id", "value2")

//    q.join(a, a("id") === q("id") , "leftouter").show
//    q.leftOuterJoin(a)



    val rdd = spark.sparkContext.parallelize(Seq(
      ( "z287570731_serv80i:7:175" , "5" ),
    ( "p286274731_serv80i:6:100" , "138" ),
    ( "t219420679_serv37i:2:50" , "5" ),
    ( "v290380588_serv81i:12:800" , "144:Jo" ),
    ( "z292902510_serv83i:4:45" , "5" )
    )).toDF("value", "id")

/*    rdd.map( row =>
      """""".r.findFirstIn(row._1)
    ).foreach(println)*/

    val B = rdd.filter($"id" === "5")

    val C = rdd.except(B)

    B.show
    C.show()
  }


  test ("error "){
    val i1 = spark.createDataFrame(Seq(("a", "string"), ("another", "string"), ("last", "one"))).toDF("a", "b")
    val i2 = spark.createDataFrame(Seq(("one", "string"), ("two", "strings"))).toDF("a", "b")
    val i1Idx = i1.withColumn("sourceId", lit(1))
    val i2Idx = i2.withColumn("sourceId", lit(2))
    val input = i1Idx.union(i2Idx)
    val weights = spark.createDataFrame(Seq((1, 0.6), (2, 0.4))).toDF("sourceId", "weight")
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    weights.join(input, "sourceId").show
//    weights.join(input, input("sourceId")===weights("sourceId"), "cross").show

//    weights.crossJoin(input).show

//    'inner', 'outer', 'full', 'fullouter', 'leftouter', 'left', 'rightouter', 'right', 'leftsemi', 'leftanti', 'cross'.





  }


  test("test array123"){

    import spark.sparkContext._

    val schema = StructType(Seq(StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("livesIn", StringType, true),
      StructField("bornIn", StringType, true)))
    val schemaFields = schema.fields
    // hardcoded for now. Need to read from Accumulo and plug it here
    val rec = List("KBN 1000000 Universe Parangipettai", "Sreedhar 38 Mysore Adoni", "Siva 8 Hyderabad Hyderabad",
      "Rishi 23 Blr Hyd", "Ram 45 Chn Hyd", "Abey 12 Del Hyd")

    // Reading from Accumulo done. Constructing the RDD now for DF.
    val rdd = spark.sparkContext.parallelize(rec)


    val rows = rdd.map( value => {
      val data = value.split(" ")
      Row(data(0), data(1).toInt, data(2), data(3))
    })
    val df = spark.createDataFrame(rows, schema)

    df.show

    df.select("livesIn").show


  }

  test ("groupbykey"){

    import spark.implicits._
    val rdd = spark.sparkContext.parallelize(Seq(
        (1, Array(2.0,5.0,6.3)),
        (5, Array(1.0,3.3,9.5)),
        (1, Array(5.0,4.2,3.1)),
        (2, Array(9.6,6.3,2.3)),
        (1, Array(8.5,2.5,1.2)),
        (5, Array(6.0,2.4,7.8)),
        (2, Array(7.8,9.1,4.2))
      )
    )

    val distinct = rdd.map(v => ( v._1, 1) ).distinct()

//    rdd.map(v => v._1)

    distinct.join(rdd).map(v => (v._1, v._2._2)).toDF.show(false)

//    rdd.groupBy( row => row._1).foreach(println)

//    rdd.groupBy( row => row._1).mapValues(r=> r.map(value => (value._1, value._2))).foreach(println)


  }

  test("test join multiple "){


    val subSchema = StructType(Seq(
      StructField("C", StringType, true),
      StructField("EID", LongType, true),
      StructField("HST", StringType, true)
    ))


    val schema = StructType(Seq
      (StructField("E", ArrayType(subSchema), true),
      StructField("BL", StringType, true),
      StructField("EventStartDate", StringType, true)
      ))


    import spark.implicits._
    val t1 = spark.sparkContext.parallelize(Seq(
      (List(("a", 123l, "b"),("a", 456l, "c"),("a", 789l,"d")), "xyz", "event123"),
      (List(("b", 123l, "b"),("b", 456l, "c"),("b", 789l,"d")), "xyz123", "event234"),
      (List(("c", 123l, "b"),("c", 456l, "c"),("c", 789l,"d")), "xyz123", "event123")
    )).toDF().rdd

    val df = spark.createDataFrame(t1, schema)

//    df.show(false)
//
//    df.select("EventStartDate", "E").show(false)


    val data = spark.sparkContext.parallelize(Seq(
      ("18kPq7GPye-YQ3Ly", List("rpOyqD_893cqmDAt", "18kPq7GPye-YQ3Ly")),
      ("4U9kSBLuBDU391x6b", List("18kPq7GPye-YQ3Ly", "18kPq7GPye-YQ3Ly"))
    )).toDF("user_id", "friends")

//    data.withColumn("friends", explode($"friends")).show(false)


    val a = Array(12, 6, 15, 2, 20, 9)
    a.reduceLeft((x,y) => {
      println(s"x and y are =   ${x}   ${y}")
      x + y
    })

    a.reduceRight((x,y) => {
      println(s"x and y are =   ${x}   ${y}")
      x + y
    })



  }





}
