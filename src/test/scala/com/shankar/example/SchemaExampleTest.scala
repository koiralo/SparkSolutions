package com.shankar.example

import java.io.File
import java.nio.file.{Files, Paths}
import java.sql.Date
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, Month}

import org.apache.commons.net.ntp.TimeStamp
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.joda.time.Days
import org.junit.experimental.theories.DataPoint
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.collection.mutable.ListBuffer

/**
  * Created by sakoirala on 7/5/17.
  */
class SchemaExampleTest extends FunSuite with BeforeAndAfterEach{

  def test123 (abc: DataFrame) = {}

  val spark = SparkSession.builder().master("local").getOrCreate()
  test ("schema example ") {

    import spark.implicits._

    val data = spark.sparkContext.parallelize(Seq(
      ("City 3","6/30/2017 16:04", 28),
    ("City 4","7/4/2017 16:04", 12),
    ("City 2","7/13/2017 16:04", 8),
    ("City 4","7/16/2017 16:04",  21),
    ("City 4","7/3/2017 16:04",  24),
    ("City 2","7/17/2017 16:04",  34),
    ("City 3","7/9/2017 16:04",  13),
    ("City 3","7/18/2017 16:04",  26),
    ("City 3","7/6/2017 16:04",  16),
    ("City 3","7/15/2017 16:04",  29),
    ("City 4","7/18/2017 16:04",  39),
    ("City 2","7/1/2017 16:04",  19),
    ("City 2","7/18/2017 16:04", 19),
    ("City 4","7/4/2017 16:04",  24),
    ("City 2","7/4/2017 16:04",   9),
    ("City 4","7/15/2017 16:04",  20),
    ("City 3","7/12/2017 16:04",  19),
    ("City 1","7/9/2017 16:04",  13),
    ("City 1","7/13/2017 16:04",  25),
    ("City 4","7/10/2017 16:04",  10)
    )).toDF("City", "TimeStamp", "Sale")



//    data.groupBy("City", "TimeStamp").agg(count(col("Sale")).as("TotalSale")).show

    val df = spark.sparkContext.parallelize(Seq("-1", "12", "18", "28", "38", "38", "388", "3", "41")).toDF("Age")

   /* df.show()


    val scrubUdf = udf((value : String ) => {
      value match {
        case "NaN"  => 0
        case "null" => 999
        case null   => 999
        case x if x.contains("-") => {
          //          (value.split("-")(0).toInt + value.split("-")(1).toInt) / 2
          x.split("-").map(x=> x.toInt).sum / 2
        }
        case x if x.toInt >= 200 => 999
        case _ => value.toInt
      }
    })
*/

    val updateUDF = udf((age : String) => {
      val range = Seq(
        (-1, 12, "(-1 - 12)"),
        (12, 17, "(12 - 17)"),
        (17, 24, "(17 - 24)"),
        (24, 34, "(24 - 34)"),
        (34, 44, "(34 - 44)"),
        (44, 54, "(44 - 54)"),
        (54, 64, "(54 - 64)"),
        (64, 10, "(64 - 100)"),
        (100, 1000, "(100- 1000)")
      )
     range.map( value =>  {
       if (age.toInt >= value._1 && age.toInt < value._2) value._3
      else  ""
     }).filter(!_.equals(""))(0)

    })



    df.withColumn("Age-Range", updateUDF($"Age")).show(false)







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
//    val new_class = new ABC
//    new_class.demo(rdd)
//    new_class.test()
//    new_class.print()

  }

  test ("test list "){


    val age = 50
    val range = Seq(
      (-1, 12, "(1 - 12)"),
      (12, 17, "(12 - 17)"),
      (17, 24, "(17 - 24)"),
      (34, 44, "(34 - 44)"),
      (44, 54, "(44 - 54)"),
      (54, 64, "(54 - 64)"),
      (64, 10, "(64 - 100)"),
      (100, 1000, "(100- 1000)")
    )

    val result = range.map( value =>  if (age.toInt >= value._1 && age.toInt < value._2) value._3 else  "").filter(a => !(a == ""))(0)

    println ("result === " + result)


    val rangeList = List(-1, 12, 17, 24, 34, 44, 54, 64, 100, 1000)
    val ranges = rangeList.sliding(2).map((list => AgeRange(list(0), list(1)))).toList

    rangeList.sliding(2).foreach(println)


    range.foreach(println)


  }
  case class AgeRange(lowerBound: Int, upperBound: Int) {
    def contains(value: Int): Boolean = value >= lowerBound && value < upperBound
  }

  test ("test window"){

    import spark.implicits._

    val data = spark
      .createDataFrame(
        Seq[(String, Int)](
          ("A", 1),
          ("A", 2),
          ("A", 3),
          ("B", 10),
          ("B", 20),
          ("B", 30)
        ))
      .toDF("name", "count")


    val firstLast = data.groupBy("name").agg(first("count").as("firstCountOfName"), last("count").as("lastCountOfName"))

    val result = data.join(firstLast, Seq("name"), "left")

    result.show()
  }


  test ("zip with index"){

    import spark.implicits._
   val df1 = spark.sparkContext.parallelize(11 to 20, 2).toDF("df1")
    val df2 = spark.sparkContext.parallelize((1 to 10), 2).toDF("df2")

    df1.join(df2, $"df1", "left").where( $"id".isNotNull  && $"id" === "")

/*    df1.withColumn("id", monotonically_increasing_id())
      .join(df2.withColumn("id", monotonically_increasing_id()), "id")
      .withColumn("result", ($"df1" + $"df2")).show*/


    val df = spark.sparkContext.parallelize(Seq(
      (1, "Fname1", "Lname1", "Belarus"),
    (2, "Fname2", "Lname2", "Belgium"),
    (3, "Fname3", "Lname3", "Austria"),
    (4, "Fname4", "Lname4", "Australia")
    )).toDF("id", "fname","lname", "country")


    val result = df.withColumn("countryFirst", split($"country", "")(0))

    result.write.partitionBy("countryFirst").format("com.databricks.spark.csv").save("outputpath")


  }

  test ("read files "){
    import spark.implicits._
    val data = spark.sparkContext.parallelize(Seq(
      (29,"City 2", 72),
      (28,"City 3", 48),
      (28,"City 2", 19),
      (27,"City 2", 16),
      (28,"City 1", 84),
      (28,"City 4", 72),
      (29,"City 4", 39),
      (27,"City 3", 42),
      (26,"City 3", 68),
      (27,"City 1", 89),
      (27,"City 4", 104),
      (26,"City 2", 19),
      (29,"City 3", 27)
    )).toDF("week", "city", "sale")


    val city = data.select("city").distinct.collect().flatMap(_.toSeq)

    val result = city.map(c => (c -> data.where(($"city" === c))))

    result.foreach(a => {
      println(s"Dataframe with ${a._1}")
      a._2.show()
    })


    val getDateFromWeek = udf((week : Int) => {
      val week1 = LocalDate.of(2016, 12, 30)
      val day = "Friday"
      val result = week1.plusWeeks(week).format(DateTimeFormatter.ofPattern("MM/dd/yyyy"))
      s"${day} (${result})"
    })

//    data.withColumn("day", getDateFromWeek($"week")).show


  }

  test ("average value "){

    import spark.implicits._
    val data = spark.sparkContext.parallelize(Seq(
      (1,"1011","06/25/2003",4,4,1.20),
      (2,"1000","06/25/2003",2,3,2.40),
      (1,"1011","06/25/2003",1,3,5.40),
      (1,"1021","06/25/2003",1,1,2.10)
    )).toDF("a", "timeStamp", "list","rid","sbid","avgvalue")

//    data.groupBy("a", "timeStamp").agg(max($"avgvalue").alias("maxAvgValue")).show

//    val result = data.
//      withColumn("list", unix_timestamp($"list", "MM/dd/yyyy").cast(TimestampType))
//      .withColumn("dayofyear", dayofyear($"list")).show


  val df1 = spark.read.json("/home/sakoirala/IdeaProjects/SparkSolutions/src/main/resources/data.json")
    df1.printSchema()
    df1.show()
  }

  test (" test pivot "){

    import spark.implicits._
    val data = spark.sparkContext.parallelize(Seq(
      ("c1", "JAN-2017", 49),
    ("c1", "FEB-2017", 46),
    ("c1", "MAR-2017", 83),
    ("c2", "JAN-2017", 59),
    ("c2", "MAY-2017", 60),
    ("c2", "JUN-2017", 49),
    ("c2", "JUL-2017", 73)
    )).toDF("city", "month", "sales")

    val fullDate = udf((value :String )=>
    {
      val months = List("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")
      val splited = value.split("-")
      println("**** " + splited(1))
      println(new Date(2017, months.indexOf(splited(0)) + 1, 1))
      println(new Date(2017, 1, 32))
      new Date(2017, 1, 1)

    })

//    val df = data.withColumn("month", fullDate($"month"))

    val window = Window.partitionBy("city").orderBy("city")


    val checkPrevMonth = udf((prev: String , current : String, value :String ) =>{
      val months = List("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")
      println(s"${prev}    ${current}     ${value}")
      val pSplit = prev.split("-")
      val cSplit = current.split("-")
      val exp = if (cSplit == "DEC")
        months(months.indexOf("DEC"))
      else
        months(months.indexOf(cSplit(0))-1)

      if (pSplit(0) == exp)  value else null



    })



    val result = data.withColumn("abc", lag($"sales", 1, null).over(window))

//    result.withColumn("newSale", checkPrevMonth(lag($"sales", 1, null).over(window), $"month", $"abc")).show


    println(('A' to 'Z').mkString("")+ "\nNow I know the alphabet!")




/*
    val df1 = df.withColumn("id", monotonically_increasing_id())

    val df2 = df.withColumn("id", monotonically_increasing_id())

//    val result = df1.join(df2, df1("id") === df2("id") + 1, "left").show()*/


    val ddf1 = Seq(List(2,3,1), List(6,4,3)).toDF("marks")

    val result12 = ddf1.rdd.map(r=> r(0)).collect()

    result12.foreach(println)
    val testUdf = udf((list: Seq[Int]) => {
      val ascending = list.sorted
      s"${ascending(0)} - ${ascending(ascending.size - 1)}"
    })

    ddf1.withColumn("marks", testUdf($"marks")).show


    ddf1.createOrReplaceTempView("test")
    ddf1.registerTempTable("test")




  }


  test("test udf 123 "){
    import spark.implicits._
    val df1 = spark.sparkContext.parallelize(Seq(
      ( 1, 3, 1),
      (1, 8, 2),
      (1, 5, 3),
      (2, 2, 1)
    )).toDF("A", "B", "C")

//    df1.show
    val format = udf((value : Seq[String]) => {
      value.sortBy(x => {x.split(",")(0)}).mkString("/")
    })


    val result = df1.withColumn("B", concat_ws(",", $"B", $"C"))
      .groupBy($"A").agg(collect_list($"B").alias("B"))
      .withColumn("B", format($"B"))



    result.show()

  }


  test ("join rdd"){

    import spark.implicits._

    val df1 = spark.sparkContext.parallelize(Seq(1,2,3,4,5)).toDF("A")
    val df2 = spark.sparkContext.parallelize(Seq("a", "b", "c", "d", "e")).toDF("B")

    val combinedRow = df1.rdd.zip(df2.select("B").rdd). map({
      case (df1Data, df2Data) => {
        Row.fromSeq(df1Data.toSeq ++ df2Data.toSeq)
      }
    })
    val combinedschema =  StructType(df1.schema.fields ++ df2.select("B").schema.fields)
    val resultDF = spark.sqlContext.createDataFrame(combinedRow, combinedschema)

    resultDF.show

    df1.withColumn("id", monotonically_increasing_id())
      .join(df2.withColumn("id", monotonically_increasing_id()), "id").drop("id").rdd




  }


  test ("read files 1234 "){

    import spark.implicits._
    val df = Seq((1, Some("z")), (2, Some("abs,abc,dfg")),(3,Some("a,b,c,d,e,f,abs,abc,dfg"))).toDF("id", "text")

    val  textList = "abs,abc,dfg"

    val valsum = udf((text: String, list: String) => {
      list.split(",").intersect(text.split(",")).size
    })

    df.withColumn("new", split($"text", ",")).show
    df.withColumn("new", split($"text", ",")).printSchema()

    df.withColumn("count" , valsum($"text", lit(textList))).show


  }

  test ("test json 123"){
    import spark.implicits._
/*    val input = "/home/sakoirala/IdeaProjects/SparkSolutions/src/main/resources/complex.json"

    val data = spark.read.option("schema", true).json(input)

    val data1 = data.withColumn("bat" , explode($"batters.batter"))

    val result = data1.groupBy("id").pivot("bat")*/


    val df = Seq("00:06:27", "01:11:40", "00:30:14").toDF("Fc_0004")
    df.withColumn("Fc_0004", unix_timestamp(df("Fc_0004"), "HH:mm:ss").cast(TimestampType)).
      withColumn("Fc_00041", to_utc_timestamp($"Fc_0004", "HH:mm:ss")).printSchema()

    import org.apache.spark.sql.Row

    val myArray = Array("1499955986039", "1499955986051", "1499955986122")

    val myrdd = spark.sparkContext.parallelize(myArray.toSeq).toDF().rdd


    val df123 = spark.createDataFrame(myrdd, StructType(Seq(StructField("myTymeStamp", StringType,true))))

    val newDf = df123.withColumn("myTymeStamp", $"myTymeStamp".cast(LongType).cast(TimestampType))
    df123.withColumn("myTymeStamp", unix_timestamp($"myTymeStamp")).show
    newDf.printSchema()
    newDf.show()

  }

  test("test last modified"){
    val directory = new File("/home/sakoirala/IdeaProjects/SparkSolutions/src/main/resources")
    val allFiles = directory.listFiles.filter(_.isFile).sortBy(-_.lastModified()).toList

    val latestFile = allFiles(0)


    import spark.implicits._
    val df = Seq(("596d799cbc6ec95d", List(1.0,2.0)), ("596d799cbc6ec95d", List(1.0,2.0, 3.0))).toDF("id", "a1")

    df.where(array_contains($"a1", 3.0)).show

    val filterUdf = udf((list : Seq[Double])=>{
      list.sorted == List(1.0, 2.0).sorted
    })

    df.filter(filterUdf($"a1")).show

//    df.where( $"a1" == array(List(1.0,2.0).map(lit(_))))


    val df1 = Seq(
      ("8:00:00 AM", "AAbbbbbbbbbbbbbbbb", "12/9/2014", 1, 0),
      ("8:31:27 AM", "AAbbbbbbbbbbbbbbbb", "12/9/2014", 1, 0)
    ).toDF("Time","address", "Date","value" ,"sample")



    val df2 = Seq(
      ("8:45:00 AM", "AAbbbbbbbbbbbbbbbb","12/9/2016", 5,0),
     ("9:15:00 AM", "AAbbbbbbbbbbbbbbbb","12/9/2016", 5,0)
    ).toDF("Time","address", "Date","value" ,"sample")

    df1.union(df2).show()

  }

  test ("test schema "){
    import spark.implicits._

    val file2 = spark.sparkContext.textFile("/home/sakoirala/IdeaProjects/SparkSolutions/src/main/resources/person.txt")
    val mapper = file2.map(x => x.split(",",-1))
    val result = mapper.map(x => x.map(x => if(x.isEmpty) 0 else x).mkString(","))






  }

  test("stat correlation matrix "){
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val df1 = Seq((1,2,3), (3,4,5), (1,2,4)).toDF("A", "B", "C")

    val funs: List[(String => Column)] = List(min, max, sum)
    val exprs = df1.columns.flatMap(c => funs.map(f => f(c))).toList

    val sums = df1.columns.map(y => sum(y))

//    df1.select(Seq("A", "B").map(mean(_)): _*).show()

//    df1.agg(sum(df1.columns.head, df1.columns.tails: _*)).show


//    val columnNames = List("A", "B", "B")
//    df1.groupBy().sum(df: _*).show



//    df1.agg(lit(1).alias("temp"),exprs: _*).drop("temp").show

//    val sums = df1.columns.map(y => s"sum($y)").mkString(", ")
//    df1.createOrReplaceTempView("dftable")
//    spark.sql(s"select $sums from dftable").show()

//    val sums = for (field <- df1.columns) yield { sum(col(field)) }
//    df1.agg(sums : _*)

/*
      +-------+------------------+------------------+---+
      |summary|               min|               max|sum|
      +-------+------------------+------------------+---+
      |      A|                 1|                 3|  5|
      |      B|                 2|                 4|  8|
      |      C|                 3|                 5| 12|
      +-------+------------------+------------------+---+



*/

    val A = Seq(
      (List(0),1),
    (List(1),0),
    (List(2),1),
    (List(3),0),
    (List(4),0),
    (List(5),0),
    (List(6),1),
    (List(7),0),
    (List(8),0),
    (List(9),0),
    (List(10),0)
    ).toDF("nodes", "count")

    val B = Seq(
      (List(0), 1),
      (List(1), 0),
      (List(2), 3),
      (List(6), 0),
      (List(8), 2)
    ).toDF("nodes", "count")

    val DecimalType = DataTypes.createDecimalType(15,10)

    val sch = StructType(StructField("COL1",StringType,true)::StructField("COL2",DecimalType,true)::Nil)

    val src = spark.sparkContext.textFile("test_file.txt")
    val row = src.map(x=>x.split(",")).map(x=>Row(x(0), x(1).toDouble))




  }














}
case class testClass(date: String, time: String, level: String, unknown1: String, unknownConsumer: String, unknownConsumer2: String, vloer: String, tegel: String, msg: String, sensor1: String, sensor2: String, sensor3: String, sensor4: String, sensor5: String, sensor6: String, sensor7: String, sensor8: String, batchsize: String, troepje1: String, troepje2: String)

case class abc(a: Double, b: Double)
case class DataPoint(b: Int, c: Int)

case class PersonClass (
                  city: String,
                  state: String,
                  age: Int,
                  name: String
                  )

class myTest extends FunSuite {
  val sc = SparkSession.builder().master("local").appName("test").getOrCreate()
  var arrRDD = sc.sparkContext.parallelize(Array(1,1,1,1,1))

  test("custom test"){
    arrRDD.collect().foreach{
      x => assert (x==1)
    }
  }
}