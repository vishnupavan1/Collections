package com.sapient

import java.text.SimpleDateFormat

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import java.util.Calendar._
object SparkPractice extends App {

    /*Setting Spark home directory */
   System.setProperty("hadoop.home.dir", "C:\\hadoop")
  /* Setting system properties and log level*/
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val now = java.util.Calendar.getInstance().getTime
  val yr_format = new SimpleDateFormat("YYYYMMDD")
    println("Start time is      :"+now)

  val  spark = SparkSession.builder().appName("Sapient Big data").master("local[*]").getOrCreate()

  val schema = StructType(Array(StructField("name",StringType,true),
    StructField("Age",IntegerType,true),
    StructField("City",StringType,true),
    StructField("ST_DATE",DateType,true),
    StructField("_corrupt_record",StringType,true)
    ))

  val df = spark.read.
    format("csv")
    .option("delimiter",",")
    .schema(schema)
    .option("header","true")
    .load("file:///C:\\Users\\vishn\\Desktop\\Hadoop\\Input\\Data1.csv")
    .cache()

  import spark.implicits._

    val bad_rec = df.filter(col("_corrupt_record").isNotNull).select("_corrupt_record")
        val bad1 =  bad_rec.rdd.map(line => line.getString(0))
        val bad2 = bad1.map( line => line.split(",")).map( a => (a(0),a(1),a(2),yr_format.parse(a(3)) ))
  bad2.collect().foreach(println)

  df.show()

        import  spark.implicits._


    //  df.filter(col("_corrupt_record").isNotNull).show()
      spark.stop()

  println("End time is :           "+java.util.Calendar.getInstance().getTime)
}
