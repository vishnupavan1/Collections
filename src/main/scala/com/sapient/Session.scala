package com.sapient

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object Session extends App {



          /*Building Spark Session*/
  val  spark = SparkSession.builder().appName("Sapient Big data").master("yarn").enableHiveSupport()
    .getOrCreate()

      /* Reading data from tab delimited file */
  val clickStream = spark.read.format("csv").option("delimiter","\t")
    .option("inferSchema","true")
    .option("header","true")
    .load("file:/home/vishnupavan/Data2.csv")

    /*Loading data into hive table */

    clickStream.write.format("parquet").mode("overwrite").saveAsTable("vishnupavan.clickstream")

    /*Reading data from hive table */
  val clickData = spark.sql("select * from vishnupavan.clickstream")


          /*Processsing the data using dataframes*/

  val wspec = Window.partitionBy("userid","Day","Month").orderBy(col("userid"),col("Timestamp"))

  val clickStream2 = clickData.withColumn("Day",dayofmonth(col("Timestamp")))
    .withColumn("Month",month(col("Timestamp")))
    .withColumn("Previous_Timestamp", lag("Timestamp",1,"0") over(wspec))

  val clickStream3 = clickStream2
    .withColumn("session",when(col("Previous_Timestamp").isNotNull,
      when(unix_timestamp(col("Timestamp"),"YYYY-MM-DD HH:MM:SS") - unix_timestamp(col("Previous_Timestamp"),"YYYY-MM-DD HH:MM:SS") >= 30 * 60 or
        unix_timestamp(col("Timestamp"),"YYYY-MM-DD HH:MM:SS") - unix_timestamp(col("Previous_Timestamp"),"YYYY-MM-DD HH:MM:SS") <= 120 * 70,"1")
        .otherwise("0")).otherwise("1"))
    .withColumn("Session_id",concat(lit("session"), (sum("session") over(wspec)).cast("Int")))
    .drop("session","Previous_Timestamp","Day","Month")


  /*writing into hive table */

    clickStream3.write.format("parquet").mode("overwrite").saveAsTable("vishnupavan.session")

  /* Stopping the spark session*/

          spark.stop()
}
