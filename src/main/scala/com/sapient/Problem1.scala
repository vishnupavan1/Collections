package com.sapient

import org.apache.spark.sql.SparkSession

object Problem1 extends App {
  val  spark = SparkSession.builder().appName("Sapient Big data").master("local[*]").getOrCreate()

  val df = spark.read.format("csv").option("delimiter","\t").option("inferSchema","true").option("header","true").load("file:/home/vishnupavan/Data.csv")

  /*dopping duplicates based on name and age */

  df.dropDuplicates("Name","Age").show()
    spark.stop()

}



