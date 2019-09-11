package com.sapient

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object rddPractice extends App {

  val conf = new SparkConf().setAppName("Practice").setMaster("local[*]")
  val sc =  new SparkContext(conf)

  /*Setting Spark home directory */
  System.setProperty("hadoop.home.dir", "C:\\hadoop")
  /* Setting system properties and log level*/
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

        val rdd1 = sc.textFile("file:///C:\\Users\\vishn\\Desktop\\Hadoop\\Input\\Data1.csv")

          val header = sc.parallelize(rdd1.take(0))
          val rdd2 = rdd1.subtract(header)
        val rdd3 =   rdd2.map(line => line.split(",")).map( a =>(a(0).toString,a(1).toString,a(2).toString,a(3).toString))
          rdd3.repartition(1).saveAsTextFile("file:///C:\\Users\\vishn\\Desktop\\Hadoop\\Input\\out.csv")
}
