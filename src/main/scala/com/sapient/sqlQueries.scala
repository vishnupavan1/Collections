package com.sapient

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dayofmonth, lag, month, unix_timestamp, when}

object sqlQueries extends  App {

  /*Building Spark Session*/
  val  spark = SparkSession.builder().appName("Sapient Big data").master("yarn")
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .enableHiveSupport()
    .getOrCreate()

  /*Reading data from hive table */
  val clickData = spark.sql("select * from vishnupavan.session")

  /*Processsing the data using dataframes*/

  val wspec = Window.partitionBy("userid","Day","Month").orderBy(col("userid"),col("Timestamp"))

  val clickStream2 = clickData.withColumn("day", dayofmonth(col("Timestamp")))
    .withColumn("month", month(col("Timestamp")))
    .withColumn("Previous_Timestamp", lag("Timestamp", 1, "0") over (wspec))
    .withColumn("TimeDiff", when(col("Previous_Timestamp").isNotNull,
      unix_timestamp(col("Timestamp"), "YYYY-MM-DD HH:MM:SS") - unix_timestamp(col("Previous_Timestamp"), "YYYY-MM-DD HH:MM:SS")).otherwise("0").cast("Integer"))

  val clickStream3 = clickStream2.select("Timestamp","userid","session_id","TimeDiff","month","day")

  /*Desigining hive table */
    spark.sql("CREATE TABLE if not exists vishnupavan.session1(\n  `Timestamp` timestamp,\n  `userid` string,\n  `session_id` string,\n  `TimeDiff` int ) \nPARTITIONED BY (\n  Month int,\n  Day int)\nROW FORMAT SERDE\n  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'\nSTORED AS INPUTFORMAT\n  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'\nOUTPUTFORMAT\n  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'")
  /*Loading data into table */

  clickStream3.write.format("parquet").partitionBy("month","day").mode("append").save("/apps/hive/warehouse/vishnupavan.db/session1/")

      /*Sync partitions to hive table */

  spark.sql("msck repair table vishnupavan.session1")

    /*spark sql queries to analyse data */

  spark.sql("select * from vishnupavan.session1 ").show()

  spark.sql(" select day,sum(max_sessions) as total_session_per_day from" +
    " (select userid,substring(max(session_id),8,1) as max_sessions,day,month from vishnupavan.session1 " +
    "group by day,month,userid)" +
    "group by day,month ").show()

  /*Total time spent by a user in day*/

  spark.sql("select userid,sum(timediff) as total_time_per_day from vishnupavan.session1 "  +
    "group by userid,day,month").show()

  /* Total time spent by a user in a month*/

  spark.sql("select userid,sum(timediff) as total_time_per_month from vishnupavan.session1 "  +
    "group by userid,month").show()

    spark.stop()


}
