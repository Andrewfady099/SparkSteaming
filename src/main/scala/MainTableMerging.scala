package sample
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions.{col, current_timestamp, expr, split}
import java.time.LocalDateTime
import java.time.ZoneId

import java.time.Instant


object MainTableMerging {

  def getYear(): Int = {
    val instant = Instant.now
    // convert the instant to a local date time of your system time zone
    LocalDateTime.ofInstant(instant, ZoneId.systemDefault).getYear
  }

  def getMonth(): Int = {
    val instant = Instant.now
    // convert the instant to a local date time of your system time zone
    LocalDateTime.ofInstant(instant, ZoneId.systemDefault).getMonthValue
  }

  def getDay(): Int = {
    val instant = Instant.now
    // convert the instant to a local date time of your system time zone
    LocalDateTime.ofInstant(instant, ZoneId.systemDefault).getDayOfMonth
  }

  def getHour(): Int = {
    val instant = Instant.now
    // convert the instant to a local date time of your system time zone
    LocalDateTime.ofInstant(instant, ZoneId.systemDefault).getHour
  }

  def getMinute(): Int = {
    val instant = Instant.now
    // convert the instant to a local date time of your system time zone
    LocalDateTime.ofInstant(instant, ZoneId.systemDefault).getMinute
  }

  def getSecond(): Int = {
    val instant = Instant.now
    // convert the instant to a local date time of your system time zone
    LocalDateTime.ofInstant(instant, ZoneId.systemDefault).getSecond
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("Staging")
      .config("hive.metastore.uris", "thrift://172.25.0.100:9083")
      //.config("spark.sql.warehouse.dir", "/home/ejada/IdeaProjects/spark-warehouse")
      .config("spark.sql.hive.lazyInit", "true")
      .enableHiveSupport()
      .master("local")
      .getOrCreate()
    spark.conf.set("spark.sql.repl.eagerEval.enabled", true)

    while (true) {
      val q1 =
        s"""SELECT TransactionId,isRequest,
                year(eventTime) AS year ,
                month(eventTime) AS month,
                day(eventTime) AS day,
                hour(eventTime) AS hour
                FROM transactions2 WHERE
                year=$getYear AND
                month=$getMonth AND
                day=$getDay     AND
                hour=$getHour   AND
                minute BETWEEN $getMinute-8 AND $getMinute-3"""
      val query = spark.sql(q1)

      query.write
        .partitionBy("year", "month", "day", "hour")
        .mode(SaveMode.Append)
        .saveAsTable("Trial2")

      val currentMinute = minute(current_timestamp())
      for (i <- 3 to 8) {
        spark.sql(s"ALTER TABLE transactions2 DROP IF EXISTS PARTITION (year='$getYear', month='$getMonth', day='$getDay', hour='$getHour', minute = ${getMinute()-i})")
      }
      //
      //      val trial2Rows = spark.sql(s"Select count(*) from trial2 WHERE hour=$getHour ").show()
      //      val transactions2Rows = spark.sql(s"Select count (*) from transactions2 WHERE hour=$getHour AND minute BETWEEN ${currentMinute - 4} AND ${currentMinute - 1}").show()
      //        println("Equality")
      //        println(trial2Rows)
      //        println(transactions2Rows)

      Thread.sleep(300000)
    }



  }
}