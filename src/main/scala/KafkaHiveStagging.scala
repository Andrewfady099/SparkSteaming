package sample
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions.{col, current_timestamp, expr, split}

import java.time.{LocalDateTime, ZoneId, ZoneOffset}

object KafkaHiveStagging {
  var spark: SparkSession = _
  import org.apache.spark.sql.SparkSession
  import java.time.Instant
  import org.apache.spark.sql.Encoders
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = SparkSession.builder()
      .appName("Insert into Hive Table")
      .config("hive.metastore.uris", "thrift://172.25.0.100:9083")
      //.config("spark.sql.warehouse.dir", "hdfs://172.25.0.101:9000/user/hive/warehouse")
      .enableHiveSupport()
      .master("local")
      .getOrCreate()
    spark.conf.set("spark.sql.repl.eagerEval.enabled", true)

    def getMinute(): Int = {
      val instant = Instant.now
      // convert the instant to a local date time of your system time zone
      LocalDateTime.ofInstant(instant, ZoneId.systemDefault).getMinute
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
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "transactions")
      .option("startingOffsets", "latest") // From starting f f
      .load()
    val valuetostring = df.withColumn("value", expr("CAST(value AS STRING)"))
    var record=valuetostring
      .withColumn("TransactionId", split(col("value"), ",").getItem(0))
      .withColumn("isRequest", split(col("value"), ",").getItem(1))
      .withColumn("eventTime", split(col("value"), ",").getItem(2))
      .withColumn("currTime", current_timestamp())
      .withColumn("year", year(col("currTime")))
      .withColumn("month", month(col("currTime")))
      .withColumn("day", dayofmonth(col("currTime")))
      .withColumn("hour", hour(col("currTime")))
      .withColumn("minute", minute(col("currTime")))
      .select("TransactionId" ,"isRequest", "eventTime" , "currTime","year",  "month", "day", "hour", "minute")
    record.printSchema()
    //    spark.sql("CREATE TABLE transactionshive (  TransactionId string,  isRequest string,  eventTime string\n)\nPARTITIONED BY (year INT, month int, day int, hour int, minute int)\nROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'\nSTORED AS TEXTFILE")
    record.writeStream.foreachBatch((df: DataFrame, batchId: Long) =>{
      df.write
        .partitionBy("year", "month", "day", "hour", "minute")
        .mode(SaveMode.Append)
        .saveAsTable("transactions2")

      //      println(batchId)
      //          spark.sql("SELECT * FROM Transactions2 ").show()

      val currentMinute = minute(current_timestamp())
      val currentDay = dayofmonth(current_timestamp())
      val currentYear = year(current_timestamp())
      val currentMonth = month(current_timestamp())
      val currentHour = hour(current_timestamp())
      spark.sql("SELECT * FROM transactions2 WHERE minute = minute(current_timestamp())" +
        "AND day = dayofmonth(current_timestamp())" +
        "AND hour = hour(current_timestamp())" +
        "AND month = month(current_timestamp())").show()
      //      val partitionFilter = s"year = ${currentYear} AND month = ${currentMonth} AND day = ${currentDay} AND hour = ${currentHour} AND minute BETWEEN ${currentMinute - 4} AND ${currentMinute - 1}"
      //      val count = spark.sql(s"SELECT SUM(cnt) FROM (SELECT COUNT(*) as cnt FROM transactions2 WHERE $partitionFilter GROUP BY year, month, day, hour, minute) t").collect()(0)(0).asInstanceOf[Long]
      //      println(s"Number of rows in transactions2: $count")
      spark.sql(s"Select count (*) from transactions2 WHERE hour=$getHour AND minute BETWEEN ${currentMinute - 4} AND ${currentMinute - 1}").show()
      //          val selected = table.filter(col("day") === currentDay && col("minute") === currentMinute && col("hour") === currentHour)
      //          val numFiles = selected.inputFiles.length
      //      val numPartitions = df.rdd.getNumPartitions
      //      println(numPartitions)
      //      println(numFiles)
      //      table.filter(col("minute") === currentMinute && col("day") === currentDay).select("*").show(1000, false)
    }).start()
      .awaitTermination()


  }
}
