package sample

import org.apache.log4j.{Level, Logger}
import org.apache.parquet.format.IntType
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{pow, _}

import scala.io.Source

object kafkaRead {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("streaming")
      .master("local[*]")
      .getOrCreate()


    //    spark.conf.set("spark.streaming.checkpointLocation", "/home/ejada/Downloads/checkPoints")

    import spark.implicits._

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topic3")
      .option("startingOffsets", "latest") // From starting f f
      .load()

    df.printSchema()

    val valuetostring = df.withColumn("value", expr("CAST(value AS STRING)"))

    val schema = new StructType()
      .add("TransactionId", LongType)
      .add("isRequest", BooleanType)
      .add("eventTime", LongType)


    val jsontocols = valuetostring.withColumn("data", from_json(col("value"), schema))

    jsontocols.printSchema()

    //    jsontocols.select("data.*").writeStream
    //      .format("console")
    //      .outputMode("append")
    //      .option("truncate", "false")
    //      .option("checkpointLocation", "/home/ejada/Downloads")
    //      .start()
    //      .awaitTermination()


    val query = jsontocols.select("value")
      .writeStream
      .format("csv")
      .queryName("query_v2")
      .option("path", "/home/ejada/Downloads/topics")
      .option("checkpointLocation", "/home/ejada/Downloads/checkPoints")
      .outputMode("append")
      .start()

//    val windowedCounts = jsontocols
//      .selectExpr("CAST(data.registertime AS TIMESTAMP) as timestamp", "data.userid")
//      .groupBy(
//        window(col("timestamp"), "20 seconds"),
//        col("userid")
//      )
//      .count()
//      .writeStream
//      .queryName("windowedCounts_v2")
//      .outputMode("complete")
//      .format("console")
//      .option("checkpointLocation", "/home/ejada/Downloads/checkPoints2")
//      .start()

    query.awaitTermination()
  //  windowedCounts.awaitTermination()


  }

}
