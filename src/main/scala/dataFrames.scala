package sample
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions.{col, current_timestamp, expr, split}
import java.time.LocalDateTime
import java.time.ZoneId

import java.time.Instant


object dataFrames {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("Staging")
      .config("spark.sql.hive.lazyInit", "true")
      .enableHiveSupport()
      .master("local")
      .getOrCreate()
    spark.conf.set("spark.sql.repl.eagerEval.enabled", true)

    val storeDF=spark.read.format("json").load("/home/ejada/Downloads/store_locations.json")
    storeDF.show(3)
    storeDF.printSchema()
    storeDF.createOrReplaceTempView("storeDFView")
    spark.sql(
      """
        SELECT *
        FROM storeDFView
        """).show()
  }
}