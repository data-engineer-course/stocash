package org.example

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

object App {
  private var TIME_SERIES_INTERVAL = 15

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    TIME_SERIES_INTERVAL = readTable(spark, "settings")
      .filter(col("key") === "interval_minutes")
      .select(col("value"))
      .first()
      .getString(0)
      .toInt

    // за прошедшие сутки
    val predicates = Array[String]("timestamp between today() - 3 and today()")

    var df = readTable(spark, "vw_time_series", predicates)

    df.printSchema()

    df.show()

    val windowSpec = Window.partitionBy("symbol")
    df = df
      .withColumn("min_timestamp", min("timestamp").over(windowSpec))
      .withColumn("max_timestamp", max("timestamp").over(windowSpec))
      .withColumn("tmp_open",
        when(col("timestamp") === col("min_timestamp"), col("open")))
      .withColumn("tmp_close",
        when(col("timestamp") === col("max_timestamp"), col("close")))
      .withColumn("first_open", first("tmp_open", ignoreNulls = true).over(windowSpec))
      .withColumn("last_close", first("tmp_close", ignoreNulls = true).over(windowSpec))
      .withColumn("percent_diff", round((col("last_close") * 100) / col("first_open"), 2) - lit(100))

      .withColumn("max_volume", max("volume").over(windowSpec))
      .withColumn("tmp_max_volume",
        when(col("volume") === col("max_volume"), col("timestamp")))
      .withColumn("first_max_volume", first("tmp_max_volume", ignoreNulls = true).over(windowSpec))

      .withColumn("max_close", max("close").over(windowSpec))
      .withColumn("min_close", min("close").over(windowSpec))
      .withColumn("tmp_max_close",
        when(col("close") === col("max_close"), col("timestamp")))
      .withColumn("tmp_min_close",
        when(col("close") === col("min_close"), col("timestamp")))
      .withColumn("first_max_close", first("tmp_max_close", ignoreNulls = true).over(windowSpec))
      .withColumn("first_min_close", first("tmp_min_close", ignoreNulls = true).over(windowSpec))

      .drop("tmp_open", "tmp_close", "tmp_max_volume", "tmp_max_close", "tmp_min_close")

    df.show()

    //    Название валюты
    //    Суммарный объем торгов за последние сутки
    //    Курс валюты на момент открытия торгов для данных суток
    //    Курс валюты на момент закрытия торгов для данных суток
    //    Разница(в %) курса с момента открытия до момента закрытия торгов для данных суток
    //    Минимальный временной интервал, на котором был зафиксирован самый крупный объем торгов для данных суток
    //    Минимальный временной интервал, на котором был зафиксирован максимальный курс для данных суток
    //    Минимальный временной интервал, на котором был зафиксирован минимальный курс торгов для данных суток

    df = df.groupBy("symbol").agg(
      sum("volume").as("total_volume"),
      first("first_open").as("opening_rate"),
      first("last_close").as("closing_rate"),
      first("percent_diff").as("percent_diff"),
      first("first_max_volume").as("first_max_volume"),
      first("first_max_close").as("first_max_close"),
      first("first_min_close").as("first_min_close"),
    )

    // превратим в интервалы
    df = df
      .withColumn("first_max_volume_minutes_added", col("first_max_volume") + expr(s"INTERVAL $TIME_SERIES_INTERVAL minutes"))
      .withColumn("first_max_volume_minutes_sub", col("first_max_volume") - expr(s"INTERVAL $TIME_SERIES_INTERVAL minutes"))
      .withColumn("max_volume_time_interval", concat(
        col("first_max_volume_minutes_sub"),
        lit(" - "),
        from_unixtime(unix_timestamp(col("first_max_volume_minutes_added"), "yyyy-MM-dd HH:mm:ss"), "HH:mm:ss")))

      .withColumn("first_max_close_minutes_added", col("first_max_close") + expr(s"INTERVAL $TIME_SERIES_INTERVAL minutes"))
      .withColumn("first_max_close_minutes_sub", col("first_max_close") - expr(s"INTERVAL $TIME_SERIES_INTERVAL minutes"))
      .withColumn("max_close_time_interval", concat(
        col("first_max_close_minutes_sub"),
        lit(" - "),
        from_unixtime(unix_timestamp(col("first_max_close_minutes_added"), "yyyy-MM-dd HH:mm:ss"), "HH:mm:ss")))

      .withColumn("first_min_close_minutes_added", col("first_min_close") + expr(s"INTERVAL $TIME_SERIES_INTERVAL minutes"))
      .withColumn("first_min_close_minutes_sub", col("first_min_close") - expr(s"INTERVAL $TIME_SERIES_INTERVAL minutes"))
      .withColumn("min_close_time_interval", concat(
        col("first_min_close_minutes_sub"),
        lit(" - "),
        from_unixtime(unix_timestamp(col("first_min_close_minutes_added"), "yyyy-MM-dd HH:mm:ss"), "HH:mm:ss")))

      .drop("first_max_volume_minutes_added", "first_max_volume_minutes_sub",
        "first_max_close_minutes_added", "first_max_close_minutes_sub",
        "first_min_close_minutes_added", "first_min_close_minutes_sub",
        "first_max_volume", "first_max_close", "first_min_close")

    df.show(false)

    val format = new SimpleDateFormat("yyyy_MM_dd__H_m_s")
    val directory = format.format(Calendar.getInstance().getTime())

    saveResult(df, directory)
  }

  private def readTable(spark: SparkSession, table: String, predicates: Array[String] = null): DataFrame = {
    val jdbcUrl = "jdbc:clickhouse://localhost:8123/de"
    val opts: collection.Map[String, String] = collection.Map("driver" -> "ru.yandex.clickhouse.ClickHouseDriver")
    val ckProperties = new Properties()
    ckProperties.put("user", "default")

    if (predicates != null)
      spark.read.options(opts).jdbc(jdbcUrl, table = table, predicates, ckProperties)
    else
      spark.read.options(opts).jdbc(jdbcUrl, table = table, ckProperties)
  }

  private def saveResult(df: DataFrame, directory: String): Unit = {
    df.write.parquet(s"hdfs://localhost:9000/gold/$directory.parquet")
  }

  private def readDirectory(spark: SparkSession, directory: String): DataFrame = {
    spark.read
      .option("header", true)
      .csv(s"hdfs://localhost:9000/bronze/$directory")
  }
}
