package org.example

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql.sparkDatasetFunctions
import com.amazonaws.SDKGlobalConfiguration

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

object App {
  private object SettingKeys extends Enumeration {
    val interval_minutes = Value
    val object_storage = Value
  }

  private object ObjectStorage extends Enumeration {
    val hdfs = Value
    val s3 = Value
  }

  private var TIME_SERIES_INTERVAL = 15
  private var OBJECT_STORAGE = ObjectStorage.hdfs

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .config("es.index.auto.create", "true")
      .config("es.mapping.date.rich", "true")
      .config("spark.es.nodes.wan.only", "true") // for Docker
      .appName("SparkByExample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    configureS3(spark)

    readSettings(spark)

    // over the past day
    val daysToSubtract = 1;
    val predicates = Array[String](s"timestamp between today() - $daysToSubtract and today() - ${daysToSubtract - 1}")

    //var df = readTable(spark, "vw_time_series", predicates)
    var df = readTable(spark, "vw_time_series")

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

     // Currency name
     // Total trading volume for the last day
     // The exchange rate at the moment of trading opening for the given day
     // Currency exchange rate at the moment of trading closing for the given day
     // Difference (in %) of the exchange rate from the moment of opening to the moment of closing of trading for the given day
     // The minimum time interval on which the largest trading volume for the given day was recorded
     // The minimum time interval at which the maximum rate was fixed for the given day
     // The minimum time interval on which the minimum trading rate was fixed for the given day

    df = df.groupBy("symbol").agg(
      sum("volume").as("total_volume"),
      first("first_open").as("opening_rate"),
      first("last_close").as("closing_rate"),
      first("percent_diff").as("percent_diff"),
      first("first_max_volume").as("first_max_volume"),
      first("first_max_close").as("first_max_close"),
      first("first_min_close").as("first_min_close"),
      first(from_unixtime(unix_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd")).as("date")
    )

    // turn into intervals
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

    val format = new SimpleDateFormat("yyyy_MM_dd__HH_mm_ss")
    val directory = format.format(Calendar.getInstance().getTime())

    import ObjectStorage._

    if (OBJECT_STORAGE == hdfs)
      saveToHDFS(df, directory)
    else
      saveToS3(df, directory)

    saveToElasticSearch(df)
  }

  private def readSettings(spark: SparkSession): Unit = {
    val settings = readTable(spark, "settings");

    implicit def stringToObjectStorage(objectStorage: String): ObjectStorage.Value = ObjectStorage.values.find(_.toString == objectStorage).get

    import SettingKeys._

    TIME_SERIES_INTERVAL = settings
      .filter(col("key") === interval_minutes.toString)
      .select(col("value"))
      .first()
      .getString(0)
      .toInt


    OBJECT_STORAGE = settings
      .filter(col("key") === object_storage.toString)
      .select(col("value"))
      .first()
      .getString(0)
  }

  private def configureS3(spark: SparkSession): Unit = {
    System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true")

    val s3accessKeyAws = ""
    val s3secretKeyAws = ""
    val connectionTimeOut = "600000"
    val s3endPointLoc: String = "http://127.0.0.1:9010"

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", s3endPointLoc)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", s3accessKeyAws)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", s3secretKeyAws)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.timeout", connectionTimeOut)
    spark.sparkContext.hadoopConfiguration.set("spark.sql.debug.maxToStringFields", "100")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
  }

  private def saveToElasticSearch(df: DataFrame): Unit = {
    // ElasticSearch doesn't work well with decimal
    val result = df
      .withColumn("opening_rate", col("opening_rate").cast(DoubleType))
      .withColumn("closing_rate", col("closing_rate").cast(DoubleType))
      .withColumn("percent_diff", col("percent_diff").cast(DoubleType))

    result.saveToEs("task2")
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

  private def saveToS3(df: DataFrame, directory: String): Unit = {
    val outputPath = s"s3a://my-s3bucket/gold/$directory.parquet"
    df.write.mode("overwrite").parquet(outputPath)
  }

  private def saveToHDFS(df: DataFrame, directory: String): Unit = {
    df.write.parquet(s"hdfs://localhost:9000/gold/$directory.parquet")
  }

  private def readFromHDFS(spark: SparkSession, directory: String): DataFrame = {
    spark.read
      .option("header", true)
      .csv(s"hdfs://localhost:9000/bronze/$directory")
  }
}
