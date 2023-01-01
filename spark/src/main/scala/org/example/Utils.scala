package org.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.elasticsearch.spark.sql.sparkDatasetFunctions

import java.util.Properties

object Utils {

  private val hdfsUrl = "hdfs://localhost:9000"
  private val s3Url = "s3a://my-s3bucket"
  private val jdbcUrl = "jdbc:clickhouse://localhost:8123/de"

  def readTable(spark: SparkSession, table: String, predicates: Array[String] = null): DataFrame = {
    val opts: collection.Map[String, String] = collection.Map("driver" -> "ru.yandex.clickhouse.ClickHouseDriver")
    val ckProperties = new Properties()
    ckProperties.put("user", "default")

    if (predicates != null)
      spark.read.options(opts).jdbc(jdbcUrl, table = table, predicates, ckProperties)
    else
      spark.read.options(opts).jdbc(jdbcUrl, table = table, ckProperties)
  }

  def readSettings(spark: SparkSession): Map[SettingKey.Value, String] = {
    val settings = Utils.readTable(spark, "settings");

    import SettingKey._

    val timeSeriesInterval = settings
      .filter(col("key") === interval_minutes.toString)
      .select(col("value"))
      .first()
      .getString(0)

    val objectStorage = settings
      .filter(col("key") === object_storage.toString)
      .select(col("value"))
      .first()
      .getString(0)

    Map[Value, String](interval_minutes -> timeSeriesInterval, object_storage -> objectStorage)
  }

  def saveToElasticSearch(df: DataFrame, index: String): Unit = {
    // ElasticSearch doesn't work well with decimal
    val result = df
      .withColumn("opening_rate", col("opening_rate").cast(DoubleType))
      .withColumn("closing_rate", col("closing_rate").cast(DoubleType))
      .withColumn("percent_diff", col("percent_diff").cast(DoubleType))

    result.saveToEs(index)
  }

  def saveToS3(df: DataFrame, directory: String): Unit = {
    val outputPath = s"$s3Url/gold/$directory.parquet"
    df.write.mode("overwrite").parquet(outputPath)
  }

  def saveToHDFS(df: DataFrame, directory: String): Unit = {
    df.write.parquet(s"$hdfsUrl/gold/$directory.parquet")
  }

  def readFromHDFS(spark: SparkSession, directory: String): DataFrame = {
    spark.read
      .option("header", true)
      .csv(s"$hdfsUrl/bronze/$directory")
  }
}
