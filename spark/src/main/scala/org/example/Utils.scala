package org.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.elasticsearch.spark.sql.sparkDatasetFunctions

import org.springframework.vault.authentication.TokenAuthentication
import org.springframework.vault.client.VaultEndpoint
import org.springframework.vault.support.Versioned
import org.springframework.vault.core.VaultTemplate

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

  def readVault(key: String): String = {
    val vaultEndpoint = new VaultEndpoint

    vaultEndpoint.setHost("127.0.0.1")
    vaultEndpoint.setPort(8200)
    vaultEndpoint.setScheme("http")

    val vaultTemplate = new VaultTemplate(vaultEndpoint, new TokenAuthentication("dev-only-token"))

    val readResponse = vaultTemplate.opsForVersionedKeyValue("secret").get("KeyName")

    var password = ""
    if (readResponse != null && readResponse.hasData)
      password = readResponse.getData.get("ALPHAVANTAGE_KEY").asInstanceOf[String]

    password
  }
}
