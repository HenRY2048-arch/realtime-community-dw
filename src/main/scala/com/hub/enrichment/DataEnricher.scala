package com.hub.enrichment

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.hub.config.AppConfig

object DataEnricher {

  def loadDimItem(spark: SparkSession): DataFrame = {
    val itemSchema = StructType(Array(
      StructField("dim_item_id", LongType, nullable = false),
      StructField("category", StringType, nullable = true),
      StructField("dim_content_len", IntegerType, nullable = true)
    ))
    spark.read
      .option("delimiter", "|")
      .option("header", "false")
      .schema(itemSchema)
      .csv(AppConfig.dimItemPath)
  }

  def enrichBatch(batchDF: DataFrame, dimItemDf: DataFrame): DataFrame = {
    val joinedDf = batchDF.join(
      dimItemDf,
      batchDF("item_id") === dimItemDf("dim_item_id"),
      "left"
    )
    joinedDf
      .withColumn("category", coalesce(col("category"), lit("Unknown")))
      .withColumn("content_len", coalesce(col("dim_content_len"), lit(0)))
      .drop("dim_item_id", "dim_content_len")
  }

  def enrich(spark: SparkSession, streamDf: DataFrame): DataFrame = {
    enrichBatch(streamDf, loadDimItem(spark))
  }
}
