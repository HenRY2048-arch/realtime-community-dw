package com.hub.ingestion

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.hub.config.AppConfig

object KafkaReader {
  def readStream(spark: SparkSession): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", AppConfig.kafkaBootstrapServers)
      .option("subscribe", AppConfig.kafkaTopic)
      .option("startingOffsets", "earliest")
      .load()
  }
}
