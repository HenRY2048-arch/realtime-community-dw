package com.hub.aggregation

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType, TimestampType}
import com.hub.state.BloomFilterService


object NewUserAggregator {
    def run(df: DataFrame, spark: SparkSession): StreamingQuery = {
      df.writeStream
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          val appOpenIds = batchDF
            .filter(col("event_type") === "app_open")
            .select("user_id")
            .distinct()
            .collect()
            .map(_.getLong(0))

          val newCount = appOpenIds.count(id => BloomFilterService.checkElement(id))
          appOpenIds.foreach(BloomFilterService.markSeen)

          val row = Row(
            new java.sql.Timestamp(System.currentTimeMillis()),
            "ALL",
            newCount.toLong
          )
          val outDF = spark.createDataFrame(
            spark.sparkContext.parallelize(Seq(row)),
            StructType(Array(
              StructField("window", TimestampType),
              StructField("category", StringType),
              StructField("u_04_new_user_count", LongType)
            ))
          )
          outDF.createOrReplaceTempView("new_user_result")
        }
        .outputMode("update")
        .start()
    }
}
