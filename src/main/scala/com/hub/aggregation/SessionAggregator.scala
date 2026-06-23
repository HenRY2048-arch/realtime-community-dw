package com.hub.aggregation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SessionAggregator {
  /**
   * 聚合会话域指�?
   * 处理 S-01, S-03
   * 由于 Spark 3.0.0 不支持原生的 session_window，此处降级采�?30 分钟的滚动窗�?(Tumbling Window)
   * 结合 session_id 进行近似会话统计�?
   */
  def aggregate(df: DataFrame): DataFrame = {
    df
      .withWatermark("ts", "30 minutes")
      .groupBy(
        window(col("ts"), "30 minutes"),
        col("category")
      )
      .agg(
        // S-01: 会话总数
        approx_count_distinct(col("session_id")).alias("s_01_session_count"),
        
        // S-03: 平均会话事件�?(总事件数 / 会话�?
        (count(lit(1)) / approx_count_distinct(col("session_id"))).alias("s_03_avg_session_events")
      )
  }
  def sessionDuration(df: DataFrame): DataFrame = {
    df
      .withWatermark("ts", "30 minutes")
      .groupBy(
        window(col("ts"), "30 minutes"), col("session_id"))
      .agg(
        (unix_timestamp(max(col("ts"))) - unix_timestamp(min(col("ts")))).alias("s_02_session_duration")
      )
      
  }
}