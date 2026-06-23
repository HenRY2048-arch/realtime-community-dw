package com.hub.aggregation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object InteractionAggregator {
  /**
   * 聚合互动与意向指�?
   * 处理 I-01~I-04, I-08~I-11
   * 5分钟滚动窗口
   */
  def aggregate(df: DataFrame): DataFrame = {
    df
      .withWatermark("ts", "5 minutes")
      .groupBy(
        window(col("ts"), "5 minutes"),
        col("category")
      )
      .agg(
        // I-01: 点赞�?
        count(when(col("event_type") === "like", true)).alias("i_01_like"),
        
        // I-02: 收藏�?
        count(when(col("event_type") === "fav", true)).alias("i_02_fav"),
        
        // I-03: 分享�?
        count(when(col("event_type") === "share", true)).alias("i_03_share"),
        
        // I-04: 评论�?
        count(when(col("event_type") === "comment", true)).alias("i_04_comment"),
        
        // I-08: 关注�?
        count(when(col("event_type") === "follow", true)).alias("i_08_follow"),
        
        // I-09: 取关�?
        count(when(col("event_type") === "unfollow", true)).alias("i_09_unfollow"),
        
        // I-10: 私信�?
        count(when(col("event_type") === "dm", true)).alias("i_10_dm"),
        
        // I-11: 进入个人主页�?
        count(when(col("event_type") === "enter_profile", true)).alias("i_11_enter_profile"),

        // I-07: 收藏�?
        (count(when(col("event_type") === "fav", true))
          / count(when(col("event_type") === "enter_item", true))).alias("i_07_fav_rate")
      )
  }
  
  def interactionRate(df: DataFrame): DataFrame = {
    df
      .withWatermark("ts", "5 minutes")
      .groupBy(
        window(col("ts"), "5 minutes")
      )
      .agg(
        // I-05: 互动�?(分母 = 全事�?
        (count(when(col("event_type").isin("like", "fav", "share", "comment"), true))
          / count(lit(1))).alias("i_05_interaction_rate"),

        // I-06: 曝光-互动转化�?(分母 = enter_item)
        (count(when(col("event_type").isin("like", "fav", "share", "comment"), true))
          / count(when(col("event_type") === "enter_item", true))).alias("i_06_conversion_rate")
      )
  }
}

