package com.hub.app

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryProgress}
import com.hub.ingestion.KafkaReader
import com.hub.cleansing.DataCleanser
import com.hub.enrichment.DataEnricher
import com.hub.aggregation.{UserActivityAggregator, InteractionAggregator, SessionAggregator, NewUserAggregator}
import com.hub.aggregation.CheatDetector
import com.hub.sink.{RedisSink, DeltaSink}
import java.io.{File, PrintWriter}
import java.nio.charset.StandardCharsets

object RealTimeApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Community Real-Time Indicators")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setCheckpointDir("D:/spark/Community_Real_Time/data/checkpoint")
    spark.sparkContext.setLogLevel("WARN")

    val isTestMode = args.contains("--test")
    val testDurationSec = if (isTestMode) {
      args.find(_.startsWith("--duration=")).map(_.split("=")(1).toInt).getOrElse(180)
    } else 0

    // �?Q-10 添加 StreamingQueryListener (监控 numRowsDroppedByWatermark)
    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        println(s"Query started: ${event.id}")
      }

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        val progress = event.progress
        val stateOperators = progress.stateOperators
        if (stateOperators.nonEmpty) {
          val dropped = stateOperators.map(s => s.customMetrics.getOrDefault("numRowsDroppedByWatermark", 0L).toLong).sum
          if (dropped > 0) {
            println(s"[Q-10 Monitor] numRowsDroppedByWatermark: $dropped")
          }
        }
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        println(s"Query terminated: ${event.id}")
      }
    })

    val enrichedStreamPath = "D:/spark/Community_Real_Time/data/delta/dwd"

    val rawStream = KafkaReader.readStream(spark)
    val cleansedStream = DataCleanser.clean(rawStream)

    val deltaDWDQuery = cleansedStream.writeStream
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        val dimItemDf = DataEnricher.loadDimItem(spark)
        val enrichedBatch = DataEnricher.enrichBatch(batchDF, dimItemDf)
        enrichedBatch.write
          .mode("append")
          .format("delta")
          .save(enrichedStreamPath)
      }
      .option("checkpointLocation", "D:/spark/Community_Real_Time/data/delta/checkpoint/dwd")
      .queryName("delta_dwd")
      .start()

    val enrichedStream = spark.readStream
      .format("delta")
      .load(enrichedStreamPath)

    val userActivityStream = UserActivityAggregator.aggregate(enrichedStream)
    val interactionStream = InteractionAggregator.aggregate(enrichedStream)
    val sessionStream = SessionAggregator.aggregate(enrichedStream)
    val newUserQuery = NewUserAggregator.run(enrichedStream, spark)
    val bounceRateStream = UserActivityAggregator.bounceRate(enrichedStream)
    val interactionRateStream = InteractionAggregator.interactionRate(enrichedStream)
    val cheatQuery = CheatDetector.detectA1(enrichedStream)
    val cheatQuery2 = CheatDetector.detectA2(enrichedStream)

    // 启动多个查询
    val userActivityQuery = userActivityStream.writeStream
      .outputMode("update") 
      .format("memory")
      .queryName("user_activity_result")
      .start()

    val userActivityRedisQuery = RedisSink.writeStream("redis_user_activity", userActivityStream, "dws:user_activity", "update")

    val interactionQuery = interactionStream.writeStream
      .outputMode("complete")
      .format("memory")
      .queryName("interaction_result")
      .start()

    val interactionRedisQuery = RedisSink.writeStream("redis_interaction", interactionStream, "dws:interaction")

    val sessionQuery = sessionStream.writeStream
      .outputMode("complete")
      .format("memory")
      .queryName("session_result")
      .start()

    val sessionRedisQuery = RedisSink.writeStream("redis_session", sessionStream, "dws:session")

    val bounceRateQuery = bounceRateStream.writeStream
      .outputMode("complete")
      .format("memory")
      .queryName("bounce_rate_result")
      .start()

    val interactionRateQuery = interactionRateStream.writeStream
      .outputMode("complete")
      .format("memory")
      .queryName("interaction_rate_result")
      .start()

    val bounceRateRedisQuery = RedisSink.writeStream("redis_bounce_rate", bounceRateStream, "dws:bounce_rate")
    val interactionRateRedisQuery = RedisSink.writeStream("redis_interaction_rate", interactionRateStream, "dws:interaction_rate")

    val deltaODSQuery = DeltaSink.writeStream("delta_ods", cleansedStream, "ods")

    val sessionDurationQuery = SessionAggregator.sessionDuration(enrichedStream).writeStream
      .outputMode("complete")
      .format("memory")
      .queryName("session_duration_result")
      .start()


    println("all query started")

    if (isTestMode) {
      println(s"[TEST] Running in test mode, will query results after ${testDurationSec}s then exit")
      println(s"[TEST] Waiting ${testDurationSec}s for data processing...")
      Thread.sleep(testDurationSec * 1000L)

      val outDir = "D:/spark/Community_Real_Time/data/test_result"
      new File(outDir).mkdirs()

      def dumpTable(name: String, df: org.apache.spark.sql.DataFrame, rows: Int): Unit = {
        println(s"\n========== $name ==========")
        df.show(rows, false)
        val csvDf = if (df.columns.contains("window")) {
          df.withColumn("window", org.apache.spark.sql.functions.col("window").cast("string"))
        } else df
        csvDf.coalesce(1).write
          .option("header", "true")
          .option("encoding", "UTF-8")
          .mode("overwrite")
          .csv(s"$outDir/$name")
        println(s"[TEST] $name -> $outDir/$name")
      }

      dumpTable("user_activity_result", spark.sql("SELECT * FROM user_activity_result ORDER BY window DESC"), 20)
      dumpTable("interaction_result", spark.sql("SELECT * FROM interaction_result ORDER BY window DESC"), 20)

      try {
        dumpTable("new_user_result", spark.sql("SELECT * FROM new_user_result"), 10)
      } catch {
        case _: Exception => println("[INFO] new_user_result not available yet")
      }

      dumpTable("session_result", spark.sql("SELECT * FROM session_result ORDER BY window DESC"), 20)
      dumpTable("bounce_rate_result", spark.sql("SELECT * FROM bounce_rate_result ORDER BY window DESC"), 10)
      dumpTable("interaction_rate_result", spark.sql("SELECT * FROM interaction_rate_result ORDER BY window DESC"), 10)
      dumpTable("session_duration_result", spark.sql("SELECT * FROM session_duration_result ORDER BY window DESC"), 20)
      dumpTable("cheat_a1_result", spark.sql("SELECT * FROM cheat_a1_result WHERE is_cheat_A1 = true"), 10)
      dumpTable("cheat_a2_result", spark.sql("SELECT * FROM cheat_a2_result WHERE is_cheat_A2 = true"), 10)

      println(s"\n[TEST] All results written to: $outDir")

      println("\n[TEST] Stopping all queries...")
      spark.streams.active.foreach(_.stop())
      println("[TEST] Done.")
    } else {
      Thread.sleep(120000)
      spark.sql("SELECT * FROM user_activity_result ORDER BY window DESC").show(5, false)
      spark.sql("SELECT * FROM interaction_result ORDER BY window DESC").show(5, false)
      try { spark.sql("SELECT * FROM new_user_result").show(5, false) }
      catch { case _: Exception => println("[INFO] new_user_result not available yet, skip") }
      spark.sql("SELECT * FROM session_result ORDER BY window DESC").show(5, false)
      spark.sql("SELECT * FROM bounce_rate_result ORDER BY window DESC").show(5, false)
      spark.sql("SELECT * FROM interaction_rate_result ORDER BY window DESC").show(5, false)
      spark.sql("SELECT * FROM session_duration_result ORDER BY window DESC").show(5, false)
      spark.sql("SELECT * FROM cheat_a1_result WHERE is_cheat_A1 = true").show(5, false)
      spark.sql("SELECT * FROM cheat_a2_result WHERE is_cheat_A2 = true").show(5, false)
      spark.streams.awaitAnyTermination()
    }
  }
}
