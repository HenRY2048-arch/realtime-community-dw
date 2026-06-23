package com.hub.aggregation

import org.apache.spark.sql.{DataFrame, Row, Encoder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, StreamingQuery}
import com.hub.model.CheatEvent
import java.sql.Timestamp
import scala.collection.mutable.ListBuffer

case class CheatStateA1(comments: List[(Timestamp, String)])
case class CheatStateA2(recentLikes: List[(Timestamp, Long)])
case class A1Key(userId: Long, itemId: Long)

object CheatDetector {

  private val MinConsecutiveComments = 5
  private val AdjacentIntervalMs    = 10000L
  private val WindowDurationMs      = 60000L
  private val DupContentThreshold   = 4

  private def evaluateA1(comments: List[(Timestamp, String)]): Boolean = {
    if (comments.size < MinConsecutiveComments) return false
    val sorted = comments.sortBy(_._1.getTime)
    var c1 = true
    var i = 1
    val checkFrom = if (sorted.size < MinConsecutiveComments) 1 else sorted.size - MinConsecutiveComments + 1
    while (i < sorted.size && c1) {
      if (i >= checkFrom && sorted(i)._1.getTime - sorted(i - 1)._1.getTime > AdjacentIntervalMs) c1 = false
      i += 1
    }
    if (!c1) return false
    val mostRecent = sorted.last._1.getTime
    val inWindow = sorted.count(c => mostRecent - c._1.getTime <= WindowDurationMs)
    val c2 = inWindow >= MinConsecutiveComments
    val c3 = sorted.groupBy(_._2).values.exists(_.size >= DupContentThreshold)
    c2 || c3
  }

  def detectA1(df: DataFrame): StreamingQuery = {
    import org.apache.spark.sql.Encoders

    def transitionFunction(
      key: A1Key,
      events: Iterator[Row],
      state: GroupState[CheatStateA1]
    ): Iterator[CheatEvent] = {
      if (state.hasTimedOut) {
        state.remove()
        return Iterator.empty
      }
      var comments = state.getOption.map(_.comments).getOrElse(Nil)
      val results = ListBuffer[CheatEvent]()
      val sorted = events.toList.sortBy { row =>
        val ts = row.getAs[java.sql.Timestamp]("ts")
        if (ts == null) 0L else ts.getTime
      }
      for (row <- sorted) {
        val et = row.getAs[String]("event_type")
        val rowTs = row.getAs[java.sql.Timestamp]("ts")
        val tsStr = if (rowTs == null) "" else rowTs.toString
        if (et == "comment") {
          val text = Option(row.getAs[String]("comment_text")).getOrElse("")
          val threshold = rowTs.getTime - WindowDurationMs
          comments = (rowTs, text) :: comments.filter(_._1.getTime >= threshold)
          val isCheat = evaluateA1(comments)
          results += CheatEvent(
            event_id    = row.getAs[String]("event_id"),
            user_id     = key.userId,
            item_id     = Some(row.getAs[Long]("item_id")),
            event_type  = et,
            ts          = tsStr,
            is_cheat_A1 = isCheat
          )
        } else {
          comments = Nil
          results += CheatEvent(
            event_id    = row.getAs[String]("event_id"),
            user_id     = key.userId,
            item_id     = Some(row.getAs[Long]("item_id")),
            event_type  = et,
            ts          = tsStr,
            is_cheat_A1 = false
          )
        }
      }
      state.update(CheatStateA1(comments))
      if (!state.exists) state.setTimeoutDuration("30 minutes")
      results.iterator
    }

    implicit val keyEnc: Encoder[A1Key] =
      Encoders.product[A1Key]
    implicit val stEnc: Encoder[CheatStateA1] =
      Encoders.product[CheatStateA1]
    implicit val outEnc: Encoder[CheatEvent] =
      Encoders.product[CheatEvent]

    df
      .withWatermark("ts", "30 minutes")
      .filter(col("item_id").isNotNull)
      .groupByKey(row => A1Key(row.getAs[Long]("user_id"), row.getAs[Long]("item_id")))
      .flatMapGroupsWithState(
        OutputMode.Append(),
        GroupStateTimeout.EventTimeTimeout()
      )(transitionFunction)
      .writeStream
      .outputMode("append")
      .format("memory")
      .queryName("cheat_a1_result")
      .start()
  }

  def detectA2(df: DataFrame): StreamingQuery = {
    import org.apache.spark.sql.Encoders

    def transitionFunctionA2(
      key: Long,
      events: Iterator[Row],
      state: GroupState[CheatStateA2]
    ): Iterator[CheatEvent] = {
      if (state.hasTimedOut) {
        state.remove()
        return Iterator.empty
      }
      var likes = state.getOption.map(_.recentLikes).getOrElse(Nil)
      val results = ListBuffer[CheatEvent]()
      val sorted = events.toList.sortBy { row =>
        val t = row.getAs[java.sql.Timestamp]("ts")
        if (t == null) 0L else t.getTime
      }
      for (row <- sorted) {
        if (row.getAs[String]("event_type") == "like") {
          val ts     = row.getAs[java.sql.Timestamp]("ts")
          if (ts != null) {
          val itemId = row.getAs[Long]("item_id")
          val threshold = ts.getTime - 5000L
          likes = (ts, itemId) :: likes.filter(_._1.getTime >= threshold)
          if (likes.map(_._2).distinct.size >= 15) {
            results += CheatEvent(
              event_id    = row.getAs[String]("event_id"),
              user_id     = key,
              item_id     = Some(itemId),
              event_type  = "like",
              ts          = ts.toString,
              is_cheat_A1 = false,
              is_cheat_A2 = true
            )
          }
          }
        }
      }
      state.update(CheatStateA2(likes))
      if (!state.exists) state.setTimeoutDuration("30 minutes")
      results.iterator
    }

    implicit val stEnc: Encoder[CheatStateA2] =
      Encoders.product[CheatStateA2]
    implicit val outEnc: Encoder[CheatEvent] =
      Encoders.product[CheatEvent]

    df
      .withWatermark("ts", "30 minutes")
      .filter(col("item_id").isNotNull)
      .groupByKey(row => row.getAs[Long]("user_id"))(Encoders.scalaLong)
      .flatMapGroupsWithState(
        OutputMode.Append(),
        GroupStateTimeout.EventTimeTimeout()
      )(transitionFunctionA2)
      .writeStream
      .outputMode("append")
      .format("memory")
      .queryName("cheat_a2_result")
      .start()
  }
}