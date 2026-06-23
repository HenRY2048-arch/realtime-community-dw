package com.hub.sink

import com.hub.config.AppConfig
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.StreamingQuery
import redis.clients.jedis.JedisPool

object RedisSink {
  private val jedisPool: Option[JedisPool] = try {
    val pool = new JedisPool(AppConfig.redisHost, AppConfig.redisPort)
    val jedis = pool.getResource
    try { jedis.ping() } finally { jedis.close() }
    Some(pool)
  } catch {
    case _: Exception =>
      println("[RedisSink] Redis unavailable, DWS writes will be skipped")
      None
  }

  def writeStream(
      queryName: String,
      stream: DataFrame,
      redisKeyPrefix: String,
      outputMode: String = "complete"
  ): StreamingQuery = {
    stream.writeStream
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        jedisPool.foreach { pool =>
          batchDF.collect().foreach { row =>
            val w = row.getAs[Row]("window")
            val start = w.getAs[java.sql.Timestamp]("start").getTime / 1000
            val end = w.getAs[java.sql.Timestamp]("end").getTime / 1000
            val key = if (row.schema.fieldNames.contains("category")) {
              val cat = row.getAs[String]("category")
              s"$redisKeyPrefix:${start}_$end:$cat"
            } else {
              s"$redisKeyPrefix:${start}_$end"
            }

            val jedis = pool.getResource
            try {
              val metricFields = row.schema.fieldNames
                .filter(n => n != "window" && n != "category" && n != "session_id")
              metricFields.foreach { field =>
                val value = Option(row.get(row.fieldIndex(field)))
                  .map(_.toString).getOrElse("0")
                jedis.hset(key, field, value)
              }
              jedis.expire(key, 3600L)
            } finally {
              jedis.close()
            }
          }
        }
      }
      .outputMode(outputMode)
      .queryName(queryName)
      .start()
  }
}