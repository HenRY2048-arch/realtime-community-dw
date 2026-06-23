package com.hub.sink

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

object DeltaSink {
  private val basePath = "D:/spark/Community_Real_Time/data/delta"

  def writeStream(
      queryName: String,
      stream: DataFrame,
      layer: String,
      outputMode: String = "append"
  ): StreamingQuery = {
    val path = s"$basePath/$layer"
    val checkpointPath = s"$basePath/checkpoint/$layer"

    stream.writeStream
      .format("delta")
      .outputMode(outputMode)
      .option("checkpointLocation", checkpointPath)
      .option("path", path)
      .queryName(queryName)
      .start()
  }
}
