package com.hub.config

object AppConfig {
  val kafkaBootstrapServers: String = "192.168.10.102:9092,192.168.10.103:9092,192.168.10.104:9092"
  val kafkaTopic: String = "content_events"
  val dimItemPath: String = "D:/spark/Community_Real_Time/project_specs/mock_generator/samples/dim_item.csv"
  val dimUserProfilePath: String = "D:/spark/Community_Real_Time/project_specs/mock_generator/samples/dim_user_profile.csv"
  val redisHost: String = "192.168.10.102"
  val redisPort: Int = 6379
  val bloomFilterRedisKey: String = "new_user_bloomfilter"
  val bloomFilterExpectedInsert: Int = 1000000
  val bloomFilterFalsePositiveRate: Double = 0.01
  
}
