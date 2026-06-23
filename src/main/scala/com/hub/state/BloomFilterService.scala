package com.hub.state

import com.hub.config.AppConfig
import com.google.common.hash.{BloomFilter, Funnels}
import redis.clients.jedis.JedisPool
import redis.clients.jedis.commands.ProtocolCommand
import redis.clients.jedis.util.SafeEncoder

object BloomFilterService {
    private val capacity = AppConfig.bloomFilterExpectedInsert
    private val fpr = AppConfig.bloomFilterFalsePositiveRate
    private val redisKey = AppConfig.bloomFilterRedisKey

    private object BFCommands {
        val BF_ADD: ProtocolCommand = () => SafeEncoder.encode("BF.ADD")
        val BF_EXISTS: ProtocolCommand = () => SafeEncoder.encode("BF.EXISTS")
    }

    private val jedisPool: Option[JedisPool] = try {
        val pool = new JedisPool(AppConfig.redisHost, AppConfig.redisPort)
        val jedis = pool.getResource
        try { jedis.ping() } finally { jedis.close() }
        Some(pool)
    } catch {
        case _: Exception =>
            println("[BloomFilterService] Redis unavailable, falling back to Guava local")
            None
    }

    private val guavaFilter: BloomFilter[java.lang.Long] = BloomFilter.create(
        Funnels.longFunnel(),
        capacity,
        fpr
    )

    private def bfExists(userId: Long): Boolean = {
        val jedis = jedisPool.get.getResource
        try {
            jedis.getClient.sendCommand(BFCommands.BF_EXISTS,
                SafeEncoder.encode(redisKey),
                SafeEncoder.encode(userId.toString))
            jedis.getClient.getIntegerReply == 0
        } finally {
            jedis.close()
        }
    }

    private def bfAdd(userId: Long): Long = {
        val jedis = jedisPool.get.getResource
        try {
            jedis.getClient.sendCommand(BFCommands.BF_ADD,
                SafeEncoder.encode(redisKey),
                SafeEncoder.encode(userId.toString))
            jedis.getClient.getIntegerReply
        } finally {
            jedis.close()
        }
    }

    def checkElement(userId: Long): Boolean = {
        jedisPool match {
            case Some(_) => bfExists(userId)
            case None    => !guavaFilter.mightContain(userId)
        }
    }

    def markSeen(userId: Long): Unit = {
        jedisPool match {
            case Some(_) => bfAdd(userId)
            case None    => guavaFilter.put(userId)
        }
    }
}

