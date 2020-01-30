package org.ekstep.analytics.api.util

import java.time.Duration

import com.typesafe.config.{Config, ConfigFactory}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import scala.collection.JavaConverters._
// import javax.inject._

// @Singleton
class RedisUtil {
  implicit val className = "org.ekstep.analytics.api.util.RedisUtil"

  private val redis_host = AppConfig.getString("redis.host")
  private val redis_port = AppConfig.getInt("redis.port")

  private def buildPoolConfig = {
    val poolConfig = new JedisPoolConfig
    poolConfig.setMaxTotal(AppConfig.getInt("redis.connection.max"))
    poolConfig.setMaxIdle(AppConfig.getInt("redis.connection.idle.max"))
    poolConfig.setMinIdle(AppConfig.getInt("redis.connection.idle.min"))
    poolConfig.setTestOnBorrow(true)
    poolConfig.setTestOnReturn(true)
    poolConfig.setTestWhileIdle(true)
    poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(AppConfig.getInt("redis.connection.minEvictableIdleTimeSeconds")).toMillis)
    poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(AppConfig.getInt("redis.connection.timeBetweenEvictionRunsSeconds")).toMillis)
    poolConfig.setNumTestsPerEvictionRun(3)
    poolConfig.setBlockWhenExhausted(true)
    poolConfig
  }

  protected var jedisPool = new JedisPool(buildPoolConfig, redis_host, redis_port)

  def getConnection(database: Int): Jedis = {
    val conn = jedisPool.getResource
    conn.select(database)
    conn
  }

  def resetConnection(): Unit = {
    jedisPool.close()
    jedisPool = new JedisPool(buildPoolConfig, redis_host, redis_port)
  }

  def closePool() = {
    jedisPool.close()
  }

  def checkConnection = {
    try {
      val conn = getConnection(2)
      conn.close()
      true;
    } catch {
      case ex: Exception => false
    }
  }
}
