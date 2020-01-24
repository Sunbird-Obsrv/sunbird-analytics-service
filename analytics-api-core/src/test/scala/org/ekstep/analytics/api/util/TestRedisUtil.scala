package org.ekstep.analytics.api.util

import java.util

import org.ekstep.analytics.api.BaseSpec
import redis.clients.jedis.Jedis
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import redis.embedded.RedisServer
import redis.clients.jedis.exceptions.JedisConnectionException

class TestRedisUtil extends FlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {

  private var redisServer:RedisServer = _;
  val redisUtil = new RedisUtil();
  
  override def beforeAll() {
    super.beforeAll()
    redisServer = new RedisServer(6379);
    redisServer.start();
  }
  
  override def afterAll() {
    super.afterAll()
    redisServer.stop();
  }

  "RedisUtil" should "assert for all available utility methods" in {
    
    redisUtil.checkConnection should be (true)
    val jedis = redisUtil.getConnection(1);
    jedis.getDB should be (1);
    
    noException should be thrownBy {
      redisUtil.resetConnection()
    }
    
    intercept[JedisConnectionException] {
      redisUtil.closePool();
      redisUtil.getConnection(1);
    }
    
    redisServer.stop();
    redisUtil.checkConnection should be (false)
    
    redisServer.start();
  }

}