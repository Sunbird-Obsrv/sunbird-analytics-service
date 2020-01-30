package org.ekstep.analytics.api.service

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{TestActorRef, TestProbe}
import com.typesafe.config.Config
import org.ekstep.analytics.api.BaseSpec
import org.ekstep.analytics.api.util.{DeviceStateDistrict, RedisUtil}
import org.mockito.Mockito.{times, verify, when}
import org.ekstep.analytics.api.util.CacheUtil
import org.ekstep.analytics.api.util.IPLocationCache
import org.ekstep.analytics.api.util.DeviceLocation
import de.sciss.fingertree.RangedSeq
import scala.math.Ordering
import redis.embedded.RedisServer;
import scala.collection.JavaConverters._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import com.typesafe.config.ConfigFactory
import org.ekstep.analytics.api.util.KafkaUtil
import redis.clients.jedis.exceptions.JedisConnectionException
import org.ekstep.analytics.api.util.APILogger

class TestDeviceProfileService extends FlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {

  implicit val config = ConfigFactory.load()
  val deviceProfileServiceMock: DeviceProfileService = mock[DeviceProfileService]
  private implicit val system: ActorSystem = ActorSystem("device-register-test-actor-system", config)
  private val configMock = mock[Config]
  private val redisUtil = new RedisUtil();
  val redisIndex: Int = 2
  implicit val executor = scala.concurrent.ExecutionContext.global
  val kafkaUtil = new KafkaUtil()
  val saveMetricsActor = TestActorRef(new SaveMetricsActor(kafkaUtil))
  val metricsActorProbe = TestProbe()
  when(configMock.getInt("redis.deviceIndex")).thenReturn(2)
  when(configMock.getInt("redis.port")).thenReturn(6379)
  when(configMock.getString("postgres.table.geo_location_city.name")).thenReturn("geo_location_city")
  when(configMock.getString("postgres.table.geo_location_city_ipv4.name")).thenReturn("geo_location_city_ipv4")
  when(configMock.getBoolean("device.api.enable.debug.log")).thenReturn(true)
  private val deviceProfileServiceActorRef = TestActorRef(new DeviceProfileService(configMock, redisUtil) {

  })
  val geoLocationCityIpv4TableName = config.getString("postgres.table.geo_location_city_ipv4.name")
  val geoLocationCityTableName = config.getString("postgres.table.geo_location_city.name")
  private var redisServer:RedisServer = _;


  override def beforeAll() {
    super.beforeAll()
    redisServer = new RedisServer(6379);
    redisServer.start();
    val jedis = redisUtil.getConnection(redisIndex);
    jedis.hmset("device-001", Map("user_declared_state" -> "Karnataka", "user_declared_district" -> "Tumkur").asJava);
    jedis.close();
  }
  
  override def afterAll() {
    super.afterAll()
    redisServer.stop();
    deviceProfileServiceActorRef.restart(new Exception() {})
    deviceProfileServiceActorRef.stop();
  }


  "DeviceProfileService" should "return location details given an IP address" in {
    when(deviceProfileServiceMock.resolveLocation(ipAddress = "106.51.74.185"))
      .thenReturn(DeviceStateDistrict("Karnataka", "BANGALORE"))
    val deviceLocation = deviceProfileServiceMock.resolveLocation("106.51.74.185")
    deviceLocation.state should be("Karnataka")
    deviceLocation.districtCustom should be("BANGALORE")
  }

  it should "return empty location if the IP address is not found" in {
    when(deviceProfileServiceMock.resolveLocation(ipAddress = "106.51.74.185"))
      .thenReturn(new DeviceStateDistrict())
    val deviceLocation = deviceProfileServiceMock.resolveLocation("106.51.74.185")
    deviceLocation.state should be("")
    deviceLocation.districtCustom should be("")
  }


  it should "get the device profile data" in {
    
    IPLocationCache.setGeoLocMap(Map(1234 -> DeviceLocation(1234, "Asia", "IN", "India", "KA", "Karnataka", "", "Bangalore", "Karnataka", "29", "Bangalore")))
    IPLocationCache.setRangeTree(RangedSeq((1781746350l, 1781746370l) -> 1234)(_._1, Ordering.Long))
    val deviceProfile = deviceProfileServiceActorRef.underlyingActor.getDeviceProfile(DeviceProfileRequest("device-001", "106.51.74.185"))
    deviceProfile.get.ipLocation.get.state should be ("Karnataka")
    deviceProfile.get.ipLocation.get.district should be ("Bangalore")
    deviceProfile.get.userDeclaredLocation.get.district should be ("Tumkur")
    deviceProfile.get.userDeclaredLocation.get.state should be ("Karnataka")
    
    val deviceProfile2 = deviceProfileServiceActorRef.underlyingActor.getDeviceProfile(DeviceProfileRequest("device-001", ""))
    deviceProfile2 should be (None)
  }

  it should "When state is not defined" in {

    IPLocationCache.setGeoLocMap(Map(1234 -> DeviceLocation(1234, "Asia", "IN", "India", "KA", "Karnataka", "", "Bangalore", "", "29", "Bangalore")))
    IPLocationCache.setRangeTree(RangedSeq((1781746350l, 1781746370l) -> 1234)(_._1, Ordering.Long))
    val deviceProfile = deviceProfileServiceActorRef.underlyingActor.getDeviceProfile(DeviceProfileRequest("device-001", "106.51.74.185"))
    deviceProfile.get.ipLocation.get.state should be ("")
    deviceProfile.get.ipLocation.get.district should be ("Bangalore")
    deviceProfile.get.userDeclaredLocation.get.district should be ("Tumkur")
    deviceProfile.get.userDeclaredLocation.get.state should be ("Karnataka")
  }


  it should "catch the exception" in {
    intercept[Exception] {
      when(configMock.getBoolean("device.api.enable.debug.log")).thenThrow(new Exception("Error"))
      deviceProfileServiceActorRef.tell(DeviceProfileRequest("device-001", "106.51.74.185"), ActorRef.noSender)
    }
  }
  
  it should "check whether all exception branches are invoked" in {
    
    APILogger.init("DeviceProfileService");
    APILogger.logMetrics(Option(Map()), "DeviceProfileService")("TestDeviceProfileService");
    noException must be thrownBy {
      deviceProfileServiceActorRef.receive(DeviceProfileRequest("device-0001", "206.51.74.185"))
    }
    intercept[Exception] {
      deviceProfileServiceActorRef.receive(DeviceProfileRequest("device-001", "xyz"))
    }
    intercept[JedisConnectionException] {
      redisServer.stop();
      deviceProfileServiceActorRef.receive(DeviceProfileRequest("device-001", "106.51.74.185"))
      redisServer.start();
    }
  }

}
