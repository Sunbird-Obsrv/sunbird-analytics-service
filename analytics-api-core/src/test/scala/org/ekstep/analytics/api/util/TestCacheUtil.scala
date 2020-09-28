package org.ekstep.analytics.api.util

import java.sql.{ResultSet, Timestamp}
import java.util.Date

import com.google.common.collect.Table
import org.ekstep.analytics.api.{BaseSpec, Response}
import org.ekstep.analytics.framework.util.HTTPClient
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import com.typesafe.config.ConfigFactory

class TestCacheUtil extends FlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {

  implicit val config = ConfigFactory.load()
  val postgresDBMock = mock[PostgresDBUtil]
  val resultSetMock = mock[ResultSet]
  val restUtilMock = mock[APIRestUtil]

  val cacheUtil = new CacheUtil(postgresDBMock, restUtilMock)

  "CacheUtil" should "populate device location cache" in {
    when(postgresDBMock.readLocation(ArgumentMatchers.any())).thenReturn(List(DeviceLocation(1234, "Asia", "IN", "India", "KA", "Karnataka", "", "Bangalore", "", "29", "Bangalore")))
    when(postgresDBMock.readGeoLocationRange(ArgumentMatchers.any())).thenReturn(List(GeoLocationRange(1234, 1234, 1)))
    when(resultSetMock.next()).thenReturn(true).thenReturn(true).thenReturn(false)

    cacheUtil.initDeviceLocationCache()

    when(postgresDBMock.readGeoLocationCity(ArgumentMatchers.any())).thenThrow(new RuntimeException("something went wrong!"))
    cacheUtil.initDeviceLocationCache()
  }

  it should "cache consumer channel" in {
    when(postgresDBMock.read(ArgumentMatchers.any())).thenReturn(List(ConsumerChannel(consumerId = "Ekstep", channel = "in.ekstep", status = 0, createdBy = "System", createdOn = new Timestamp(new Date().getTime), updatedOn = new Timestamp(new Date().getTime))))

    cacheUtil.initConsumerChannelCache()
    verify(postgresDBMock, times(1)).read(ArgumentMatchers.any())

    when(postgresDBMock.read(ArgumentMatchers.any())).thenThrow(new RuntimeException("something went wrong!"))
    cacheUtil.initConsumerChannelCache()
  }

  it should "cache super admin channel" in {
      cacheUtil.getSuperAdminChannel() should be("")
      val orgRequest = """{"request":{"filters":{"channel":"sunbird"},"offset":0,"limit":1000,"fields":["id"]}}"""
      when(restUtilMock.post[Response]("https://dev.sunbirded.org/api/org/v1/search", orgRequest)).thenReturn(JSONUtils.deserialize[Response]("{\"id\":\"api.org.search\",\"ver\":\"v1\",\"ts\":\"2020-09-14 11:27:41:233+0000\",\"params\":{\"resmsgid\":null,\"msgid\":\"70ae090e-d620-4ba2-972b-865b9ea811a8\",\"err\":null,\"status\":\"success\",\"errmsg\":null},\"responseCode\":\"OK\",\"result\":{\"response\":{\"count\":1,\"content\":[{\"slug\":\"channel-admin\"}]}}}"))
      cacheUtil.initSuperAdminChannelCache()
      verify(restUtilMock, times(2)).post("https://dev.sunbirded.org/api/org/v1/search", orgRequest)
      cacheUtil.getSuperAdminChannel() should be("channel-admin")

    }

  it should "populate consumer channel table" in {
    reset(postgresDBMock)
    when(postgresDBMock.read(ArgumentMatchers.any())).thenReturn(List(ConsumerChannel(consumerId = "Ekstep", channel = "in.ekstep", status = 0, createdBy = "System", createdOn = new Timestamp(new Date().getTime), updatedOn = new Timestamp(new Date().getTime))))
    val cacheUtilSpy = spy(cacheUtil)
    cacheUtilSpy.getConsumerChannelTable()
    verify(cacheUtilSpy, times(1)).initConsumerChannelCache()

    when(postgresDBMock.read(ArgumentMatchers.any())).thenReturn(List(ConsumerChannel(consumerId = "Ekstep", channel = "in.ekstep", status = 0, createdBy = "System", createdOn = new Timestamp(new Date().getTime), updatedOn = new Timestamp(new Date().getTime))))
    val result = cacheUtilSpy.getConsumerChannelTable()
    result.isInstanceOf[Table[String, String, Integer]] should be(true)
  }
  
  it should "validate all exception branches" in {
    noException must be thrownBy {
      val cacheUtil2 = new CacheUtil(new PostgresDBUtil(), new APIRestUtil)
      cacheUtil2.initDeviceLocationCache()
    }
    
    when(postgresDBMock.readGeoLocationCity(ArgumentMatchers.any())).thenReturn(List(GeoLocationCity(geoname_id = 29, subdivision_1_name = "Karnataka", subdivision_2_custom_name = "Karnataka")))
    when(postgresDBMock.readGeoLocationRange(ArgumentMatchers.any())).thenReturn(List(GeoLocationRange(1234, 1236, 1)))
    cacheUtil.initDeviceLocationCache();
    
    val dl = IPLocationCache.getDeviceLocation(1234);
    Console.println("dl", dl);
    
    val ipLocation = IPLocationCache.getIpLocation(1234);
    Console.println("ipLocation", ipLocation);

  }

  it should "call init method" in {
    reset(postgresDBMock)
    when(postgresDBMock.readLocation(ArgumentMatchers.any())).thenReturn(List(DeviceLocation(1234, "Asia", "IN", "India", "KA", "Karnataka", "", "Bangalore", "", "29", "Bangalore")))
    when(postgresDBMock.readGeoLocationRange(ArgumentMatchers.any())).thenReturn(List(GeoLocationRange(1234, 1234, 1)))
    when(postgresDBMock.read(ArgumentMatchers.any())).thenReturn(List(ConsumerChannel(consumerId = "Ekstep", channel = "in.ekstep", status = 0, createdBy = "System", createdOn = new Timestamp(new Date().getTime), updatedOn = new Timestamp(new Date().getTime))))
    val orgRequest = """{"request":{"filters":{"channel":"sunbird"},"offset":0,"limit":1000,"fields":["id"]}}"""
    when(restUtilMock.post[Response]("https://dev.sunbirded.org/api/org/v1/search", orgRequest)).thenReturn(JSONUtils.deserialize[Response]("{\"id\":\"api.org.search\",\"ver\":\"v1\",\"ts\":\"2020-09-14 11:27:41:233+0000\",\"params\":{\"resmsgid\":null,\"msgid\":\"70ae090e-d620-4ba2-972b-865b9ea811a8\",\"err\":null,\"status\":\"success\",\"errmsg\":null},\"responseCode\":\"OK\",\"result\":{\"response\":{\"count\":1,\"content\":[{\"slug\":\"channel-admin\"}]}}}"))

    cacheUtil.init()
  }
}
