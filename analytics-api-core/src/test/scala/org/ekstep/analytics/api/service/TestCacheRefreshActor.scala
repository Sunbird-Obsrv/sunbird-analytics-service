package org.ekstep.analytics.api.service

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.TestActorRef
import org.ekstep.analytics.api.BaseSpec
import org.ekstep.analytics.api.util.{CacheUtil, DeviceLocation}
import org.mockito.Mockito.{times, verify}
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.FlatSpec
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.Matchers

class TestCacheRefreshActor extends FlatSpec with Matchers with MockitoSugar {

  implicit val config = ConfigFactory.load()
  private implicit val system: ActorSystem = ActorSystem("cache-refresh-test-actor-system", config)

  "Cache refresh actor" should "refresh the cache periodically" in {
    implicit val config: Config = ConfigFactory.load()
    val cacheUtilMock = mock[CacheUtil]
    val cacheRefreshActorRef = TestActorRef(new CacheRefreshActor(cacheUtilMock))

    cacheRefreshActorRef.tell(DeviceLocation(1234, continentName = "Asia", countryCode = "IN", countryName = "India", stateCode = "KA",
      state = "Karnataka", subDivsion2 = "", city = "Bangalore",
      stateCustom = "Karnataka", stateCodeCustom = "29", districtCustom = ""), ActorRef.noSender)

    verify(cacheUtilMock, times(1)).initDeviceLocationCache()
  }

}
