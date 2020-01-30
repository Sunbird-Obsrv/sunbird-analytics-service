package org.ekstep.analytics.api.service

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.TestActorRef
import org.ekstep.analytics.api.BaseSpec
import org.ekstep.analytics.api.util.{CacheUtil, DeviceLocation}
import org.mockito.Mockito._
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
    
    doNothing().when(cacheUtilMock).initDeviceLocationCache()
    val cacheRefreshActorRef = TestActorRef(new CacheRefreshActor(cacheUtilMock))

    cacheRefreshActorRef.underlyingActor.receive("refresh")

    verify(cacheUtilMock, atLeastOnce()).initDeviceLocationCache()
  }

}
