package org.ekstep.analytics.api.util

import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.HTTPClient
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar

class TestAPIRestUtil extends FlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {


    "APIRestUtil" should "should return the get response" in {
        val HTTPClientMock = mock[HTTPClient]
        val apiURL = AppConf.getConfig("druid.coordinator.host") + AppConf.getConfig("druid.healthcheck.url")
        when(HTTPClientMock.get[String](apiURL, None)).thenReturn("SUCCESS")
        val apiUtil = new APIRestUtil()
        val response = apiUtil.get[String](apiURL, None, HTTPClientMock)
        println(apiURL)
        println(response)
        response should be("SUCCESS")
    }

    "APIRestUtil" should "should return the post response" in {
        val HTTPClientMock = mock[HTTPClient]
        val apiURL = AppConf.getConfig("druid.coordinator.host") + AppConf.getConfig("druid.healthcheck.url")
        when(HTTPClientMock.post[String](apiURL, "", None)).thenReturn("SUCCESS")
        val apiUtil = new APIRestUtil()
        val response = apiUtil.post[String](apiURL, "", None, HTTPClientMock)
        response should be("SUCCESS")
    }
}
