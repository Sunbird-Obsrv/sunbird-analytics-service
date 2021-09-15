package org.ekstep.analytics.api.service

import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import akka.util.Timeout
import com.google.common.collect.Table
import org.ekstep.analytics.api.{BaseSpec, RequestHeaderData}
import org.ekstep.analytics.api.util.{APIRestUtil, APIValidator, CacheUtil, EmbeddedPostgresql, PostgresDBUtil}
import org.ekstep.analytics.framework.FrameworkContext
import org.sunbird.cloud.storage.BaseStorageService

import scala.concurrent.duration._
import scala.concurrent.ExecutionContextExecutor

class TestJobAPIServiceFor500Error  extends BaseSpec  {

  implicit val mockFc = mock[FrameworkContext];
  private implicit val system: ActorSystem = ActorSystem("test-actor-system", config)
  implicit val executionContext: ExecutionContextExecutor =  scala.concurrent.ExecutionContext.global
  implicit val timeout: Timeout = 20.seconds


  it should "check for 500 internal error" in {

      val postgresUtil = new PostgresDBUtil
      val restUtilMock = mock[APIRestUtil]
      val cacheUtil = mock[CacheUtil]
      val apiValidator = new APIValidator(postgresUtil, restUtilMock, cacheUtil)
      val jobApiServiceActorRef = TestActorRef(new JobAPIService(postgresUtil, apiValidator))
      val requestHeaderData = RequestHeaderData("in.ekstep", "consumer-1", "test-1")
      intercept[Exception] {
        // submitRequest
        val request1 = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"dataset":"druid-dataset","tag":"test-tag","datasetConfig":{"type":"ml-task-detail-exhaust","params":{"programId":"program-1","state_slug":"apekx","solutionId":"solution-1"}},"encryptionKey":"test@123"}}"""
        val response = jobApiServiceActorRef.underlyingActor.dataRequest(request1, "in.ekstep", requestHeaderData)
      }
      intercept[Exception] {
        // searchRequest
        val request2 = """{"id":"ekstep.analytics.job.request.search","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"filters":{"dataset":"progress-exhaust","channel":"in.ekstep","status":"SUBMITTED"},"limit":10}}"""
        jobApiServiceActorRef.underlyingActor.searchRequest(request2)
      }
      intercept[Exception] {
        // getRequest
        jobApiServiceActorRef.underlyingActor.getDataRequest("dev-portal", "14621312DB7F8ED99BA1B16D8B430FAC", requestHeaderData)
      }
      intercept[Exception] {
        // listRequest
        jobApiServiceActorRef.underlyingActor.getDataRequestList("client-2", 10, requestHeaderData)
      }
  }
}
