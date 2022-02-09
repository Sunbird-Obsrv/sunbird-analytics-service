package org.ekstep.analytics.api.service

import java.util.Date
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.api._
import org.ekstep.analytics.api.util._
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.conf.AppConf
import org.joda.time.{DateTime, LocalDate}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.sunbird.cloud.storage.BaseStorageService

import scala.collection.immutable.List
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers
import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import akka.actor.ActorRef
import org.ekstep.analytics.api.service.{ChannelData, DataRequest, DataRequestList, GetDataRequest}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContextExecutor
import akka.util.Timeout
import com.google.common.collect.Table

class TestJobAPIService extends BaseSpec  {
  
  implicit val mockFc = mock[FrameworkContext];
  private val mockStorageService = mock[BaseStorageService]
  private implicit val system: ActorSystem = ActorSystem("test-actor-system", config)
  private val postgresUtil = new PostgresDBUtil
  private val restUtilMock = mock[APIRestUtil]
  private val cacheUtil = mock[CacheUtil]
  private val mockTable = mock[Table[String, String, Integer]];
  private val apiValidator = new APIValidator(postgresUtil, restUtilMock, cacheUtil)
  val jobApiServiceActorRef = TestActorRef(new JobAPIService(postgresUtil, apiValidator))
  implicit val executionContext: ExecutionContextExecutor =  scala.concurrent.ExecutionContext.global
  implicit val timeout: Timeout = 20.seconds

  val requestHeaderData = RequestHeaderData("in.ekstep", "consumer-1", "test-1")


  override def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedPostgresql.start()
    EmbeddedPostgresql.createTables()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    EmbeddedPostgresql.close()
  }


  "JobAPIService" should "return response for data request" in {

      when(cacheUtil.getConsumerChannelTable()).thenReturn(mockTable)
      when(mockTable.get(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(1)
      val requestHeaderData = RequestHeaderData("in.ekstep", "consumer-1", "test-1")
      val request = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"tag":"test-client","requestedBy":"test-1","dataset":"assessment-score-report","encryptionKey":"xxxxx","datasetConfig":{"batchFilter":["TPD","NCFCOPY"],"contentFilters":{"request":{"filters":{"identifier":["do_11305960936384921612216","do_1130934466492252161819"],"prevState":"Draft"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel","prevState"]}},"reportPath":"course-progress-v2/"},"outputFormat":"csv"}}"""
      val response = jobApiServiceActorRef.underlyingActor.dataRequest(request, "in.ekstep", requestHeaderData)
      response.responseCode should be("OK")

      // request with searchFilter
      val response1 = jobApiServiceActorRef.underlyingActor.dataRequest("""{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"tag":"test-client","requestedBy":"test-1","dataset":"progress-exhaust","encryptionKey":"xxxxx","datasetConfig":{"searchFilter":{"request":{"filters":{"identifier":["do_11305960936384921612216","do_1130934466492252161819"],"prevState":"Draft"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel","prevState"]}},"reportPath":"course-progress-v2/"},"outputFormat":"csv"}}""", "in.ekstep", requestHeaderData)
      response1.responseCode should be("OK")
  }

  "JobAPIService" should "return response for search  api" in {
    val request = """{"id":"ekstep.analytics.job.request.search","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"filters":{"dataset":"progress-exhaust","channel":"in.ekstep","status":"SUBMITTED"},"limit":10}}"""
    val response = jobApiServiceActorRef.underlyingActor.searchRequest(request)
    response.responseCode should be("OK")
    response.result.isEmpty should be(false)
    response.result.getOrElse(Map())("count") should be(1)
    response.result.getOrElse(Map())("jobs").asInstanceOf[List[Map[String, AnyRef]]].size should be(1)
  }


  "JobAPIService" should "fail for invalid filter field in the filter Obj" in {
    val request = """{"id":"ekstep.analytics.job.request.search","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"filters":{"job_id":"progress-exhaust"},"limit":10}}"""
    val response = jobApiServiceActorRef.underlyingActor.searchRequest(request)
    response.params.status should be("failed")
    response.params.errmsg should be("Unsupported filters")
  }

  "JobAPIService" should "return error response when filters are not available in the request" in {
    val request = """{"id":"ekstep.analytics.job.request.search","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"limit":10}}"""
    val response = jobApiServiceActorRef.underlyingActor.searchRequest(request)
    response.params.status should be("failed")
    response.params.errmsg should be ("Filters are empty")
  }

  "JobAPIService" should "return response for search api job submitted date filter " in {
    EmbeddedPostgresql.execute(
      s"""insert into job_request ("tag", "request_id", "job_id", "status", "request_data", "requested_by",
        "requested_channel", "dt_job_submitted", "encryption_key") values ('client-1:in.ekstep', 'test-score-report1-dd-mm', 'test-score-report1',
        'SUBMITTED',  '{"batchFilter":["TPD","NCFCOPY"],"contentFilters":{"request":{"filters":{"identifier":["do_11305960936384921612216","do_1130934466492252161819"],"prevState":"Draft"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel","prevState"]}},"reportPath":"course-progress-v2/"}',
        'test-1', 'in.ekstep' , '2020-09-07T13:54:39.019+05:30', 'xxxx-xxxx');""")

    val submissionDate = DateTime.now().toString("yyyy-MM-dd")
    val request = s"""{"id":"ekstep.analytics.job.request.search","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"filters":{"requestedDate": "$submissionDate"},"limit":5}}"""
    val response = jobApiServiceActorRef.underlyingActor.searchRequest(request)
    response.responseCode should be("OK")
    response.result.isEmpty should be(false)
    response.result.getOrElse(Map())("count") should be(2) // Total available requests in the DB
    response.result.getOrElse(Map())("jobs").asInstanceOf[List[Map[String, AnyRef]]].size should be(2) // Requests in the response is equal to limit
  }

  "JobAPIService" should "return response for data request when re-submitted request for already submitted job" in {

      val submissionDate = DateTime.now().toString("yyyy-MM-dd")
      val requestId1 = jobApiServiceActorRef.underlyingActor._getRequestId("client-1", "assessment-score-report", "test-1", "in.ekstep", submissionDate)

      EmbeddedPostgresql.execute(
      s"""insert into job_request ("tag", "request_id", "job_id", "status", "request_data", "requested_by",
        "requested_channel", "dt_job_submitted", "encryption_key") values ('client-1:in.ekstep', '$requestId1', 'assessment-score-report',
        'SUBMITTED',  '{"batchFilter":["TPD","NCFCOPY"],"contentFilters":{"request":{"filters":{"identifier":["do_11305960936384921612216","do_1130934466492252161819"],"prevState":"Draft"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel","prevState"]}},"reportPath":"course-progress-v2/"}',
        'test-1', 'in.ekstep' , '2020-09-07T13:54:39.019+05:30', 'xxxx-xxxx');""")

    reset(mockStorageService)
    when(mockFc.getStorageService(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn(mockStorageService);
    when(mockStorageService.getSignedURL(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn("https://sunbird.org/test/signed/file1.csv");
    doNothing().when(mockStorageService).closeContext()

    val res = jobApiServiceActorRef.underlyingActor.getDataRequest("client-1:in.ekstep", requestId1, requestHeaderData)
    res.responseCode should be("OK")
    val stringResponse = JSONUtils.serialize(res.result.get)
    stringResponse.contains("encryption_key") should be(false)
    val responseData = JSONUtils.deserialize[JobResponse](stringResponse)
    responseData.status should be("SUBMITTED")

    val request = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"tag":"client-1","requestedBy":"test-1","dataset":"assessment-score-report","datasetConfig":{"batchFilter":["TPD","NCFCOPY"],"contentFilters":{"request":{"filters":{"identifier":["do_11305960936384921612216","do_1130934466492252161819"],"prevState":"Draft"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel","prevState"]}},"reportPath":"course-progress-v2/"},"outputFormat":"csv"}}"""
    val res1 = jobApiServiceActorRef.underlyingActor.dataRequest(request, "in.ekstep", requestHeaderData)
    res1.responseCode should be("OK")
    val responseData1 = JSONUtils.deserialize[JobResponse](JSONUtils.serialize(res1.result.get))
    responseData1.status should be("SUBMITTED")
    responseData1.tag should be("client-1:in.ekstep")
  }


  "JobAPIService" should "return failed response for data request with empty tag in request" in {
    val request = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"dataset":"assessment-score-report","datasetConfig":{"batchFilter":["TPD","NCFCOPY"],"contentFilters":{"request":{"filters":{"identifier":["do_11305960936384921612216","do_1130934466492252161819"],"prevState":"Draft"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel","prevState"]}},"reportPath":"course-progress-v2/"},"outputFormat":"csv"}}"""
    val response = jobApiServiceActorRef.underlyingActor.dataRequest(request, "in.ekstep", requestHeaderData)
    response.params.status should be("failed")
    response.params.errmsg should be ("tag is empty")
  }

  "JobAPIService" should "return failed response for data request with empty dataset in request" in {
    val request = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"tag":"client-1","datasetConfig":{"batchFilter":["TPD","NCFCOPY"],"contentFilters":{"request":{"filters":{"identifier":["do_11305960936384921612216","do_1130934466492252161819"],"prevState":"Draft"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel","prevState"]}},"reportPath":"course-progress-v2/"},"outputFormat":"csv"}}"""
    val response = jobApiServiceActorRef.underlyingActor.dataRequest(request, "in.ekstep", requestHeaderData)
    response.params.status should be("failed")
    response.params.errmsg should be ("dataset is empty")
  }
  
  it should "validate the request body" in {
    var response = jobApiServiceActorRef.underlyingActor.dataRequest("""{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"tag":"client-1","dataset":"assessment-score-report","config":{"batchFilter":["TPD","NCFCOPY"],"contentFilters":{"request":{"filters":{"identifier":["do_11305960936384921612216","do_1130934466492252161819"],"prevState":"Draft"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel","prevState"]}},"reportPath":"course-progress-v2/"}}}""", "in.ekstep", requestHeaderData)
    response.params.errmsg should be ("datasetConfig is empty")
    
    response = jobApiServiceActorRef.underlyingActor.dataRequest("""{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"dataset":"assessment-score-report","datasetConfig":{"batchFilter":["TPD","NCFCOPY"],"contentFilters":{"request":{"filters":{"identifier":["do_11305960936384921612216","do_1130934466492252161819"],"prevState":"Draft"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel","prevState"]}},"reportPath":"course-progress-v2/"},"outputFormat":"csv"}}""", "in.ekstep", requestHeaderData)
    response.params.errmsg should be ("tag is empty")

    response = jobApiServiceActorRef.underlyingActor.dataRequest("""{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"tag":"client-1","datasetConfig":{"batchFilter":["TPD","NCFCOPY"],"contentFilters":{"request":{"filters":{"identifier":["do_11305960936384921612216","do_1130934466492252161819"],"prevState":"Draft"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel","prevState"]}},"reportPath":"course-progress-v2/"},"outputFormat":"csv"}}""", "in.ekstep", requestHeaderData)
    response.params.errmsg should be ("dataset is empty")

  }

  it should "return response for get data request" in {
    val response = jobApiServiceActorRef.underlyingActor.getDataRequest("dev-portal", "14621312DB7F8ED99BA1B16D8B430FAC", requestHeaderData)
    response.responseCode should be("OK")
  }

  it should "return the list of jobs in descending order" in {

    EmbeddedPostgresql.execute(
      s"""insert into job_request ("tag", "request_id", "job_id", "status", "request_data", "requested_by",
        "requested_channel", "dt_job_submitted", "dt_job_completed", "download_urls", "dt_file_created", "execution_time") values ('client-2', '462CDD1241226D5CA2E777DA522691EF', 'assessment-score-report',
        'SUCCESS',  '{"batchFilter":["TPD","NCFCOPY"],"contentFilters":{"request":{"filters":{"identifier":["do_11305960936384921612216","do_1130934466492252161819"],"prevState":"Draft"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel","prevState"]}},"reportPath":"course-progress-v2/"}',
        'test-1', 'in.ekstep' , '2020-09-07T13:54:39.019+05:30', '2020-09-08T13:54:39.019+05:30', '{"https://sunbird.org/test/signed/file1.csv", "https://sunbird.org/test/signed/file2.csv"}', '2020-09-08T13:50:39.019+05:30', '10');""")

    EmbeddedPostgresql.execute(
      s"""insert into job_request ("tag", "request_id", "job_id", "status", "request_data", "requested_by",
        "requested_channel", "dt_job_submitted", "dt_job_completed", "download_urls", "dt_file_created", "execution_time") values ('client-2', '562CDD1241226D5CA2E777DA522691EF', 'assessment-score-report',
        'SUCCESS',  '{"batchFilter":["TPD","NCFCOPY"],"contentFilters":{"request":{"filters":{"identifier":["do_1130596093638492161","do_1130934466492252169"],"prevState":"Draft"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel","prevState"]}},"reportPath":"course-progress-v2/"}',
        'test-1', 'in.ekstep' , '2020-09-07T13:55:39.019+05:30', '2020-09-08T14:54:39.019+05:30', '{"https://sunbird.org/test/signed/file1.csv", "https://sunbird.org/test/signed/file2.csv"}', '2020-09-08T13:53:39.019+05:30', '5');""")

    reset(mockStorageService)
    when(mockFc.getStorageService(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn(mockStorageService);
    when(mockStorageService.getSignedURL(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn("https://sunbird.org/test/signed/file1.csv");
    doNothing().when(mockStorageService).closeContext()

    val res = jobApiServiceActorRef.underlyingActor.getDataRequestList("client-2", 10, requestHeaderData)
    val resultMap = res.result.get
    val jobRes = JSONUtils.deserialize[List[JobResponse]](JSONUtils.serialize(resultMap.get("jobs").get))
    jobRes.length should be(2)

    // fetch data with limit less than the number of record available
    val res2 = jobApiServiceActorRef.underlyingActor.getDataRequestList("client-2", 1, requestHeaderData)
    val resultMap2 = res2.result.get
    val jobRes2 = JSONUtils.deserialize[List[JobResponse]](JSONUtils.serialize(resultMap2.get("jobs").get))
    jobRes2.length should be(1)

    // trying to fetch the record with a key for which data is not available
    val res1 = jobApiServiceActorRef.underlyingActor.getDataRequestList("testKey", 10, requestHeaderData)
    val resultMap1 = res1.result.get.asInstanceOf[Map[String, AnyRef]]
    resultMap1.get("count").get.asInstanceOf[Int] should be(0)
  }

  it should "re-submit job if it is already completed" in {

    val submissionDate = DateTime.now().toString("yyyy-MM-dd")
    val requestId1 = jobApiServiceActorRef.underlyingActor._getRequestId("client-3", "assessment-score-report", "test-1", "in.ekstep", submissionDate)
    val requestId2 = jobApiServiceActorRef.underlyingActor._getRequestId("client-3", "assessment-score-report", "test-2", "in.ekstep", submissionDate)
    EmbeddedPostgresql.execute(
      s"""insert into job_request ("tag", "request_id", "job_id", "status", "request_data", "requested_by",
        "requested_channel", "dt_job_submitted", "dt_job_completed", "download_urls", "dt_file_created", "execution_time", "iteration") values ('client-3:in.ekstep', '$requestId1', 'assessment-score-report',
        'FAILED',  '{"batchFilter":["TPD","NCFCOPY"],"contentFilters":{"request":{"filters":{"identifier":["do_11305960936384921612216","do_1130934466492252161819"],"prevState":"Draft"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel","prevState"]}},"reportPath":"course-progress-v2/"}',
        'test-1', 'in.ekstep' , '2020-09-07T13:54:39.019+05:30', '2020-09-08T13:54:39.019+05:30', '{"https://sunbird.org/test/signed/file1.csv", "https://sunbird.org/test/signed/file2.csv"}', '2020-09-08T13:50:39.019+05:30', '10', '0');""")

    EmbeddedPostgresql.execute(
      s"""insert into job_request ("tag", "request_id", "job_id", "status", "request_data", "requested_by",
        "requested_channel", "dt_job_submitted", "dt_job_completed", "download_urls", "dt_file_created", "execution_time", "iteration") values ('client-3:in.ekstep', '$requestId2', 'assessment-score-report',
        'SUCCESS',  '{"batchFilter":["TPD","NCFCOPY"],"contentFilters":{"request":{"filters":{"identifier":["do_11305960936384921612216","do_1130934466492252161819"],"prevState":"Draft"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel","prevState"]}},"reportPath":"course-progress-v2/"}',
        'test-2', 'in.ekstep' , '2020-09-07T13:54:39.019+05:30', '2020-09-08T13:54:39.019+05:30', '{"https://sunbird.org/test/signed/file1.csv", "https://sunbird.org/test/signed/file2.csv"}', '2020-09-08T13:50:39.019+05:30', '10', '0');""")

    reset(mockStorageService)
    when(mockFc.getStorageService(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn(mockStorageService);
    when(mockStorageService.getSignedURL(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn("https://sunbird.org/test/signed/file1.csv");
    doNothing().when(mockStorageService).closeContext()

    val res = jobApiServiceActorRef.underlyingActor.getDataRequest("client-3:in.ekstep", requestId1, requestHeaderData)
    res.responseCode should be("OK")
    val responseData = JSONUtils.deserialize[JobResponse](JSONUtils.serialize(res.result.get))
    responseData.downloadUrls.get.size should be(2)
    responseData.status should be("FAILED")
    responseData.tag should be("client-3:in.ekstep")

    // without encryption key
    val request = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"tag":"client-3","requestedBy":"test-1","dataset":"assessment-score-report","datasetConfig":{"batchFilter":["TPD","NCFCOPY"],"contentFilters":{"request":{"filters":{"identifier":["do_11305960936384921612216","do_1130934466492252161819"],"prevState":"Draft"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel","prevState"]}},"reportPath":"course-progress-v2/"},"outputFormat":"csv"}}"""
    val res1 = jobApiServiceActorRef.underlyingActor.dataRequest(request, "in.ekstep", requestHeaderData)
    res1.responseCode should be("OK")
    val responseData1 = JSONUtils.deserialize[JobResponse](JSONUtils.serialize(res1.result.get))
    responseData1.status should be("SUBMITTED")
    responseData1.tag should be("client-3:in.ekstep")

      // with encryption key
    val request2 = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"tag":"client-3","requestedBy":"test-2","dataset":"assessment-score-report","encryptionKey":"xxxxx","datasetConfig":{"batchFilter":["TPD","NCFCOPY"],"contentFilters":{"request":{"filters":{"identifier":["do_11305960936384921612216","do_1130934466492252161819"],"prevState":"Draft"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel","prevState"]}},"reportPath":"course-progress-v2/"},"outputFormat":"csv"}}"""
    val res2 = jobApiServiceActorRef.underlyingActor.dataRequest(request2, "in.ekstep", requestHeaderData)
    res2.responseCode should be("OK")
    val responseData2 = JSONUtils.deserialize[JobResponse](JSONUtils.serialize(res2.result.get))
    responseData2.status should be("SUCCESS")
    responseData2.tag should be("client-3:in.ekstep")

  }

  "JobAPIService" should "return different request id for same tag having different requested channel" in {
    val request1 = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"tag":"client-2","requestedBy":"test-1","dataset":"assessment-score-report","datasetConfig":{"batchFilter":["TPD","NCFCOPY"],"contentFilters":{"request":{"filters":{"identifier":["do_11305960936384921612216","do_1130934466492252161819"],"prevState":"Draft"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel","prevState"]}},"reportPath":"course-progress-v2/"},"outputFormat":"csv"}}"""
    val response1 = jobApiServiceActorRef.underlyingActor.dataRequest(request1, "test-channel-1", requestHeaderData)
    val request2 = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"tag":"client-2","requestedBy":"test-1","dataset":"assessment-score-report","datasetConfig":{"batchFilter":["TPD","NCFCOPY"],"contentFilters":{"request":{"filters":{"identifier":["do_11305960936384921612216","do_1130934466492252161819"],"prevState":"Draft"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel","prevState"]}},"reportPath":"course-progress-v2/"},"outputFormat":"csv"}}"""
    val response2 = jobApiServiceActorRef.underlyingActor.dataRequest(request2, "test-channel-2", requestHeaderData)
    response2.result.head.get("requestId").get should not be (response1.result.head.get("requestId").get)

  }

  //  // Channel Exhaust Test Cases
  //  // -ve Test cases
  it should "return response for default datasetId if we set `datasetID` other than valid" in {

      reset(mockStorageService)
      when(mockFc.getStorageService(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn(mockStorageService);
      when(mockStorageService.upload(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn("");
      when(mockStorageService.getSignedURL(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn("");
      when(mockStorageService.searchObjectkeys(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn(List());
      doNothing().when(mockStorageService).closeContext()

      val datasetId = "test"
      val resObj = jobApiServiceActorRef.underlyingActor.getChannelData("in.ekstep", datasetId, Option("2018-05-14"), Option("2018-05-15"))
      resObj.responseCode should be("OK")
      val res = resObj.result.getOrElse(Map())
      val urls = res.get("files").get.asInstanceOf[List[String]];
      urls.size should be (0)
  }

  it should "return a CLIENT_ERROR in the response if 'fromDate' is empty and taking previous day by default" in {
    val resObj = jobApiServiceActorRef.underlyingActor.getChannelData("in.ekstep", "raw", None, Option("2018-05-15"))
    resObj.responseCode should be("CLIENT_ERROR")
    resObj.params.errmsg should be("Date range should not be -ve. Please check your 'from' & 'to'")
  }

  it should "return a CLIENT_ERROR in the response if 'endDate' is empty older than fromDate" in {
    val toDate = "2018-05-10"
    val resObj = jobApiServiceActorRef.underlyingActor.getChannelData("in.ekstep", "raw", Option("2018-05-15"), Option(toDate))
    resObj.responseCode should be("CLIENT_ERROR")
    resObj.params.errmsg should be("Date range should not be -ve. Please check your 'from' & 'to'")
  }

  it should "return a CLIENT_ERROR in the response if 'endDate' is a future date" in {
    val toDate = new LocalDate().plusDays(1).toString()
    val resObj = jobApiServiceActorRef.underlyingActor.getChannelData("in.ekstep", "raw", Option("2018-05-15"), Option(toDate))
    resObj.responseCode should be("CLIENT_ERROR")
    resObj.params.errmsg should be("'to' should be LESSER OR EQUAL TO today's date..")
  }
  //
  it should "return a CLIENT_ERROR in the response if date_range > 10" in {
    val toDate = new LocalDate().toString()
    val fromDate = new LocalDate().minusDays(11).toString()

    val resObj = jobApiServiceActorRef.underlyingActor.getChannelData("in.ekstep", "raw", Option(fromDate), Option(toDate))
    resObj.responseCode should be("CLIENT_ERROR")
    resObj.params.errmsg should be("Date range should be < 10 days")
  }
  //
  //  // +ve test cases
  //
  ignore should "return a successfull response if 'to' is empty" in {
    val resObj = jobApiServiceActorRef.underlyingActor.getChannelData("in.ekstep", "raw", Option("2018-05-20"), None)
    resObj.responseCode should be("OK")
  }

  ignore should "return a successfull response if datasetID is valid - S3" in {
    val datasetId = "raw"
    val resObj = jobApiServiceActorRef.underlyingActor.getChannelData("in.ekstep", datasetId, Option("2018-05-20"), Option("2018-05-21"))
    resObj.responseCode should be("OK")
  }

  it should "get the channel data for raw data" in {
    
    reset(mockStorageService)
    when(mockFc.getStorageService(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn(mockStorageService);
    when(mockStorageService.upload(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn("");
    when(mockStorageService.getSignedURL(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn("");
    when(mockStorageService.searchObjectkeys(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn(List());
    doNothing().when(mockStorageService).closeContext()
    
    val resObj = jobApiServiceActorRef.underlyingActor.getChannelData("in.ekstep", "raw", Option("2018-05-20"), Option("2018-05-20"))
    resObj.responseCode should be("OK")
    val res = resObj.result.getOrElse(Map())
    val urls = res.get("files").get.asInstanceOf[List[String]];
    urls.size should be (0)
    val periodWiseFiles = res.get("periodWiseFiles").get.asInstanceOf[Map[String,List[String]]];
    periodWiseFiles.size should be (0)
  }
  
  it should "get the channel data for summary data" in {
    
    reset(mockStorageService)
    when(mockFc.getStorageService(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn(mockStorageService);
    when(mockStorageService.upload(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn("");
    when(mockStorageService.getSignedURL(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn("https://sunbird.org/test/signed/2018-05-20.json");
    when(mockStorageService.searchObjectkeys(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn(List("https://sunbird.org/test/2018-05-20.json"));
    doNothing().when(mockStorageService).closeContext()
    
    val resObj = jobApiServiceActorRef.underlyingActor.getChannelData("in.ekstep", "raw", Option("2018-05-20"), Option("2018-05-20"))
    resObj.responseCode should be("OK")
    val res = resObj.result.getOrElse(Map())
    val urls = res.get("files").get.asInstanceOf[List[String]];
    urls.size should be (1)
    urls.head should be ("https://sunbird.org/test/signed/2018-05-20.json")
    val periodWiseFiles = res.get("periodWiseFiles").get.asInstanceOf[Map[String,List[String]]];
    periodWiseFiles.size should be (1)
    periodWiseFiles.get("2018-05-20").get.head should be ("https://sunbird.org/test/signed/2018-05-20.json")
    
  }

  it should "get the channel data for summary rollup data" in {

    reset(mockStorageService)
    when(mockFc.getStorageService(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn(mockStorageService);
    when(mockStorageService.upload(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn("");
    when(mockStorageService.getSignedURL(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn("https://sunbird.org/test/signed");
    when(mockStorageService.searchObjectkeys(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn(List("https://sunbird.org/test"));
    doNothing().when(mockStorageService).closeContext()

    val resObj = jobApiServiceActorRef.underlyingActor.getChannelData("in.ekstep", "summary-rollup", Option("2018-05-20"), Option("2018-05-20"))
    resObj.responseCode should be("OK")
    val res = resObj.result.getOrElse(Map())
    val urls = res.get("files").get.asInstanceOf[List[String]];
    urls.size should be (1)
    urls.head should be ("https://sunbird.org/test/signed")

  }

  it should "cover all cases for summary rollup channel data" in {

    reset(mockStorageService)
    when(mockFc.getStorageService(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn(mockStorageService);
    when(mockStorageService.upload(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn("");
    when(mockStorageService.getSignedURL(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn("");
    when(mockStorageService.searchObjectkeys(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn(List());
    doNothing().when(mockStorageService).closeContext()

    val resObj1 = jobApiServiceActorRef.underlyingActor.getChannelData("in.ekstep", "summary-rollup", Option("2018-05-20"), Option("2018-05-20"))
    resObj1.responseCode should be("OK")
    val res1 = resObj1.result.getOrElse(Map())
    val urls1 = res1.get("files").get.asInstanceOf[List[String]];
    urls1.size should be (0)

    val resObj2 = jobApiServiceActorRef.underlyingActor.getChannelData("in.ekstep", "summary-rollup", Option("2018-05-20"), Option("9999-05-20"))
    resObj2.responseCode should be("CLIENT_ERROR")
    resObj2.params.errmsg should be("'to' should be LESSER OR EQUAL TO today's date..")

    val resObj3 = jobApiServiceActorRef.underlyingActor.getChannelData("in.ekstep", "summary-rollup", Option("2018-05-10"), Option("2018-05-30"))
    resObj3.responseCode should be("CLIENT_ERROR")
    resObj3.params.errmsg should be("Date range should be < 10 days")

    val resObj4 = jobApiServiceActorRef.underlyingActor.getChannelData("in.ekstep", "summary-rollup", Option("2018-06-20"), Option("2018-05-30"))
    resObj4.responseCode should be("CLIENT_ERROR")
    resObj4.params.errmsg should be("Date range should not be -ve. Please check your 'from' & 'to'")
  }
  
  it should "test all exception branches" in {
    import akka.pattern.ask
    val toDate = Option(new LocalDate().toString())
    val fromDate = Option(new LocalDate().minusDays(11).toString())
    var result = Await.result((jobApiServiceActorRef ? ChannelData("in.ekstep", "raw", fromDate, toDate, None, config)).mapTo[Response], 20.seconds)
    result.responseCode should be("CLIENT_ERROR")
    result.params.errmsg should be("Date range should be < 10 days")

    result = Await.result((jobApiServiceActorRef ? ChannelData("in.ekstep", "summary-rollup", fromDate, toDate, None, config)).mapTo[Response], 20.seconds)
    result.responseCode should be("CLIENT_ERROR")
    result.params.errmsg should be("Date range should be < 10 days")

    val request1 = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"requestedBy":"test-1","dataset":"course-progress-report","datasetConfig":{"batchFilter":["TPD","NCFCOPY"],"contentFilters":{"request":{"filters":{"identifier":["do_11305960936384921612216","do_1130934466492252161819"],"prevState":"Draft"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel","prevState"]}},"reportPath":"course-progress-v2/"},"outputFormat":"csv"}}"""
    result = Await.result((jobApiServiceActorRef ? DataRequest(request1, requestHeaderData, "in.ekstep", config)).mapTo[Response], 20.seconds)
    result.responseCode should be("CLIENT_ERROR")

    result = Await.result((jobApiServiceActorRef ? GetDataRequest("test-tag-1", "14621312DB7F8ED99BA1B16D8B430FAC", requestHeaderData, config)).mapTo[Response], 20.seconds)
    result.responseCode should be("OK")

    result = Await.result((jobApiServiceActorRef ? DataRequestList("client-4", 2, requestHeaderData, config)).mapTo[Response], 20.seconds)
    val resultMap = result.result.get
    val jobRes = JSONUtils.deserialize[List[JobResponse]](JSONUtils.serialize(resultMap.get("jobs").get))
    jobRes.length should be(0)

    val fromDateforPublicExhaust = new LocalDate().minusDays(31).toString()
    result = Await.result((jobApiServiceActorRef ? PublicData("summary-rollup", Option(fromDateforPublicExhaust), toDate, None, None, None, config)).mapTo[Response], 20.seconds)
    result.responseCode should be("CLIENT_ERROR")
    result.params.errmsg should be("Date range should be < 30 days")

    val addDatasetRequest = """{"id":"ekstep.analytics.dataset.add","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"datasetConfig":{},"datasetType":"Public Data Exhaust","visibility":"public","version":"v1","authorizedRoles":["public"],"sampleRequest":"curl -X GET 'https://domain_name/api/dataset/get/public-data-exhaust?date_range=LAST_7_DAYS'","sampleResponse":"{\"id\":\"org.ekstep.analytics.public.telemetry.exhaust\",\"ver\":\"1.0\",\"ts\":\"2021-04-19T06:04:49.891+00:00\",\"params\":{\"resmsgid\":\"cc2b1053-ddcf-4ee1-a12e-d17212677e6e\",\"status\":\"successful\",\"client_key\":null},\"responseCode\":\"OK\",\"result\":{\"files\":[\"https://data.domain_name/datasets/public-data-exhaust/2021-04-14.zip\"],\"periodWiseFiles\":{\"2021-04-14\":[\"https://data.domain_name/datasets/public-data-exhaust/2021-04-14.zip\"]}}}"}}"""
    result = Await.result((jobApiServiceActorRef ? AddDataSet(addDatasetRequest, config)).mapTo[Response], 20.seconds)
    result.responseCode should be("CLIENT_ERROR")
    result.params.errmsg should be("dataset is empty")

    result = Await.result((jobApiServiceActorRef ? ListDataSet(config)).mapTo[Response], 20.seconds)
    result.responseCode should be("OK")

    val searchRequest =  """{"id":"ekstep.analytics.dataset.request.search","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"filters":{"dataset":"progress-exhaust","channel":"in.ekstep","status":"SUBMITTED"},"limit":10}}"""
    result = Await.result((jobApiServiceActorRef ? SearchRequest(searchRequest, config)).mapTo[Response], 20.seconds)
    result.responseCode should be("OK")
  }

  it should "get the public exhaust files for summary rollup data" in {

    val fromDate = CommonUtil.getPreviousDay()
    val toDate = CommonUtil.getToday()

    reset(mockStorageService)
    when(mockFc.getStorageService(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn(mockStorageService);
    when(mockStorageService.searchObjectkeys(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn(List(s"summary-rollup/$fromDate.csv"));
    doNothing().when(mockStorageService).closeContext()

    val resObj1 = jobApiServiceActorRef.underlyingActor.getPublicData("summary-rollup", Option(fromDate), Option(toDate))
    resObj1.responseCode should be("OK")
    val res1 = resObj1.result.getOrElse(Map())
    val urls1 = res1.get("files").get.asInstanceOf[List[String]];
    urls1.size should be (1)
    urls1.head should be (s"https://cdn.abc.com/ekstep-dev-data-store/summary-rollup/$fromDate.csv")

    val resObj2 = jobApiServiceActorRef.underlyingActor.getPublicData("summary-rollup", Option(fromDate), Option("9999-05-20"))
    resObj2.responseCode should be("CLIENT_ERROR")
    resObj2.params.errmsg should be("'to' should be LESSER OR EQUAL TO today's date..")

    reset(mockStorageService)
    when(mockFc.getStorageService(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn(mockStorageService);
    when(mockStorageService.searchObjectkeys(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn(List());
    doNothing().when(mockStorageService).closeContext()

    // Test for no files available condition
    val resObj3 = jobApiServiceActorRef.underlyingActor.getPublicData("summary-rollup", Option(fromDate), Option(toDate))
    resObj3.responseCode should be("OK")
    val res3 = resObj3.result.getOrElse(Map())
    res3.get("message").get should be("Files are not available for requested date. Might not yet generated. Please come back later")

    // Test for invalid datasetId
    val resObj4 = jobApiServiceActorRef.underlyingActor.getPublicData("telemetry-rollup", Option(fromDate), Option(toDate))
    resObj4.responseCode should be("CLIENT_ERROR")
    resObj4.params.errmsg should be("Provided dataset is invalid. Please use any one from this list - [summary-rollup]")

    // Test for older date range
    val resObj5 = jobApiServiceActorRef.underlyingActor.getPublicData("summary-rollup", Option("2010-05-26"), Option("2010-05-26"))
    resObj5.responseCode should be("CLIENT_ERROR")
    resObj5.params.errmsg should be("Date range cannot be older than 2 months")

    // Test for provided date field
    reset(mockStorageService)
    when(mockFc.getStorageService(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn(mockStorageService);
    when(mockStorageService.searchObjectkeys(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn(List(s"summary-rollup/$fromDate.csv"));
    doNothing().when(mockStorageService).closeContext()

    val resObj6 = jobApiServiceActorRef.underlyingActor.getPublicData("summary-rollup", None, None, None, Option(fromDate))
    resObj6.responseCode should be("OK")
    val res6 = resObj6.result.getOrElse(Map())
    val urls6 = res6.get("files").get.asInstanceOf[List[String]];
    urls6.size should be (1)
    urls6.head should be (s"https://cdn.abc.com/ekstep-dev-data-store/summary-rollup/$fromDate.csv")

    // Test for provided date_range field
    val from = CommonUtil.getPreviousDay()
    val to = CommonUtil.dateFormat.print(new DateTime().minusDays(2))

    reset(mockStorageService)
    when(mockFc.getStorageService(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn(mockStorageService);
    when(mockStorageService.searchObjectkeys(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn(List(s"summary-rollup/$from.csv", s"summary-rollup/$to.csv"));
    doNothing().when(mockStorageService).closeContext()

    val resObj7 = jobApiServiceActorRef.underlyingActor.getPublicData("summary-rollup", None, None, None, None, Option("LAST_2_DAYS"))
    resObj7.responseCode should be("OK")
    val res7 = resObj7.result.getOrElse(Map())
    val urls7 = res7.get("files").get.asInstanceOf[List[String]];
    urls7.size should be (2)
    urls7.head should be (s"https://cdn.abc.com/ekstep-dev-data-store/summary-rollup/$from.csv")

    // Test for invalid date_range field
    val resObj8 = jobApiServiceActorRef.underlyingActor.getPublicData("summary-rollup", None, None, None, None, Option("LAST_20_DAYS"))
    resObj8.responseCode should be("CLIENT_ERROR")
    resObj8.params.errmsg should be("Provided dateRange LAST_20_DAYS is not valid. Please use any one from this list - List(LAST_DAY, LAST_2_DAYS, LAST_7_DAYS, LAST_14_DAYS, LAST_30_DAYS, LAST_WEEK)")

  }

  it should "add dataset and cover all cases" in {

    val submissionDate = DateTime.now().toString("yyyy-MM-dd")

    EmbeddedPostgresql.execute(
      s"""truncate table dataset_metadata;""")
    EmbeddedPostgresql.execute(
      s"""insert into dataset_metadata ("dataset_id", "dataset_sub_id", "dataset_config", "visibility", "dataset_type", "version",
          "authorized_roles", "available_from", "sample_request", "sample_response")
          values ('progress-exhaust', 'progress-exhaust', '{"batchFilter":[],"contentFilters":{"request":{"filters":{"identifier":"","prevState":""},"sort_by":{"created_on":"desc"},"limit":100,"fields":[]}},"reportPath":"/test","output_format":"csv"}',
           'private', 'On-Demand', '1.0', '{"portal"}', '$submissionDate', '', '');""")

    reset(mockStorageService)
    when(mockFc.getStorageService(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn(mockStorageService);
    doNothing().when(mockStorageService).closeContext()

    val request1 = """{"id":"ekstep.analytics.dataset.add","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"dataset":"progress-exhaust","datasetConfig":{"batchFilter":[],"contentFilters":{"request":{"filters":{"identifier":"","prevState":""},"sort_by":{"created_on":"desc"},"limit":100,"fields":[]}},"reportPath":"/test","output_format":"csv"},"datasetType":"non-druid","visibility":"private","version":"v1","authorizedRoles":["ORG_ADMIN","REPORT_ADMIN","CONTENT_CREATOR","COURSE_MENTOR"],"validationJson":{},"supportedFormats":["csv"],"exhaustType":"On-demand exhaust"}}"""
    val res1 = jobApiServiceActorRef.underlyingActor.addDataSet(request1)
    res1.responseCode should be("OK")
    val stringResponse1 = JSONUtils.serialize(res1.result.get)
    stringResponse1.contains("Dataset progress-exhaust added successfully") should be(true)

    val request2 = """{"id":"ekstep.analytics.dataset.add","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"dataset":"response-exhaust","datasetConfig":{"batchFilter":[],"contentFilters":{"request":{"filters":{"identifier":"","prevState":""},"sort_by":{"created_on":"desc"},"limit":100,"fields":[]}},"reportPath":"/test","output_format":"csv"},"datasetType":"non-druid","visibility":"private","version":"v1","authorizedRoles":["ORG_ADMIN","REPORT_ADMIN","CONTENT_CREATOR","COURSE_MENTOR"],"availableFrom":"2021-01-01","supportedFormats":["csv"],"exhaustType":"On-demand exhaust"}}"""
    val res2 = jobApiServiceActorRef.underlyingActor.addDataSet(request2)
    res2.responseCode should be("OK")
    val stringResponse2 = JSONUtils.serialize(res2.result.get)
    stringResponse2.contains("Dataset response-exhaust added successfully") should be(true)

    val request3 = """{"id":"ekstep.analytics.dataset.add","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"dataset":"public-data-exhaust","datasetConfig":{},"datasetType":"non-druid","visibility":"public","version":"v1","authorizedRoles":["public"],"sampleRequest":"curl -X GET 'https://domain_name/api/dataset/get/public-data-exhaust?date_range=LAST_7_DAYS'","sampleResponse":"{\"id\":\"org.ekstep.analytics.public.telemetry.exhaust\",\"ver\":\"1.0\",\"ts\":\"2021-04-19T06:04:49.891+00:00\",\"params\":{\"resmsgid\":\"cc2b1053-ddcf-4ee1-a12e-d17212677e6e\",\"status\":\"successful\",\"client_key\":null},\"responseCode\":\"OK\",\"result\":{\"files\":[\"https://data.domain_name/datasets/public-data-exhaust/2021-04-14.zip\"],\"periodWiseFiles\":{\"2021-04-14\":[\"https://data.domain_name/datasets/public-data-exhaust/2021-04-14.zip\"]}}}","supportedFormats":["csv"],"exhaustType":"Public exhaust"}}"""
    val res3 = jobApiServiceActorRef.underlyingActor.addDataSet(request3)
    res3.responseCode should be("OK")
    val stringResponse3 = JSONUtils.serialize(res3.result.get)
    stringResponse3.contains("Dataset public-data-exhaust added successfully") should be(true)

    val request = """{"id":"ekstep.analytics.dataset.add","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"dataset":"druid-dataset","datasetSubId":"ml-task-detail-exhaust","datasetConfig":{},"datasetType":"druid","visibility":"public","version":"v1","authorizedRoles":["PROGRAM_MANAGER","PROGRAM_DESIGNER"],"druidQuery":{},"supportedFormats":["csv"],"exhaustType":"On-demand Exhaust"}}"""
    val res = jobApiServiceActorRef.underlyingActor.addDataSet(request)
    res.responseCode should be("OK")
    val stringResponse = JSONUtils.serialize(res.result.get)
    stringResponse.contains("Dataset ml-task-detail-exhaust added successfully") should be(true)

    val res4 = jobApiServiceActorRef.underlyingActor.listDataSet()
    res4.responseCode should be("OK")
    val resultMap = res4.result.get
    val datasetsRes = JSONUtils.deserialize[List[DatasetResponse]](JSONUtils.serialize(resultMap.get("datasets").get))
    datasetsRes.length should be(4)

    // Missing datasetId
    val request5 = """{"id":"ekstep.analytics.dataset.add","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"datasetConfig":{},"datasetType":"Public Data Exhaust","visibility":"public","version":"v1","authorizedRoles":["public"],"sampleRequest":"curl -X GET 'https://domain_name/api/dataset/get/public-data-exhaust?date_range=LAST_7_DAYS'","sampleResponse":"{\"id\":\"org.ekstep.analytics.public.telemetry.exhaust\",\"ver\":\"1.0\",\"ts\":\"2021-04-19T06:04:49.891+00:00\",\"params\":{\"resmsgid\":\"cc2b1053-ddcf-4ee1-a12e-d17212677e6e\",\"status\":\"successful\",\"client_key\":null},\"responseCode\":\"OK\",\"result\":{\"files\":[\"https://data.domain_name/datasets/public-data-exhaust/2021-04-14.zip\"],\"periodWiseFiles\":{\"2021-04-14\":[\"https://data.domain_name/datasets/public-data-exhaust/2021-04-14.zip\"]}}}"}}"""
    val res5 = jobApiServiceActorRef.underlyingActor.addDataSet(request5)
    res5.responseCode should be("CLIENT_ERROR")
    res5.params.errmsg should be("dataset is empty")

    // Missing datasetType
    val request7 = """{"id":"ekstep.analytics.dataset.add","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"dataset":"progress-exhaust","datasetConfig":{"batchFilter":[],"contentFilters":{"request":{"filters":{"identifier":"","prevState":""},"sort_by":{"created_on":"desc"},"limit":100,"fields":[]}},"reportPath":"/test","output_format":"csv"},"visibility":"private","version":"v1","authorizedRoles":["portal"]}}"""
    val res7 = jobApiServiceActorRef.underlyingActor.addDataSet(request7)
    res7.responseCode should be("CLIENT_ERROR")
    res7.params.errmsg should be("datasetType is empty")

    // Missing version
    val request8 = """{"id":"ekstep.analytics.dataset.add","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"dataset":"response-exhaust","datasetConfig":{"batchFilter":[],"contentFilters":{"request":{"filters":{"identifier":"","prevState":""},"sort_by":{"created_on":"desc"},"limit":100,"fields":[]}},"reportPath":"/test","output_format":"csv"},"datasetType":"on-demand exhaust","visibility":"private","authorizedRoles":["portal","app"],"availableFrom":"2021-01-01"}}"""
    val res8 = jobApiServiceActorRef.underlyingActor.addDataSet(request8)
    res8.responseCode should be("CLIENT_ERROR")
    res8.params.errmsg should be("version is empty")

    // Missing visibility
    val request9 = """{"id":"ekstep.analytics.dataset.add","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"dataset":"response-exhaust","datasetConfig":{"batchFilter":[],"contentFilters":{"request":{"filters":{"identifier":"","prevState":""},"sort_by":{"created_on":"desc"},"limit":100,"fields":[]}},"reportPath":"/test","output_format":"csv"},"datasetType":"on-demand exhaust","version":"v1","authorizedRoles":["portal","app"],"availableFrom":"2021-01-01"}}"""
    val res9 = jobApiServiceActorRef.underlyingActor.addDataSet(request9)
    res9.responseCode should be("CLIENT_ERROR")
    res9.params.errmsg should be("visibility is empty")

    // Missing authorizedRoles
    val request10 = """{"id":"ekstep.analytics.dataset.add","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"dataset":"response-exhaust","datasetConfig":{"batchFilter":[],"contentFilters":{"request":{"filters":{"identifier":"","prevState":""},"sort_by":{"created_on":"desc"},"limit":100,"fields":[]}},"reportPath":"/test","output_format":"csv"},"datasetType":"on-demand exhaust","visibility":"private","version":"v1","availableFrom":"2021-01-01"}}"""
    val res10 = jobApiServiceActorRef.underlyingActor.addDataSet(request10)
    res10.responseCode should be("CLIENT_ERROR")
    res10.params.errmsg should be("authorizedRoles is empty")
  }

  it should "check data request for druid datasets" in {
    val request1 = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"dataset":"druid-dataset","tag":"test-tag","datasetConfig":{"type":"ml-task-detail-exhaust","params":{"programId":"program-1","state_slug":"apekx","solutionId":"solution-1"}},"encryptionKey":"test@123"}}"""
    val response1 = jobApiServiceActorRef.underlyingActor.dataRequest(request1, "in.ekstep", requestHeaderData)
    response1.responseCode should be("OK")
    val responseData1 = JSONUtils.deserialize[JobResponse](JSONUtils.serialize(response1.result.get))
    responseData1.status should be("SUBMITTED")
    responseData1.dataset should be("druid-dataset")

    val request2 = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"dataset":"druid-dataset","tag":"test-tag","datasetConfig":{"type":"ml-obs-question-detail-exhaust","params":{"programId":"program-1","state_slug":"apekx","solutionId":"solution-2"}},"encryptionKey":"test@123"}}"""
    val response2 = jobApiServiceActorRef.underlyingActor.dataRequest(request2, "in.ekstep", requestHeaderData)
    response2.responseCode should be("OK")
    val responseData2 = JSONUtils.deserialize[JobResponse](JSONUtils.serialize(response2.result.get))
    responseData2.status should be("SUBMITTED")
    responseData1.dataset should be("druid-dataset")
  }

  it should "check for data request validation" in {

    reset(cacheUtil);
    when(cacheUtil.getConsumerChannelTable()).thenReturn(mockTable)
    when(mockTable.get(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(0)
    val request1 = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"dataset":"druid-dataset","tag":"test-tag","datasetConfig":{"type":"ml-task-detail-exhaust","params":{"programId":"program-1","state_slug":"apekx","solutionId":"solution-1"}},"encryptionKey":"test@123"}}"""
    val response1 = jobApiServiceActorRef.underlyingActor.dataRequest(request1, "in.ekstep", requestHeaderData)
    response1.responseCode should be("FORBIDDEN")
    response1.params.errmsg should be("Given X-Consumer-ID='consumer-1' and X-Channel-ID='in.ekstep' are not authorized")

    reset(cacheUtil);
    when(cacheUtil.getConsumerChannelTable()).thenReturn(mockTable)
    when(mockTable.get(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(1)
    val requestHeaderData1 = RequestHeaderData("", "consumer-1", "test-1")
    val request2 = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"dataset":"druid-dataset","tag":"test-tag","datasetConfig":{"type":"ml-obs-question-detail-exhaust","params":{"programId":"program-1","state_slug":"apekx","solutionId":"solution-2"}},"encryptionKey":"test@123"}}"""
    val response2 = jobApiServiceActorRef.underlyingActor.dataRequest(request2, "", requestHeaderData1)
    response2.responseCode should be("FORBIDDEN")
    response2.params.errmsg should be("X-Channel-ID is missing in request header")

    // check for user-token: success case
    reset(cacheUtil);
    when(cacheUtil.getConsumerChannelTable()).thenReturn(mockTable)
    when(mockTable.get(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(0)
    val userResponse1 = """{"id":"api.user.read","ver":"v2","ts":"2020-09-11 07:52:25:227+0000","params":{"resmsgid":null,"msgid":"43aaf2c2-8ac5-456c-8edf-e17bf2b4f1a4","err":null,"status":"success","errmsg":null},"responseCode":"OK","result":{"response":{"tncLatestVersion":"v1","maskedPhone":"******4105","rootOrgName":null,"roles":["PUBLIC"],"channel":"testChannel","stateValidated":false,"isDeleted":false,"organisations":[{"orgJoinDate":"2020-08-31 10:18:17:833+0000","organisationId":"0126796199493140480","isDeleted":false,"hashTagId":"0126796199493140480","roles":["REPORT_ADMIN"],"id":"01309794241378713625","userId":"4fe7fe33-5e18-4f15-82d2-02255abc1501"}],"countryCode":"+91","flagsValue":3,"tncLatestVersionUrl":"https://dev-sunbird-temp.azureedge.net/portal/terms-and-conditions-v1.html","maskedEmail":"15***********@yopmail.com","id":"4fe7fe33-5e18-4f15-82d2-02255abc1501","email":"15***********@yopmail.com","rootOrg":{"dateTime":null,"preferredLanguage":null,"approvedBy":null,"channel":"custodian","description":"Pre-prod Custodian Organization","updatedDate":null,"addressId":null,"provider":null,"locationId":null,"orgCode":null,"theme":null,"id":"testChannel","communityId":null,"isApproved":null,"email":null,"slug":"testChannel","identifier":"0126796199493140480","thumbnail":null,"orgName":"Pre-prod Custodian Organization","updatedBy":null,"locationIds":[],"externalId":null,"isRootOrg":true,"rootOrgId":"0126796199493140480","approvedDate":null,"imgUrl":null,"homeUrl":null,"orgTypeId":null,"isDefault":true,"contactDetail":null,"createdDate":"2019-01-18 09:48:13:428+0000","createdBy":"system","parentOrgId":null,"hashTagId":"0126796199493140480","noOfMembers":null,"status":1},"identifier":"4fe7fe33-5e18-4f15-82d2-02255abc1501","phoneVerified":true,"userName":"1598868632-71","rootOrgId":"0126796199493140480","promptTnC":true,"firstName":"1598868632-71","emailVerified":true,"createdDate":"2020-08-31 10:18:17:826+0000","phone":"******4105","userType":"OTHER","status":1}}}"""
    when(restUtilMock.get[Response]("https://dev.sunbirded.org/api/user/v2/read/testUser", Option(Map("x-authenticated-user-token" -> "testUserToken")))).thenReturn(JSONUtils.deserialize[Response](userResponse1))
    val requestHeaderData2 = RequestHeaderData("testChannel", "consumer-1", "testUser", Option("testUserToken"))
    val request3 = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"dataset":"druid-dataset","tag":"test-tag","datasetConfig":{"type":"ml-obs-question-detail-exhaust","params":{"programId":"program-1","state_slug":"apekx","solutionId":"solution-2"}},"encryptionKey":"test@123"}}"""
    val response3 = jobApiServiceActorRef.underlyingActor.dataRequest(request3, "testChannel", requestHeaderData2)
    response3.responseCode should be("OK")

    // Failure cases: user without admin access
    val userResponse2 = """{"id":"api.user.read","ver":"v2","ts":"2020-09-11 07:52:25:227+0000","params":{"resmsgid":null,"msgid":"43aaf2c2-8ac5-456c-8edf-e17bf2b4f1a4","err":null,"status":"success","errmsg":null},"responseCode":"OK","result":{"response":{"tncLatestVersion":"v1","maskedPhone":"******4105","rootOrgName":null,"roles":["PUBLIC"],"channel":"testChannel","stateValidated":false,"isDeleted":false,"organisations":[{"orgJoinDate":"2020-08-31 10:18:17:833+0000","organisationId":"0126796199493140480","isDeleted":false,"hashTagId":"0126796199493140480","roles":["PUBLIC"],"id":"01309794241378713625","userId":"4fe7fe33-5e18-4f15-82d2-02255abc1501"}],"countryCode":"+91","flagsValue":3,"tncLatestVersionUrl":"https://dev-sunbird-temp.azureedge.net/portal/terms-and-conditions-v1.html","maskedEmail":"15***********@yopmail.com","id":"4fe7fe33-5e18-4f15-82d2-02255abc1501","email":"15***********@yopmail.com","rootOrg":{"dateTime":null,"preferredLanguage":null,"approvedBy":null,"channel":"custodian","description":"Pre-prod Custodian Organization","updatedDate":null,"addressId":null,"provider":null,"locationId":null,"orgCode":null,"theme":null,"id":"testChannel","communityId":null,"isApproved":null,"email":null,"slug":"testChannel","identifier":"0126796199493140480","thumbnail":null,"orgName":"Pre-prod Custodian Organization","updatedBy":null,"locationIds":[],"externalId":null,"isRootOrg":true,"rootOrgId":"0126796199493140480","approvedDate":null,"imgUrl":null,"homeUrl":null,"orgTypeId":null,"isDefault":true,"contactDetail":null,"createdDate":"2019-01-18 09:48:13:428+0000","createdBy":"system","parentOrgId":null,"hashTagId":"0126796199493140480","noOfMembers":null,"status":1},"identifier":"4fe7fe33-5e18-4f15-82d2-02255abc1501","phoneVerified":true,"userName":"1598868632-71","rootOrgId":"0126796199493140480","promptTnC":true,"firstName":"1598868632-71","emailVerified":true,"createdDate":"2020-08-31 10:18:17:826+0000","phone":"******4105","userType":"OTHER","status":1}}}"""
    when(restUtilMock.get[Response]("https://dev.sunbirded.org/api/user/v2/read/testUser", Option(Map("x-authenticated-user-token" -> "testUserToken")))).thenReturn(JSONUtils.deserialize[Response](userResponse2))
    val requestHeaderData3 = RequestHeaderData("testChannel", "consumer-1", "testUser", Option("testUserToken"))
    val request4 = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"dataset":"druid-dataset","tag":"test-tag","datasetConfig":{"type":"ml-obs-question-detail-exhaust","params":{"programId":"program-1","state_slug":"apekx","solutionId":"solution-2"}},"encryptionKey":"test@123"}}"""
    val response4 = jobApiServiceActorRef.underlyingActor.dataRequest(request4, "testChannel", requestHeaderData3)
    response4.responseCode should be("FORBIDDEN")
    response4.params.errmsg should be("You are not authorized.")

    // Failure cases: user with invalid channel access
    val userResponse3 = """{"id":"api.user.read","ver":"v2","ts":"2020-09-11 07:52:25:227+0000","params":{"resmsgid":null,"msgid":"43aaf2c2-8ac5-456c-8edf-e17bf2b4f1a4","err":null,"status":"success","errmsg":null},"responseCode":"OK","result":{"response":{"tncLatestVersion":"v1","maskedPhone":"******4105","rootOrgName":null,"roles":["PUBLIC"],"channel":"channel-1","stateValidated":false,"isDeleted":false,"organisations":[{"orgJoinDate":"2020-08-31 10:18:17:833+0000","organisationId":"0126796199493140480","isDeleted":false,"hashTagId":"0126796199493140480","roles":["REPORT_ADMIN"],"id":"01309794241378713625","userId":"4fe7fe33-5e18-4f15-82d2-02255abc1501"}],"countryCode":"+91","flagsValue":3,"tncLatestVersionUrl":"https://dev-sunbird-temp.azureedge.net/portal/terms-and-conditions-v1.html","maskedEmail":"15***********@yopmail.com","id":"4fe7fe33-5e18-4f15-82d2-02255abc1501","email":"15***********@yopmail.com","rootOrg":{"dateTime":null,"preferredLanguage":null,"approvedBy":null,"channel":"custodian","description":"Pre-prod Custodian Organization","updatedDate":null,"addressId":null,"provider":null,"locationId":null,"orgCode":null,"theme":null,"id":"channel-1","communityId":null,"isApproved":null,"email":null,"slug":"testChannel","identifier":"0126796199493140480","thumbnail":null,"orgName":"Pre-prod Custodian Organization","updatedBy":null,"locationIds":[],"externalId":null,"isRootOrg":true,"rootOrgId":"0126796199493140480","approvedDate":null,"imgUrl":null,"homeUrl":null,"orgTypeId":null,"isDefault":true,"contactDetail":null,"createdDate":"2019-01-18 09:48:13:428+0000","createdBy":"system","parentOrgId":null,"hashTagId":"0126796199493140480","noOfMembers":null,"status":1},"identifier":"4fe7fe33-5e18-4f15-82d2-02255abc1501","phoneVerified":true,"userName":"1598868632-71","rootOrgId":"0126796199493140480","promptTnC":true,"firstName":"1598868632-71","emailVerified":true,"createdDate":"2020-08-31 10:18:17:826+0000","phone":"******4105","userType":"OTHER","status":1}}}"""
    when(restUtilMock.get[Response]("https://dev.sunbirded.org/api/user/v2/read/testUser", Option(Map("x-authenticated-user-token" -> "testUserToken")))).thenReturn(JSONUtils.deserialize[Response](userResponse3))
    val requestHeaderData4 = RequestHeaderData("testChannel", "consumer-1", "testUser", Option("testUserToken"))
    val request5 = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"dataset":"druid-dataset","tag":"test-tag","datasetConfig":{"type":"ml-obs-question-detail-exhaust","params":{"programId":"program-1","state_slug":"apekx","solutionId":"solution-2"}},"encryptionKey":"test@123"}}"""
    val response5 = jobApiServiceActorRef.underlyingActor.dataRequest(request5, "testChannel", requestHeaderData4)
    response5.responseCode should be("FORBIDDEN")
    response5.params.errmsg should be("You are not authorized.")

    EmbeddedPostgresql.execute(
      s"""insert into job_request ("tag", "request_id", "job_id", "status", "request_data", "requested_by",
        "requested_channel", "dt_job_submitted", "dt_job_completed", "download_urls", "dt_file_created", "execution_time", "iteration") values ('consumer-1:testChannel', '562CDD1241226D5CA2E777DA522691EF-1', 'assessment-score-report',
        'SUCCESS',  '{"batchFilter":["TPD","NCFCOPY"],"contentFilters":{"request":{"filters":{"identifier":["do_11305960936384921612216","do_1130934466492252161819"],"prevState":"Draft"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel","prevState"]}},"reportPath":"course-progress-v2/"}',
        'test-2', 'in.ekstep' , '2020-09-07T13:54:39.019+05:30', '2020-09-08T13:54:39.019+05:30', '{"https://sunbird.org/test/signed/file1.csv", "https://sunbird.org/test/signed/file2.csv"}', '2020-09-08T13:50:39.019+05:30', '10', '0');""")

    val readResponse5 = jobApiServiceActorRef.underlyingActor.getDataRequest("consumer-1:testChannel", "562CDD1241226D5CA2E777DA522691EF-1", requestHeaderData4)
    readResponse5.responseCode should be("FORBIDDEN")
    readResponse5.params.errmsg should be("You are not authorized.")

    val listResponse5 = jobApiServiceActorRef.underlyingActor.getDataRequestList("consumer-1", 10, requestHeaderData4)
    listResponse5.responseCode should be("FORBIDDEN")
    listResponse5.params.errmsg should be("You are not authorized.")

    // Failure cases: user read API failure
    val userResponse5 = """{"id":"api.user.read","ver":"v2","ts":"2020-09-17 13:39:41:496+0000","params":{"resmsgid":null,"msgid":"08db1cfd-68a9-42e9-87ce-2e53e33f8b6d","err":"USER_NOT_FOUND","status":"USER_NOT_FOUND","errmsg":"user not found."},"responseCode":"RESOURCE_NOT_FOUND","result":{}}"""
    when(restUtilMock.get[Response]("https://dev.sunbirded.org/api/user/v2/read/testUser", Option(Map("x-authenticated-user-token" -> "testUserToken")))).thenReturn(JSONUtils.deserialize[Response](userResponse5))
    val requestHeaderData5 = RequestHeaderData("testChannel", "consumer-1", "testUser", Option("testUserToken"))
    val request6 = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"dataset":"druid-dataset","tag":"test-tag","datasetConfig":{"type":"ml-obs-question-detail-exhaust","params":{"programId":"program-1","state_slug":"apekx","solutionId":"solution-2"}},"encryptionKey":"test@123"}}"""
    val response6 = jobApiServiceActorRef.underlyingActor.dataRequest(request6, "testChannel", requestHeaderData5)
    response6.responseCode should be("FORBIDDEN")
    response6.params.errmsg should be("user not found.")

    // check for validation schema & roles from dataset metadata: success case
    val submissionDate = DateTime.now().toString("yyyy-MM-dd")
    EmbeddedPostgresql.execute(
      s"""truncate table dataset_metadata;""")
    EmbeddedPostgresql.execute(
      s"""insert into dataset_metadata ("dataset_id", "dataset_sub_id", "dataset_config", "visibility", "dataset_type", "version",
          "authorized_roles", "available_from", "sample_request", "sample_response", "validation_json")
          values ('druid-dataset', 'ml-obs-question-detail-exhaust', '{"batchFilter":[],"contentFilters":{"request":{"filters":{"identifier":"","prevState":""},"sort_by":{"created_on":"desc"},"limit":100,"fields":[]}},"reportPath":"/test","output_format":"csv"}',
           'private', 'On-Demand', '1.0', '{"PROGRAM_MANAGER"}', '$submissionDate', '', '', '{"type":"object","properties":{"tag":{"id":"http://api.ekstep.org/dataexhaust/request/tag","type":"string"},"dataset":{"id":"http://api.ekstep.org/dataexhaust/request/dataset","type":"string"},"requestedBy":{"id":"http://api.ekstep.org/dataexhaust/request/requestedBy","type":"string"},"encryptionKey":{"id":"http://api.ekstep.org/dataexhaust/request/encryptionKey","type":"string"},"datasetConfig":{"id":"http://api.ekstep.org/dataexhaust/request/datasetConfig","type":"object"}},"required":["tag","dataset","datasetConfig"]}');""")

    reset(cacheUtil);
    when(cacheUtil.getConsumerChannelTable()).thenReturn(mockTable)
    when(mockTable.get(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(0)
    val userResponse6 = """{"id":"api.user.read","ver":"v2","ts":"2020-09-11 07:52:25:227+0000","params":{"resmsgid":null,"msgid":"43aaf2c2-8ac5-456c-8edf-e17bf2b4f1a4","err":null,"status":"success","errmsg":null},"responseCode":"OK","result":{"response":{"tncLatestVersion":"v1","maskedPhone":"******4105","rootOrgName":null,"roles":["PUBLIC"],"channel":"testChannel","stateValidated":false,"isDeleted":false,"organisations":[{"orgJoinDate":"2020-08-31 10:18:17:833+0000","organisationId":"0126796199493140480","isDeleted":false,"hashTagId":"0126796199493140480","roles":["PROGRAM_MANAGER"],"id":"01309794241378713625","userId":"4fe7fe33-5e18-4f15-82d2-02255abc1501"}],"countryCode":"+91","flagsValue":3,"tncLatestVersionUrl":"https://dev-sunbird-temp.azureedge.net/portal/terms-and-conditions-v1.html","maskedEmail":"15***********@yopmail.com","id":"4fe7fe33-5e18-4f15-82d2-02255abc1501","email":"15***********@yopmail.com","rootOrg":{"dateTime":null,"preferredLanguage":null,"approvedBy":null,"channel":"custodian","description":"Pre-prod Custodian Organization","updatedDate":null,"addressId":null,"provider":null,"locationId":null,"orgCode":null,"theme":null,"id":"testChannel","communityId":null,"isApproved":null,"email":null,"slug":"testChannel","identifier":"0126796199493140480","thumbnail":null,"orgName":"Pre-prod Custodian Organization","updatedBy":null,"locationIds":[],"externalId":null,"isRootOrg":true,"rootOrgId":"0126796199493140480","approvedDate":null,"imgUrl":null,"homeUrl":null,"orgTypeId":null,"isDefault":true,"contactDetail":null,"createdDate":"2019-01-18 09:48:13:428+0000","createdBy":"system","parentOrgId":null,"hashTagId":"0126796199493140480","noOfMembers":null,"status":1},"identifier":"4fe7fe33-5e18-4f15-82d2-02255abc1501","phoneVerified":true,"userName":"1598868632-71","rootOrgId":"0126796199493140480","promptTnC":true,"firstName":"1598868632-71","emailVerified":true,"createdDate":"2020-08-31 10:18:17:826+0000","phone":"******4105","userType":"OTHER","status":1}}}"""
    when(restUtilMock.get[Response]("https://dev.sunbirded.org/api/user/v2/read/testUser", Option(Map("x-authenticated-user-token" -> "testUserToken")))).thenReturn(JSONUtils.deserialize[Response](userResponse6))
    val requestHeaderData7 = RequestHeaderData("testChannel", "consumer-1", "testUser", Option("testUserToken"))
    val request7 = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"dataset":"druid-dataset","datasetSubId":"ml-obs-question-detail-exhaust","tag":"test-tag","datasetConfig":{"type":"ml-obs-question-detail-exhaust","params":{"programId":"program-1","state_slug":"apekx","solutionId":"solution-2"}},"encryptionKey":"test@123"}}"""
    val response7 = jobApiServiceActorRef.underlyingActor.dataRequest(request7, "testChannel", requestHeaderData7)
    response7.responseCode should be("OK")

    // check for validation schema from dataset metadata: failure case
    val requestHeaderData8 = RequestHeaderData("testChannel", "consumer-1", "testUser", Option("testUserToken"))
    val request8 = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"datasetSubId":"ml-obs-question-detail-exhaust","tag":"test-tag","requestedBy":"testUser","datasetConfig":{"type":"ml-obs-question-detail-exhaust","params":{"programId":"program-1","state_slug":"apekx","solutionId":"solution-2"}},"encryptionKey":"test@123"}}"""
    val response8 = jobApiServiceActorRef.underlyingActor.dataRequest(request8, "testChannel", requestHeaderData8)
    response8.responseCode should be("CLIENT_ERROR")
    response8.params.errmsg should be("""Request object has missing required properties (["dataset"])""")

  }

  it should "return signed URL for csv file" in {

    EmbeddedPostgresql.execute(
      s"""insert into job_request ("tag", "request_id", "job_id", "status", "request_data", "requested_by",
        "requested_channel", "dt_job_submitted", "dt_job_completed", "download_urls", "dt_file_created", "execution_time") values ('client-2', '462CDD1241226D5CA2E777DA522691EF', 'assessment-score-report',
        'SUCCESS',  '{"batchFilter":["TPD","NCFCOPY"],"contentFilters":{"request":{"filters":{"identifier":["do_11305960936384921612216","do_1130934466492252161819"],"prevState":"Draft"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel","prevState"]}},"reportPath":"course-progress-v2/"}',
        'test-1', 'in.ekstep' , '2020-09-07T13:54:39.019+05:30', '2020-09-08T13:54:39.019+05:30', '{"wasb://reports@testaccount.blob.core.windows.net/uci-response-exhaust/427C56316649909179E69188C5CDB091/d655cf03-1f6f-4510-acf6-d3f51b488a5e_response_20220209.csv"}', '2020-09-08T13:50:39.019+05:30', '10');""")

    when(cacheUtil.getConsumerChannelTable()).thenReturn(mockTable)
    when(mockTable.get(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(1)
    reset(mockStorageService)
    when(mockFc.getStorageService(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn(mockStorageService);
    when(mockStorageService.getSignedURL(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn("https://sunbird.org/test/signed/file1.csv");
    doNothing().when(mockStorageService).closeContext()

    val res = jobApiServiceActorRef.underlyingActor.getDataRequestList("client-2", 10, requestHeaderData)
    val resultMap = res.result.get
    val jobRes = JSONUtils.deserialize[List[JobResponse]](JSONUtils.serialize(resultMap.get("jobs").get))
    jobRes.length should be(1)

    // check for extracting object key logic
    val testStr = "wasb://reports@testaccount.blob.core.windows.net/uci-response-exhaust/427C56316649909179E69188C5CDB091/d655cf03-1f6f-4510-acf6-d3f51b488a5e_response_20220209.csv"
    val values = testStr.split("/").toList.drop(3)
    values.mkString("/") should be("uci-response-exhaust/427C56316649909179E69188C5CDB091/d655cf03-1f6f-4510-acf6-d3f51b488a5e_response_20220209.csv")

  }
}
