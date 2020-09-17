
import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import akka.util.Timeout
import com.typesafe.config.Config
import controllers.JobController
import org.ekstep.analytics.api.{APIIds, Response}
import org.ekstep.analytics.api.service.{ChannelData, DataRequest, DataRequestList, GetDataRequest}
import org.ekstep.analytics.api.service._
import org.ekstep.analytics.api.util._
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import play.api.Configuration
import play.api.libs.json.Json
import play.api.test.{FakeRequest, Helpers}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import com.google.common.collect.Table
import org.ekstep.analytics.api.util.auth_verifier.AccessTokenValidator

@RunWith(classOf[JUnitRunner])
class JobControllerSpec extends FlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {

  implicit val system = ActorSystem()
  implicit val timeout: Timeout = 20.seconds
  implicit val mockConfig = mock[Config];
  private val configurationMock = mock[Configuration]
  private val cacheUtil = mock[CacheUtil]
  private val mockTable = mock[Table[String, String, Integer]];
  private val postgresUtilMock = mock[PostgresDBUtil]
  private val restUtilMock = mock[APIRestUtil]
  private val accessTokenValidator = mock[AccessTokenValidator]
  when(configurationMock.underlying).thenReturn(mockConfig)


  val jobAPIActor = TestActorRef(new JobAPIService(postgresUtilMock) {
    override def receive: Receive = {
      case DataRequest(request: String, channelId: String, config: Config) => {
        sender() ! CommonUtil.OK(APIIds.DATA_REQUEST, Map())
      }
      case GetDataRequest(clientKey: String, requestId: String, config: Config) => {
        sender() ! CommonUtil.OK(APIIds.GET_DATA_REQUEST, Map())
      }
      case DataRequestList(clientKey: String, limit: Int, config: Config) => {
        sender() ! CommonUtil.OK(APIIds.GET_DATA_REQUEST_LIST, Map())
      }
      case ChannelData(channel: String, eventType: String, from: String, to: String, since: String, config: Config) => {
        sender() ! CommonUtil.OK(APIIds.CHANNEL_TELEMETRY_EXHAUST, Map())
      }
    }
  })

  val controller = new JobController(jobAPIActor, system, configurationMock, Helpers.stubControllerComponents(), cacheUtil, restUtilMock, accessTokenValidator)

  "JobController" should "test get job API " in {

    when(mockConfig.getBoolean("dataexhaust.authorization_check")).thenReturn(true);
    when(cacheUtil.getConsumerChannelTable()).thenReturn(mockTable)
    when(mockTable.get(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(1)
    var result = controller.getJob("client1", "request1").apply(FakeRequest().withHeaders(("X-Channel-ID", "testChannel")))
    Helpers.status(result) should be (Helpers.OK)

    reset(cacheUtil);
    when(cacheUtil.getConsumerChannelTable()).thenReturn(mockTable)
    when(mockTable.get(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(0)
    result = controller.getJob("client1", "request1").apply(FakeRequest().withHeaders(("X-Channel-ID", "testChannel")))
    Helpers.status(result) should be (Helpers.FORBIDDEN)
    Helpers.contentAsString(result).indexOf(""""errmsg":"Given X-Consumer-ID='' and X-Channel-ID='testChannel' are not authorized"""") should not be (-1)

    result = controller.getJob("client1", "request1").apply(FakeRequest())
    Helpers.status(result) should be (Helpers.FORBIDDEN)
    Helpers.contentAsString(result).indexOf(""""errmsg":"X-Channel-ID is missing in request header"""") should not be (-1)

    reset(cacheUtil);
    reset(mockConfig);
    when(mockConfig.getBoolean("dataexhaust.authorization_check")).thenReturn(false);
    when(cacheUtil.getConsumerChannelTable()).thenReturn(mockTable)
    when(mockTable.get(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(0)
    result = controller.getJob("client1", "request1").apply(FakeRequest().withHeaders(("X-Channel-ID", "testChannel")))
    Helpers.status(result) should be (Helpers.OK)

    // check for user-token: success case
    reset(cacheUtil);
    reset(mockConfig);
    when(mockConfig.getBoolean("dataexhaust.authorization_check")).thenReturn(true);
    when(mockConfig.getString("user.profile.url")).thenReturn("https://dev.sunbirded.org/api/user/v2/read/");
    when(mockConfig.getStringList("standard.dataexhaust.roles")).thenReturn(List("ORG_ADMIN","REPORT_ADMIN"));
    when(mockConfig.getStringList("ondemand.dataexhaust.roles")).thenReturn(List("ORG_ADMIN","REPORT_ADMIN","COURSE_ADMIN"));
    when(cacheUtil.getConsumerChannelTable()).thenReturn(mockTable)
    when(mockTable.get(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(0)
    val response1 = """{"id":"api.user.read","ver":"v2","ts":"2020-09-11 07:52:25:227+0000","params":{"resmsgid":null,"msgid":"43aaf2c2-8ac5-456c-8edf-e17bf2b4f1a4","err":null,"status":"success","errmsg":null},"responseCode":"OK","result":{"response":{"tncLatestVersion":"v1","maskedPhone":"******4105","rootOrgName":null,"roles":["PUBLIC"],"channel":"testChannel","stateValidated":false,"isDeleted":false,"organisations":[{"orgJoinDate":"2020-08-31 10:18:17:833+0000","organisationId":"0126796199493140480","isDeleted":false,"hashTagId":"0126796199493140480","roles":["REPORT_ADMIN"],"id":"01309794241378713625","userId":"4fe7fe33-5e18-4f15-82d2-02255abc1501"}],"countryCode":"+91","flagsValue":3,"tncLatestVersionUrl":"https://dev-sunbird-temp.azureedge.net/portal/terms-and-conditions-v1.html","maskedEmail":"15***********@yopmail.com","id":"4fe7fe33-5e18-4f15-82d2-02255abc1501","email":"15***********@yopmail.com","rootOrg":{"dateTime":null,"preferredLanguage":null,"approvedBy":null,"channel":"custodian","description":"Pre-prod Custodian Organization","updatedDate":null,"addressId":null,"provider":null,"locationId":null,"orgCode":null,"theme":null,"id":"testChannel","communityId":null,"isApproved":null,"email":null,"slug":"testChannel","identifier":"0126796199493140480","thumbnail":null,"orgName":"Pre-prod Custodian Organization","updatedBy":null,"locationIds":[],"externalId":null,"isRootOrg":true,"rootOrgId":"0126796199493140480","approvedDate":null,"imgUrl":null,"homeUrl":null,"orgTypeId":null,"isDefault":true,"contactDetail":null,"createdDate":"2019-01-18 09:48:13:428+0000","createdBy":"system","parentOrgId":null,"hashTagId":"0126796199493140480","noOfMembers":null,"status":1},"identifier":"4fe7fe33-5e18-4f15-82d2-02255abc1501","phoneVerified":true,"userName":"1598868632-71","rootOrgId":"0126796199493140480","promptTnC":true,"firstName":"1598868632-71","emailVerified":true,"createdDate":"2020-08-31 10:18:17:826+0000","phone":"******4105","userType":"OTHER","status":1}}}"""
    when(restUtilMock.get[Response]("https://dev.sunbirded.org/api/user/v2/read/testUser", Option(Map("x-authenticated-user-token" -> "testUserToken", "Authorization" -> "testBearerToken")))).thenReturn(JSONUtils.deserialize[Response](response1))
    when(accessTokenValidator.getUserId("testUserToken")).thenReturn("testUser")
    result = controller.getJob("client1", "request1").apply(FakeRequest().withHeaders(("X-Channel-ID", "testChannel")).withHeaders(("x-authenticated-user-token", "testUserToken")).withHeaders(("Authorization", "testBearerToken")))
    println(Helpers.contentAsString(result))
    Helpers.status(result) should be (Helpers.OK)

    // Failure cases: user without admin access
    val response2 = """{"id":"api.user.read","ver":"v2","ts":"2020-09-11 07:52:25:227+0000","params":{"resmsgid":null,"msgid":"43aaf2c2-8ac5-456c-8edf-e17bf2b4f1a4","err":null,"status":"success","errmsg":null},"responseCode":"OK","result":{"response":{"tncLatestVersion":"v1","maskedPhone":"******4105","rootOrgName":null,"roles":["PUBLIC"],"channel":"testChannel","stateValidated":false,"isDeleted":false,"organisations":[{"orgJoinDate":"2020-08-31 10:18:17:833+0000","organisationId":"0126796199493140480","isDeleted":false,"hashTagId":"0126796199493140480","roles":["PUBLIC"],"id":"01309794241378713625","userId":"4fe7fe33-5e18-4f15-82d2-02255abc1501"}],"countryCode":"+91","flagsValue":3,"tncLatestVersionUrl":"https://dev-sunbird-temp.azureedge.net/portal/terms-and-conditions-v1.html","maskedEmail":"15***********@yopmail.com","id":"4fe7fe33-5e18-4f15-82d2-02255abc1501","email":"15***********@yopmail.com","rootOrg":{"dateTime":null,"preferredLanguage":null,"approvedBy":null,"channel":"custodian","description":"Pre-prod Custodian Organization","updatedDate":null,"addressId":null,"provider":null,"locationId":null,"orgCode":null,"theme":null,"id":"testChannel","communityId":null,"isApproved":null,"email":null,"slug":"testChannel","identifier":"0126796199493140480","thumbnail":null,"orgName":"Pre-prod Custodian Organization","updatedBy":null,"locationIds":[],"externalId":null,"isRootOrg":true,"rootOrgId":"0126796199493140480","approvedDate":null,"imgUrl":null,"homeUrl":null,"orgTypeId":null,"isDefault":true,"contactDetail":null,"createdDate":"2019-01-18 09:48:13:428+0000","createdBy":"system","parentOrgId":null,"hashTagId":"0126796199493140480","noOfMembers":null,"status":1},"identifier":"4fe7fe33-5e18-4f15-82d2-02255abc1501","phoneVerified":true,"userName":"1598868632-71","rootOrgId":"0126796199493140480","promptTnC":true,"firstName":"1598868632-71","emailVerified":true,"createdDate":"2020-08-31 10:18:17:826+0000","phone":"******4105","userType":"OTHER","status":1}}}"""
    when(restUtilMock.get[Response]("https://dev.sunbirded.org/api/user/v2/read/testUser", Option(Map("x-authenticated-user-token" -> "testUserToken", "Authorization" -> "testBearerToken")))).thenReturn(JSONUtils.deserialize[Response](response2))
    when(accessTokenValidator.getUserId("testUserToken")).thenReturn("testUser")
    result = controller.getJob("client1", "request1").apply(FakeRequest().withHeaders(("X-Channel-ID", "testChannel")).withHeaders(("x-authenticated-user-token", "testUserToken")).withHeaders(("Authorization", "testBearerToken")))
    Helpers.status(result) should be (Helpers.FORBIDDEN)
    Helpers.contentAsString(result).indexOf(""""errmsg":"You are not authorized."""") should not be (-1)

    // Failure cases: user with invalid channel access
    val response3 = """{"id":"api.user.read","ver":"v2","ts":"2020-09-11 07:52:25:227+0000","params":{"resmsgid":null,"msgid":"43aaf2c2-8ac5-456c-8edf-e17bf2b4f1a4","err":null,"status":"success","errmsg":null},"responseCode":"OK","result":{"response":{"tncLatestVersion":"v1","maskedPhone":"******4105","rootOrgName":null,"roles":["PUBLIC"],"channel":"channel-1","stateValidated":false,"isDeleted":false,"organisations":[{"orgJoinDate":"2020-08-31 10:18:17:833+0000","organisationId":"0126796199493140480","isDeleted":false,"hashTagId":"0126796199493140480","roles":["REPORT_ADMIN"],"id":"01309794241378713625","userId":"4fe7fe33-5e18-4f15-82d2-02255abc1501"}],"countryCode":"+91","flagsValue":3,"tncLatestVersionUrl":"https://dev-sunbird-temp.azureedge.net/portal/terms-and-conditions-v1.html","maskedEmail":"15***********@yopmail.com","id":"4fe7fe33-5e18-4f15-82d2-02255abc1501","email":"15***********@yopmail.com","rootOrg":{"dateTime":null,"preferredLanguage":null,"approvedBy":null,"channel":"custodian","description":"Pre-prod Custodian Organization","updatedDate":null,"addressId":null,"provider":null,"locationId":null,"orgCode":null,"theme":null,"id":"channel-1","communityId":null,"isApproved":null,"email":null,"slug":"testChannel","identifier":"0126796199493140480","thumbnail":null,"orgName":"Pre-prod Custodian Organization","updatedBy":null,"locationIds":[],"externalId":null,"isRootOrg":true,"rootOrgId":"0126796199493140480","approvedDate":null,"imgUrl":null,"homeUrl":null,"orgTypeId":null,"isDefault":true,"contactDetail":null,"createdDate":"2019-01-18 09:48:13:428+0000","createdBy":"system","parentOrgId":null,"hashTagId":"0126796199493140480","noOfMembers":null,"status":1},"identifier":"4fe7fe33-5e18-4f15-82d2-02255abc1501","phoneVerified":true,"userName":"1598868632-71","rootOrgId":"0126796199493140480","promptTnC":true,"firstName":"1598868632-71","emailVerified":true,"createdDate":"2020-08-31 10:18:17:826+0000","phone":"******4105","userType":"OTHER","status":1}}}"""
    when(restUtilMock.get[Response]("https://dev.sunbirded.org/api/user/v2/read/testUser", Option(Map("x-authenticated-user-token" -> "testUserToken", "Authorization" -> "testBearerToken")))).thenReturn(JSONUtils.deserialize[Response](response3))
    when(accessTokenValidator.getUserId("testUserToken")).thenReturn("testUser")
    result = controller.getJob("client1", "request1").apply(FakeRequest().withHeaders(("X-Channel-ID", "testChannel")).withHeaders(("x-authenticated-user-token", "testUserToken")).withHeaders(("Authorization", "testBearerToken")))
    Helpers.status(result) should be (Helpers.FORBIDDEN)
    Helpers.contentAsString(result).indexOf(""""errmsg":"You are not authorized."""") should not be (-1)

    // Failure cases: unauthorized user
    val response4 = """{"id":"api.user.read","ver":"v2","ts":"2020-09-11 07:52:25:227+0000","params":{"resmsgid":null,"msgid":"43aaf2c2-8ac5-456c-8edf-e17bf2b4f1a4","err":null,"status":"success","errmsg":null},"responseCode":"OK","result":{"response":{"tncLatestVersion":"v1","maskedPhone":"******4105","rootOrgName":null,"roles":["PUBLIC"],"channel":"testChannel","stateValidated":false,"isDeleted":false,"organisations":[{"orgJoinDate":"2020-08-31 10:18:17:833+0000","organisationId":"0126796199493140480","isDeleted":false,"hashTagId":"0126796199493140480","roles":["REPORT_ADMIN"],"id":"01309794241378713625","userId":"4fe7fe33-5e18-4f15-82d2-02255abc1501"}],"countryCode":"+91","flagsValue":3,"tncLatestVersionUrl":"https://dev-sunbird-temp.azureedge.net/portal/terms-and-conditions-v1.html","maskedEmail":"15***********@yopmail.com","id":"4fe7fe33-5e18-4f15-82d2-02255abc1501","email":"15***********@yopmail.com","rootOrg":{"dateTime":null,"preferredLanguage":null,"approvedBy":null,"channel":"custodian","description":"Pre-prod Custodian Organization","updatedDate":null,"addressId":null,"provider":null,"locationId":null,"orgCode":null,"theme":null,"id":"testChannel","communityId":null,"isApproved":null,"email":null,"slug":"testChannel","identifier":"0126796199493140480","thumbnail":null,"orgName":"Pre-prod Custodian Organization","updatedBy":null,"locationIds":[],"externalId":null,"isRootOrg":true,"rootOrgId":"0126796199493140480","approvedDate":null,"imgUrl":null,"homeUrl":null,"orgTypeId":null,"isDefault":true,"contactDetail":null,"createdDate":"2019-01-18 09:48:13:428+0000","createdBy":"system","parentOrgId":null,"hashTagId":"0126796199493140480","noOfMembers":null,"status":1},"identifier":"4fe7fe33-5e18-4f15-82d2-02255abc1501","phoneVerified":true,"userName":"1598868632-71","rootOrgId":"0126796199493140480","promptTnC":true,"firstName":"1598868632-71","emailVerified":true,"createdDate":"2020-08-31 10:18:17:826+0000","phone":"******4105","userType":"OTHER","status":1}}}"""
    when(restUtilMock.get[Response]("https://dev.sunbirded.org/api/user/v2/read/testUser", Option(Map("x-authenticated-user-token" -> "testUserToken", "Authorization" -> "testBearerToken")))).thenReturn(JSONUtils.deserialize[Response](response4))
    when(accessTokenValidator.getUserId("testUserToken")).thenReturn("Unauthorized")
    result = controller.getJob("client1", "request1").apply(FakeRequest().withHeaders(("X-Channel-ID", "testChannel")).withHeaders(("x-authenticated-user-token", "testUserToken")).withHeaders(("Authorization", "testBearerToken")))
    Helpers.status(result) should be (Helpers.FORBIDDEN)
    Helpers.contentAsString(result).indexOf(""""errmsg":"You are not authorized."""") should not be (-1)

    // Failure cases: user read API failure
    val response5 = """{"id":"api.user.read","ver":"v2","ts":"2020-09-17 13:39:41:496+0000","params":{"resmsgid":null,"msgid":"08db1cfd-68a9-42e9-87ce-2e53e33f8b6d","err":"USER_NOT_FOUND","status":"USER_NOT_FOUND","errmsg":"user not found."},"responseCode":"RESOURCE_NOT_FOUND","result":{}}"""
    when(restUtilMock.get[Response]("https://dev.sunbirded.org/api/user/v2/read/testUser", Option(Map("x-authenticated-user-token" -> "testUserToken", "Authorization" -> "testBearerToken")))).thenReturn(JSONUtils.deserialize[Response](response5))
    when(accessTokenValidator.getUserId("testUserToken")).thenReturn("testUser")
    result = controller.getJob("client1", "request1").apply(FakeRequest().withHeaders(("X-Channel-ID", "testChannel")).withHeaders(("x-authenticated-user-token", "testUserToken")).withHeaders(("Authorization", "testBearerToken")))
    Helpers.status(result) should be (Helpers.FORBIDDEN)
    Helpers.contentAsString(result).indexOf(""""errmsg":"user not found."""") should not be (-1)

  }

  it should "test data request API" in {

    reset(cacheUtil);
    reset(mockConfig);
    reset(mockTable);
    when(mockConfig.getBoolean("dataexhaust.authorization_check")).thenReturn(true);
    when(cacheUtil.getConsumerChannelTable()).thenReturn(mockTable)
    when(mockTable.get(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(0)
    var result = controller.dataRequest().apply(FakeRequest().withHeaders(("X-Channel-ID", "testChannel")).withJsonBody(Json.parse("""{}""")))
    Helpers.status(result) should be (Helpers.FORBIDDEN)
    Helpers.contentAsString(result).indexOf(""""errmsg":"Given X-Consumer-ID='' and X-Channel-ID='testChannel' are not authorized"""") should not be (-1)

    result = controller.dataRequest().apply(FakeRequest().withJsonBody(Json.parse("""{}""")))
    Helpers.status(result) should be (Helpers.FORBIDDEN)
    Helpers.contentAsString(result).indexOf(""""errmsg":"X-Channel-ID is missing in request header"""") should not be (-1)

    reset(mockConfig);
    when(mockConfig.getBoolean("dataexhaust.authorization_check")).thenReturn(false);
    result = controller.dataRequest().apply(FakeRequest().withHeaders(("X-Channel-ID", "testChannel")).withJsonBody(Json.parse("""{}""")))
    Helpers.status(result) should be (Helpers.OK)
  }

  it should "test get job list API" in {

    reset(cacheUtil);
    reset(mockConfig);
    reset(mockTable);
    when(mockConfig.getBoolean("dataexhaust.authorization_check")).thenReturn(true);
    when(cacheUtil.getConsumerChannelTable()).thenReturn(mockTable)
    when(mockTable.get(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(0)
    var result = controller.getJobList("testClientKey").apply(FakeRequest().withHeaders(("X-Channel-ID", "testChannel")));
    Helpers.status(result) should be (Helpers.FORBIDDEN)
    Helpers.contentAsString(result).indexOf(""""errmsg":"Given X-Consumer-ID='' and X-Channel-ID='testChannel' are not authorized"""") should not be (-1)

    result = controller.getJobList("testClientKey").apply(FakeRequest());
    Helpers.status(result) should be (Helpers.FORBIDDEN)
    Helpers.contentAsString(result).indexOf(""""errmsg":"X-Channel-ID is missing in request header"""") should not be (-1)

    reset(mockConfig);
    when(mockConfig.getBoolean("dataexhaust.authorization_check")).thenReturn(false);
    when(mockConfig.getString("data_exhaust.list.limit")).thenReturn("10");
    result = controller.getJobList("testClientKey").apply(FakeRequest().withHeaders(("X-Channel-ID", "testChannel")));
    Helpers.status(result) should be (Helpers.OK)
  }

  it should "test get telemetry API" in {

    reset(cacheUtil);
    reset(mockConfig);
    when(mockConfig.getBoolean("dataexhaust.authorization_check")).thenReturn(true);
    when(cacheUtil.getConsumerChannelTable()).thenReturn(mockTable)
    when(mockTable.get(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(0)

    var result = controller.getTelemetry("testDataSet").apply(FakeRequest().withHeaders(("X-Channel-ID", "testChannel")));
    Helpers.status(result) should be (Helpers.FORBIDDEN)
    Helpers.contentAsString(result).indexOf(""""errmsg":"Given X-Consumer-ID='' and X-Channel-ID='testChannel' are not authorized"""") should not be (-1)

    reset(mockConfig);
    when(mockConfig.getBoolean("dataexhaust.authorization_check")).thenReturn(false);
    result = controller.getTelemetry("raw").apply(FakeRequest().withHeaders(("X-Channel-ID", "testChannel")));
    Helpers.status(result) should be (Helpers.OK)

    // check for user-token: success case
    reset(cacheUtil);
    reset(mockConfig);
    when(mockConfig.getBoolean("dataexhaust.authorization_check")).thenReturn(true);
    when(mockConfig.getString("user.profile.url")).thenReturn("https://dev.sunbirded.org/api/user/v2/read/");
    when(mockConfig.getString("org.search.url")).thenReturn("https://dev.sunbirded.org/api/org/v1/search");
    when(mockConfig.getStringList("standard.dataexhaust.roles")).thenReturn(List("ORG_ADMIN","REPORT_ADMIN"));
    when(mockConfig.getStringList("ondemand.dataexhaust.roles")).thenReturn(List("ORG_ADMIN","REPORT_ADMIN","COURSE_ADMIN"));
    when(cacheUtil.getConsumerChannelTable()).thenReturn(mockTable)
    when(mockTable.get(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(0)
    val response1 = """{"id":"api.user.read","ver":"v2","ts":"2020-09-11 07:52:25:227+0000","params":{"resmsgid":null,"msgid":"43aaf2c2-8ac5-456c-8edf-e17bf2b4f1a4","err":null,"status":"success","errmsg":null},"responseCode":"OK","result":{"response":{"tncLatestVersion":"v1","maskedPhone":"******4105","rootOrgName":null,"roles":["PUBLIC"],"channel":"channel-mhrd","stateValidated":false,"isDeleted":false,"organisations":[{"orgJoinDate":"2020-08-31 10:18:17:833+0000","organisationId":"0126796199493140480","isDeleted":false,"hashTagId":"0126796199493140480","roles":["REPORT_ADMIN"],"id":"01309794241378713625","userId":"4fe7fe33-5e18-4f15-82d2-02255abc1501"}],"countryCode":"+91","flagsValue":3,"tncLatestVersionUrl":"https://dev-sunbird-temp.azureedge.net/portal/terms-and-conditions-v1.html","maskedEmail":"15***********@yopmail.com","id":"4fe7fe33-5e18-4f15-82d2-02255abc1501","email":"15***********@yopmail.com","rootOrg":{"dateTime":null,"preferredLanguage":null,"approvedBy":null,"channel":"custodian","description":"Pre-prod Custodian Organization","updatedDate":null,"addressId":null,"provider":null,"locationId":null,"orgCode":null,"theme":null,"id":"testChannel","communityId":null,"isApproved":null,"email":null,"slug":"testChannel","identifier":"0126796199493140480","thumbnail":null,"orgName":"Pre-prod Custodian Organization","updatedBy":null,"locationIds":[],"externalId":null,"isRootOrg":true,"rootOrgId":"0126796199493140480","approvedDate":null,"imgUrl":null,"homeUrl":null,"orgTypeId":null,"isDefault":true,"contactDetail":null,"createdDate":"2019-01-18 09:48:13:428+0000","createdBy":"system","parentOrgId":null,"hashTagId":"0126796199493140480","noOfMembers":null,"status":1},"identifier":"4fe7fe33-5e18-4f15-82d2-02255abc1501","phoneVerified":true,"userName":"1598868632-71","rootOrgId":"0126796199493140480","promptTnC":true,"firstName":"1598868632-71","emailVerified":true,"createdDate":"2020-08-31 10:18:17:826+0000","phone":"******4105","userType":"OTHER","status":1}}}"""
    when(restUtilMock.get[Response]("https://dev.sunbirded.org/api/user/v2/read/testUser", Option(Map("x-authenticated-user-token" -> "testUserToken", "Authorization" -> "testBearerToken")))).thenReturn(JSONUtils.deserialize[Response](response1))
    when(accessTokenValidator.getUserId("testUserToken")).thenReturn("testUser")
    val orgRequest = """{"request":{"filters":{"channel":"mhrd"},"offset":0,"limit":1000,"fields":["id"]}}"""
    when(restUtilMock.post[Response]("https://dev.sunbirded.org/api/org/v1/search", orgRequest)).thenReturn(JSONUtils.deserialize[Response]("{\"id\":\"api.org.search\",\"ver\":\"v1\",\"ts\":\"2020-09-14 11:27:41:233+0000\",\"params\":{\"resmsgid\":null,\"msgid\":\"70ae090e-d620-4ba2-972b-865b9ea811a8\",\"err\":null,\"status\":\"success\",\"errmsg\":null},\"responseCode\":\"OK\",\"result\":{\"response\":{\"count\":1,\"content\":[{\"id\":\"channel-mhrd\"}]}}}"))
    result = controller.getTelemetry("raw").apply(FakeRequest().withHeaders(("X-Channel-ID", "testChannel")).withHeaders(("x-authenticated-user-token", "testUserToken")).withHeaders(("Authorization", "testBearerToken")))
    Helpers.status(result) should be (Helpers.OK)

    // Failure cases: user without admin access
    val response2 = """{"id":"api.user.read","ver":"v2","ts":"2020-09-11 07:52:25:227+0000","params":{"resmsgid":null,"msgid":"43aaf2c2-8ac5-456c-8edf-e17bf2b4f1a4","err":null,"status":"success","errmsg":null},"responseCode":"OK","result":{"response":{"tncLatestVersion":"v1","maskedPhone":"******4105","rootOrgName":null,"roles":["PUBLIC"],"channel":"testChannel","stateValidated":false,"isDeleted":false,"organisations":[{"orgJoinDate":"2020-08-31 10:18:17:833+0000","organisationId":"0126796199493140480","isDeleted":false,"hashTagId":"0126796199493140480","roles":["PUBLIC"],"id":"01309794241378713625","userId":"4fe7fe33-5e18-4f15-82d2-02255abc1501"}],"countryCode":"+91","flagsValue":3,"tncLatestVersionUrl":"https://dev-sunbird-temp.azureedge.net/portal/terms-and-conditions-v1.html","maskedEmail":"15***********@yopmail.com","id":"4fe7fe33-5e18-4f15-82d2-02255abc1501","email":"15***********@yopmail.com","rootOrg":{"dateTime":null,"preferredLanguage":null,"approvedBy":null,"channel":"custodian","description":"Pre-prod Custodian Organization","updatedDate":null,"addressId":null,"provider":null,"locationId":null,"orgCode":null,"theme":null,"id":"testChannel","communityId":null,"isApproved":null,"email":null,"slug":"testChannel","identifier":"0126796199493140480","thumbnail":null,"orgName":"Pre-prod Custodian Organization","updatedBy":null,"locationIds":[],"externalId":null,"isRootOrg":true,"rootOrgId":"0126796199493140480","approvedDate":null,"imgUrl":null,"homeUrl":null,"orgTypeId":null,"isDefault":true,"contactDetail":null,"createdDate":"2019-01-18 09:48:13:428+0000","createdBy":"system","parentOrgId":null,"hashTagId":"0126796199493140480","noOfMembers":null,"status":1},"identifier":"4fe7fe33-5e18-4f15-82d2-02255abc1501","phoneVerified":true,"userName":"1598868632-71","rootOrgId":"0126796199493140480","promptTnC":true,"firstName":"1598868632-71","emailVerified":true,"createdDate":"2020-08-31 10:18:17:826+0000","phone":"******4105","userType":"OTHER","status":1}}}"""
    when(restUtilMock.get[Response]("https://dev.sunbirded.org/api/user/v2/read/testUser", Option(Map("x-authenticated-user-token" -> "testUserToken", "Authorization" -> "testBearerToken")))).thenReturn(JSONUtils.deserialize[Response](response2))
    when(accessTokenValidator.getUserId("testUserToken")).thenReturn("testUser")
    result = controller.getTelemetry("raw").apply(FakeRequest().withHeaders(("X-Channel-ID", "testChannel")).withHeaders(("x-authenticated-user-token", "testUserToken")).withHeaders(("Authorization", "testBearerToken")))
    Helpers.status(result) should be (Helpers.FORBIDDEN)
    Helpers.contentAsString(result).indexOf(""""errmsg":"You are not authorized."""") should not be (-1)

    // userChannel matching MHRD tenant
    val response3 = """{"id":"api.user.read","ver":"v2","ts":"2020-09-11 07:52:25:227+0000","params":{"resmsgid":null,"msgid":"43aaf2c2-8ac5-456c-8edf-e17bf2b4f1a4","err":null,"status":"success","errmsg":null},"responseCode":"OK","result":{"response":{"tncLatestVersion":"v1","maskedPhone":"******4105","rootOrgName":null,"roles":["PUBLIC"],"channel":"channel-1","stateValidated":false,"isDeleted":false,"organisations":[{"orgJoinDate":"2020-08-31 10:18:17:833+0000","organisationId":"0126796199493140480","isDeleted":false,"hashTagId":"0126796199493140480","roles":["REPORT_ADMIN"],"id":"01309794241378713625","userId":"4fe7fe33-5e18-4f15-82d2-02255abc1501"}],"countryCode":"+91","flagsValue":3,"tncLatestVersionUrl":"https://dev-sunbird-temp.azureedge.net/portal/terms-and-conditions-v1.html","maskedEmail":"15***********@yopmail.com","id":"4fe7fe33-5e18-4f15-82d2-02255abc1501","email":"15***********@yopmail.com","rootOrg":{"dateTime":null,"preferredLanguage":null,"approvedBy":null,"channel":"channel-mhrd","description":"Pre-prod Custodian Organization","updatedDate":null,"addressId":null,"provider":null,"locationId":null,"orgCode":null,"theme":null,"id":"testChannel","communityId":null,"isApproved":null,"email":null,"slug":"testChannel","identifier":"0126796199493140480","thumbnail":null,"orgName":"Pre-prod Custodian Organization","updatedBy":null,"locationIds":[],"externalId":null,"isRootOrg":true,"rootOrgId":"0126796199493140480","approvedDate":null,"imgUrl":null,"homeUrl":null,"orgTypeId":null,"isDefault":true,"contactDetail":null,"createdDate":"2019-01-18 09:48:13:428+0000","createdBy":"system","parentOrgId":null,"hashTagId":"0126796199493140480","noOfMembers":null,"status":1},"identifier":"4fe7fe33-5e18-4f15-82d2-02255abc1501","phoneVerified":true,"userName":"1598868632-71","rootOrgId":"0126796199493140480","promptTnC":true,"firstName":"1598868632-71","emailVerified":true,"createdDate":"2020-08-31 10:18:17:826+0000","phone":"******4105","userType":"OTHER","status":1}}}"""
    when(restUtilMock.get[Response]("https://dev.sunbirded.org/api/user/v2/read/testUser", Option(Map("x-authenticated-user-token" -> "testUserToken", "Authorization" -> "testBearerToken")))).thenReturn(JSONUtils.deserialize[Response](response3))
    when(accessTokenValidator.getUserId("testUserToken")).thenReturn("testUser")
    val orgRequest3 = """{"request":{"filters":{"channel":"mhrd"},"offset":0,"limit":1000,"fields":["id"]}}"""
    when(restUtilMock.post[Response]("https://dev.sunbirded.org/api/org/v1/search", orgRequest3)).thenReturn(JSONUtils.deserialize[Response]("{\"id\":\"api.org.search\",\"ver\":\"v1\",\"ts\":\"2020-09-14 11:27:41:233+0000\",\"params\":{\"resmsgid\":null,\"msgid\":\"70ae090e-d620-4ba2-972b-865b9ea811a8\",\"err\":null,\"status\":\"success\",\"errmsg\":null},\"responseCode\":\"OK\",\"result\":{\"response\":{\"count\":1,\"content\":[{\"id\":\"channel-mhrd\"}]}}}"))
    result = controller.getTelemetry("raw").apply(FakeRequest().withHeaders(("X-Channel-ID", "channel-1")).withHeaders(("x-authenticated-user-token", "testUserToken")).withHeaders(("Authorization", "testBearerToken")))
    Helpers.status(result) should be (Helpers.OK)

    // Failure cases: userChannel not matching MHRD tenant
    val response4 = """{"id":"api.user.read","ver":"v2","ts":"2020-09-11 07:52:25:227+0000","params":{"resmsgid":null,"msgid":"43aaf2c2-8ac5-456c-8edf-e17bf2b4f1a4","err":null,"status":"success","errmsg":null},"responseCode":"OK","result":{"response":{"tncLatestVersion":"v1","maskedPhone":"******4105","rootOrgName":null,"roles":["PUBLIC"],"channel":"channel-1","stateValidated":false,"isDeleted":false,"organisations":[{"orgJoinDate":"2020-08-31 10:18:17:833+0000","organisationId":"0126796199493140480","isDeleted":false,"hashTagId":"0126796199493140480","roles":["REPORT_ADMIN"],"id":"01309794241378713625","userId":"4fe7fe33-5e18-4f15-82d2-02255abc1501"}],"countryCode":"+91","flagsValue":3,"tncLatestVersionUrl":"https://dev-sunbird-temp.azureedge.net/portal/terms-and-conditions-v1.html","maskedEmail":"15***********@yopmail.com","id":"4fe7fe33-5e18-4f15-82d2-02255abc1501","email":"15***********@yopmail.com","rootOrg":{"dateTime":null,"preferredLanguage":null,"approvedBy":null,"channel":"custodian","description":"Pre-prod Custodian Organization","updatedDate":null,"addressId":null,"provider":null,"locationId":null,"orgCode":null,"theme":null,"id":"testChannel","communityId":null,"isApproved":null,"email":null,"slug":"testChannel","identifier":"0126796199493140480","thumbnail":null,"orgName":"Pre-prod Custodian Organization","updatedBy":null,"locationIds":[],"externalId":null,"isRootOrg":true,"rootOrgId":"0126796199493140480","approvedDate":null,"imgUrl":null,"homeUrl":null,"orgTypeId":null,"isDefault":true,"contactDetail":null,"createdDate":"2019-01-18 09:48:13:428+0000","createdBy":"system","parentOrgId":null,"hashTagId":"0126796199493140480","noOfMembers":null,"status":1},"identifier":"4fe7fe33-5e18-4f15-82d2-02255abc1501","phoneVerified":true,"userName":"1598868632-71","rootOrgId":"0126796199493140480","promptTnC":true,"firstName":"1598868632-71","emailVerified":true,"createdDate":"2020-08-31 10:18:17:826+0000","phone":"******4105","userType":"OTHER","status":1}}}"""
    when(restUtilMock.get[Response]("https://dev.sunbirded.org/api/user/v2/read/testUser", Option(Map("x-authenticated-user-token" -> "testUserToken", "Authorization" -> "testBearerToken")))).thenReturn(JSONUtils.deserialize[Response](response4))
    when(accessTokenValidator.getUserId("testUserToken")).thenReturn("testUser")
    val orgRequest4 = """{"request":{"filters":{"channel":"mhrd"},"offset":0,"limit":1000,"fields":["id"]}}"""
    when(restUtilMock.post[Response]("https://dev.sunbirded.org/api/org/v1/search", orgRequest4)).thenReturn(JSONUtils.deserialize[Response]("{\"id\":\"api.org.search\",\"ver\":\"v1\",\"ts\":\"2020-09-14 11:27:41:233+0000\",\"params\":{\"resmsgid\":null,\"msgid\":\"70ae090e-d620-4ba2-972b-865b9ea811a8\",\"err\":null,\"status\":\"success\",\"errmsg\":null},\"responseCode\":\"OK\",\"result\":{\"response\":{\"count\":1,\"content\":[{\"id\":\"channel-mhrd\"}]}}}"))
    result = controller.getTelemetry("raw").apply(FakeRequest().withHeaders(("X-Channel-ID", "channel-1")).withHeaders(("x-authenticated-user-token", "testUserToken")).withHeaders(("Authorization", "testBearerToken")))
    Helpers.status(result) should be (Helpers.FORBIDDEN)
    Helpers.contentAsString(result).indexOf(""""errmsg":"You are not authorized."""") should not be (-1)

  }

  it should "test get telemetry API - summary rollup data" in {

    reset(cacheUtil);
    reset(mockConfig);
    when(mockConfig.getBoolean("dataexhaust.authorization_check")).thenReturn(true);
    when(cacheUtil.getConsumerChannelTable()).thenReturn(mockTable)
    when(mockTable.get(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(0)

    var result = controller.getTelemetry("summary-rollup").apply(FakeRequest().withHeaders(("X-Channel-ID", "testChannel")));
    Helpers.status(result) should be (Helpers.FORBIDDEN)
    Helpers.contentAsString(result).indexOf(""""errmsg":"Given X-Consumer-ID='' and X-Channel-ID='testChannel' are not authorized"""") should not be (-1)

    reset(mockConfig);
    when(mockConfig.getBoolean("dataexhaust.authorization_check")).thenReturn(true);
    val consumerList = new java.util.ArrayList[String]()
    consumerList.add("trusted-consumer")
    when(mockConfig.getStringList("channel.data_exhaust.whitelisted.consumers")).thenReturn(consumerList);
    result = controller.getTelemetry("summary-rollup").apply(FakeRequest().withHeaders(("X-Channel-ID", "testChannel"),("X-Consumer-ID", "trusted-consumer")));
    Helpers.status(result) should be (Helpers.OK)

    reset(mockConfig);
    when(mockConfig.getBoolean("dataexhaust.authorization_check")).thenReturn(false);
    result = controller.getTelemetry("summary-rollup").apply(FakeRequest().withHeaders(("X-Channel-ID", "testChannel")));
    Helpers.status(result) should be (Helpers.OK)
    
  }

  it should "test refresh cache API" in {

    reset(cacheUtil);
    doNothing().when(cacheUtil).initConsumerChannelCache()
    doNothing().when(cacheUtil).initDeviceLocationCache()

    var result = controller.refreshCache("ConsumerChannel").apply(FakeRequest());
    Helpers.status(result) should be (Helpers.OK)

    result = controller.refreshCache("DeviceLocation").apply(FakeRequest());
    Helpers.status(result) should be (Helpers.OK)
  }

}

