
import akka.actor.ActorSystem
import akka.testkit.{TestActorRef}
import akka.util.Timeout
import com.typesafe.config.Config
import controllers.JobController
import org.ekstep.analytics.api.{APIIds}
import org.ekstep.analytics.api.service.JobAPIService.{ChannelData, DataRequest, DataRequestList, GetDataRequest}
import org.ekstep.analytics.api.service._
import org.ekstep.analytics.api.util.{CacheUtil, CommonUtil}
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import play.api.Configuration
import play.api.libs.json.Json
import play.api.test.{FakeRequest, Helpers}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import com.google.common.collect.Table

@RunWith(classOf[JUnitRunner])
class JobControllerSpec extends FlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {

  implicit val system = ActorSystem()
  implicit val timeout: Timeout = 20.seconds
  implicit val mockConfig = mock[Config];
  private val configurationMock = mock[Configuration]
  private val cacheUtil = mock[CacheUtil]
  private val mockTable = mock[Table[String, String, Integer]];
  when(configurationMock.underlying).thenReturn(mockConfig)


  val jobAPIActor = TestActorRef(new JobAPIService() {
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
      case ChannelData(channel: String, eventType: String, from: String, to: String, config: Config, summaryType: Option[String]) => {
        sender() ! CommonUtil.OK(APIIds.CHANNEL_TELEMETRY_EXHAUST, Map())
      }
    }
  })

  val controller = new JobController(jobAPIActor, system, configurationMock, Helpers.stubControllerComponents(), cacheUtil)

  "JobController" should "test get job API " in {

    when(mockConfig.getBoolean("dataexhaust.authorization_check")).thenReturn(true);
    when(cacheUtil.getConsumerChannelTable()).thenReturn(mockTable)
    when(mockTable.get(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(1)
    var result = controller.getJob("client1", "request1").apply(FakeRequest())
    Helpers.status(result) should be (Helpers.OK)

    reset(cacheUtil);
    when(cacheUtil.getConsumerChannelTable()).thenReturn(mockTable)
    when(mockTable.get(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(0)
    result = controller.getJob("client1", "request1").apply(FakeRequest())
    Helpers.status(result) should be (Helpers.FORBIDDEN)
    Helpers.contentAsString(result).indexOf(""""errmsg":"Given X-Consumer-ID and X-Channel-ID are not authorized"""") should not be (-1)

    reset(cacheUtil);
    reset(mockConfig);
    when(mockConfig.getBoolean("dataexhaust.authorization_check")).thenReturn(false);
    when(cacheUtil.getConsumerChannelTable()).thenReturn(mockTable)
    when(mockTable.get(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(0)
    result = controller.getJob("client1", "request1").apply(FakeRequest())
    Helpers.status(result) should be (Helpers.OK)
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
    var result = controller.getJobList("testClientKey").apply(FakeRequest());
    Helpers.status(result) should be (Helpers.FORBIDDEN)
    Helpers.contentAsString(result).indexOf(""""errmsg":"Given X-Consumer-ID='' and X-Channel-ID='' are not authorized"""") should not be (-1)

    reset(mockConfig);
    when(mockConfig.getBoolean("dataexhaust.authorization_check")).thenReturn(false);
    when(mockConfig.getString("data_exhaust.list.limit")).thenReturn("10");

    result = controller.getJobList("testClientKey").apply(FakeRequest());
    Helpers.status(result) should be (Helpers.OK)
  }

  it should "test get telemetry API" in {

    reset(cacheUtil);
    reset(mockConfig);
    when(mockConfig.getBoolean("dataexhaust.authorization_check")).thenReturn(true);
    when(cacheUtil.getConsumerChannelTable()).thenReturn(mockTable)
    when(mockTable.get(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(0)

    var result = controller.getTelemetry("testDataSet").apply(FakeRequest());
    Helpers.status(result) should be (Helpers.FORBIDDEN)
    Helpers.contentAsString(result).indexOf(""""errmsg":"Given X-Consumer-ID='' and X-Channel-ID='' are not authorized"""") should not be (-1)

    reset(mockConfig);
    when(mockConfig.getBoolean("dataexhaust.authorization_check")).thenReturn(false);
    result = controller.getTelemetry("testDataSet").apply(FakeRequest());
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

