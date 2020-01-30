
import akka.actor.ActorSystem
import akka.testkit.{TestActorRef}
import com.typesafe.config.Config
import controllers.DeviceController
import org.ekstep.analytics.api.service.{DeviceProfile, DeviceProfileRequest, DeviceProfileService, DeviceRegisterFailureAck, DeviceRegisterService, DeviceRegisterSuccesfulAck, Location, RegisterDevice, SaveMetricsActor}
import org.ekstep.analytics.api.util.{ElasticsearchService, KafkaUtil, RedisUtil}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import play.api.Configuration
import play.api.libs.json.{Json}
import play.api.test.{FakeRequest, Helpers}
import akka.util.Timeout
import org.ekstep.analytics.api.service.experiment.{ExperimentData, ExperimentRequest, ExperimentService}
import scala.concurrent.{Future}
import akka.pattern.pipe

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

@RunWith(classOf[JUnitRunner])
class DeviceControllerSpec extends FlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {

  implicit val system = ActorSystem()
  implicit val timeout: Timeout = 20.seconds
  private val configMock = mock[Config]
  private val configurationMock = mock[Configuration]

  private val redisUtilMock = mock[RedisUtil]
  private val kafkaUtilMock = mock[KafkaUtil]
  val saveMetricsActor = TestActorRef(new SaveMetricsActor(kafkaUtilMock))

  val deviceRegisterActor = TestActorRef(new DeviceRegisterService(saveMetricsActor, configMock, redisUtilMock, kafkaUtilMock) {
    override def receive: Receive = {
      case msg:RegisterDevice => {
        if(msg.did.equals("device123") || msg.did.equals("device125")) {
          sender() ! Option(DeviceRegisterSuccesfulAck)
        } else if(msg.did.equals("device124")) {
          sender() ! DeviceRegisterFailureAck
        } else {
          sender() ! Option(DeviceRegisterFailureAck)
        }
      }
    }
  })

  val deviceProfileActor = TestActorRef(new DeviceProfileService(configMock, redisUtilMock) {
    override def receive: Receive = {
      case dp: DeviceProfileRequest => {
        if("device124".equals(dp.did)) {
          sender() ! None
        } else if("device125".equals(dp.did)) {
          sender() ! DeviceProfile(Option(Location("Karnataka", "Bangalore")), Option(Location("Karnataka", "Belgaum")))
        } else {
          sender() ! Option(DeviceProfile(Option(Location("Karnataka", "Bangalore")), Option(Location("Karnataka", "Belgaum"))))
        }

      }
    }
  })

  val expActor = TestActorRef(new ExperimentService(redisUtilMock, mock[ElasticsearchService]) {
    override def receive: Receive = {
      case req: ExperimentRequest => {
        val senderActor = sender()

        if("device123".equals(req.deviceId.get)) {
          val result = Future {
            Option(ExperimentData("exp1", "Exp 1", "2020-01-01", "2020-01-31", "key1", "test", "user1", "device1", 0, 0))
          }
          result.pipeTo(senderActor)
        } else if("device125".equals(req.deviceId.get)) {
          senderActor ! ExperimentData("exp1", "Exp 1", "2020-01-01", "2020-01-31", "key1", "test", "user1", "device1", 0, 0)
        } else {
          senderActor ! None
        }

      }
    }
  })

  val controller = new DeviceController(deviceRegisterActor, deviceProfileActor, expActor, system, configurationMock, Helpers.stubControllerComponents(), null)

  "DeviceController" should "invoke device register API " in {

    when(configurationMock.getOptional[Boolean]("deviceRegisterAPI.experiment.enable")).thenReturn(Option(false));
    var fakeRequest = FakeRequest().withHeaders(("X-Forwarded-For", "88.22.146.124")).withJsonBody(Json.parse("""{"id":"sunbird.portal","ver":"2.6.5","ts":"2020-01-22T13:58:11+05:30","params":{"msgid":"a1687e7f-ede7-e433-f6a6-18a18333e7ff"},"request":{"did":"93c4a9302e8ecb600f8aada6f9cd192c","producer":"sunbird.portal","ext":{"userid":"user1","url":"http://sunbird.org"},"dspec":{"os":"Ubuntu"},"uaspec":{"agent":"Chrome","ver":"79.0.3945.74","system":"mac-os-x-15","platform":"Mac","raw":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.74 Safari/537.36"},"userDeclaredLocation":{"state":"Karnataka","district":"Belagavi"}}}"""))
    var result = controller.registerDevice("device123").apply(fakeRequest)
    Helpers.status(result) should be (200)
    Helpers.contentAsString(result).indexOf(""""result":{"message":"Device registered successfully","actions":[]}""") should not be (-1)

    fakeRequest = FakeRequest().withHeaders(("X-Forwarded-For", "192.168.0.1,88.22.146.124")).withJsonBody(Json.parse("""{"id":"sunbird.portal","ver":"2.6.5","ts":"2020-01-22T13:58:11+05:30","params":{"msgid":"a1687e7f-ede7-e433-f6a6-18a18333e7ff"},"request":{"did":"93c4a9302e8ecb600f8aada6f9cd192c","producer":"sunbird.portal","ext":{"url":"http://sunbird.org"},"dspec":{"os":"Ubuntu"},"uaspec":{"agent":"Chrome","ver":"79.0.3945.74","system":"mac-os-x-15","platform":"Mac","raw":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.74 Safari/537.36"},"userDeclaredLocation":{"state":"Karnataka","district":"Belagavi"}}}"""))
    result = controller.registerDevice("device123").apply(fakeRequest)
    Helpers.status(result) should be (200)
    Helpers.contentAsString(result).indexOf(""""result":{"message":"Device registered successfully","actions":[]}""") should not be (-1)

    fakeRequest = FakeRequest().withHeaders(("X-Forwarded-For", "192.168.0.1,88.22.146.124")).withJsonBody(Json.parse("""{"id":"sunbird.portal","ver":"2.6.5","ts":"2020-01-22T13:58:11+05:30","params":{"msgid":"a1687e7f-ede7-e433-f6a6-18a18333e7ff"},"request":{"did":"93c4a9302e8ecb600f8aada6f9cd192c","producer":"sunbird.portal","ext":{"userid":"user1"},"dspec":{"os":"Ubuntu"},"uaspec":{"agent":"Chrome","ver":"79.0.3945.74","system":"mac-os-x-15","platform":"Mac","raw":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.74 Safari/537.36"},"userDeclaredLocation":{"state":"Karnataka","district":"Belagavi"}}}"""))
    result = controller.registerDevice("device124").apply(fakeRequest)
    Helpers.status(result) should be (500)
  }

  it should "invoke the device register API and return experiment data" in {

    reset(configurationMock)
    when(configurationMock.getOptional[Boolean]("deviceRegisterAPI.experiment.enable")).thenReturn(Option(true));

    var fakeRequest = FakeRequest().withHeaders(("X-Forwarded-For", "192.168.0.1,88.22.146.124")).withJsonBody(Json.parse("""{"id":"sunbird.portal","ver":"2.6.5","ts":"2020-01-22T13:58:11+05:30","params":{"msgid":"a1687e7f-ede7-e433-f6a6-18a18333e7ff"},"request":{"did":"93c4a9302e8ecb600f8aada6f9cd192c","producer":"sunbird.portal","ext":{"userid":"user1"},"dspec":{"os":"Ubuntu"},"uaspec":{"agent":"Chrome","ver":"79.0.3945.74","system":"mac-os-x-15","platform":"Mac","raw":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.74 Safari/537.36"},"userDeclaredLocation":{"state":"Karnataka","district":"Belagavi"}}}"""))
    var result = controller.registerDevice("device123").apply(fakeRequest)
    Helpers.status(result) should be (200)
    Helpers.contentAsString(result).indexOf(""""result":{"message":"Device registered successfully","actions":[{"type":"experiment","data":{"endDate":"2020-01-31","experimentName":"Exp 1","key":"key1","experimentId":"exp1","title":"experiment","startDate":"2020-01-01"}}]}""") should not be (-1)

    fakeRequest = FakeRequest().withHeaders(("X-Forwarded-For", "192.168.0.1,88.22.146.124")).withJsonBody(Json.parse("""{"id":"sunbird.portal","ver":"2.6.5","ts":"2020-01-22T13:58:11+05:30","params":{"msgid":"a1687e7f-ede7-e433-f6a6-18a18333e7ff"},"request":{"did":"93c4a9302e8ecb600f8aada6f9cd192c","producer":"sunbird.portal","ext":{"userid":"user1"},"dspec":{"os":"Ubuntu"},"uaspec":{"agent":"Chrome","ver":"79.0.3945.74","system":"mac-os-x-15","platform":"Mac","raw":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.74 Safari/537.36"},"userDeclaredLocation":{"state":"Karnataka","district":"Belagavi"}}}"""))
    result = controller.registerDevice("device126").apply(fakeRequest)
    Helpers.status(result) should be (200)
    Helpers.contentAsString(result).indexOf(""""result":{"message":"Device registered successfully","actions":[]}""") should not be (-1)

    fakeRequest = FakeRequest().withHeaders(("X-Forwarded-For", "192.168.0.1,88.22.146.124")).withJsonBody(Json.parse("""{"id":"sunbird.portal","ver":"2.6.5","ts":"2020-01-22T13:58:11+05:30","params":{"msgid":"a1687e7f-ede7-e433-f6a6-18a18333e7ff"},"request":{"did":"93c4a9302e8ecb600f8aada6f9cd192c","producer":"sunbird.portal","ext":{"userid":"user1"},"dspec":{"os":"Ubuntu"},"uaspec":{"agent":"Chrome","ver":"79.0.3945.74","system":"mac-os-x-15","platform":"Mac","raw":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.74 Safari/537.36"},"userDeclaredLocation":{"state":"Karnataka","district":"Belagavi"}}}"""))
    result = controller.registerDevice("device125").apply(fakeRequest)
    Helpers.status(result) should be (500)
  }

  it should "invoke the device register API" in {

    var fakeRequest = FakeRequest().withHeaders(("X-Forwarded-For", "88.22.146.124"));
    var result = controller.getDeviceProfile("device123").apply(fakeRequest)
    Helpers.status(result) should be (200)
    Helpers.contentAsString(result).indexOf(""""result":{"userDeclaredLocation":{"state":"Karnataka","district":"Bangalore"},"ipLocation":{"state":"Karnataka","district":"Belgaum"}}""") should not be (-1)

    fakeRequest = FakeRequest().withHeaders(("X-Forwarded-For", "192.168.0.1,88.22.146.124"));
    result = controller.getDeviceProfile("device124").apply(fakeRequest)
    Helpers.status(result) should be (500)
    Helpers.contentAsString(result).indexOf(""""errmsg":"IP is missing in the header""") should not be (-1)

    fakeRequest = FakeRequest().withHeaders(("X-Forwarded-For", "192.168.0.1,88.22.146.124"));
    result = controller.getDeviceProfile("device125").apply(fakeRequest)
    Helpers.status(result) should be (500)
  }
}

