
import akka.actor.ActorSystem
import akka.testkit.{TestActorRef}
import akka.util.Timeout
import com.typesafe.config.Config
import controllers.ExperimentController
import org.ekstep.analytics.api.service.ExperimentAPIService.{CreateExperimentRequest, GetExperimentRequest}
import org.ekstep.analytics.api.service._
import org.ekstep.analytics.api.util.{CommonUtil}
import org.ekstep.analytics.api.{APIIds, ExperimentBodyResponse, ExperimentParams}
import org.junit.runner.RunWith
import org.mockito.Mockito.when
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import play.api.Configuration
import play.api.libs.json.Json
import play.api.test.{FakeRequest, Helpers}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

@RunWith(classOf[JUnitRunner])
class ExperimentControllerSpec extends FlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {

  implicit val system = ActorSystem()
  implicit val timeout: Timeout = 20.seconds
  implicit val mockConfig = mock[Config];
  private val configurationMock = mock[Configuration]
  when(configurationMock.underlying).thenReturn(mockConfig)

  val experimentActor = TestActorRef(new ExperimentAPIService() {
    override def receive: Receive = {
      case CreateExperimentRequest(request: String, config: Config) => sender() ! ExperimentBodyResponse("exp1", "1.0", "", ExperimentParams("", "", "", "", Map()), "OK", Option(Map()))
      case GetExperimentRequest(requestId: String, config: Config) => sender() ! CommonUtil.OK(APIIds.EXPERIEMNT_GET_REQUEST, Map())
    }
  })


  val controller = new ExperimentController(experimentActor, system, configurationMock, Helpers.stubControllerComponents())

  "ExperimentController" should "test the save experiment and get experiment API " in {
    var result = controller.createExperiment().apply(FakeRequest().withJsonBody(Json.parse("""{}""")))
    Helpers.status(result) should be(Helpers.OK)

    result = controller.getExperiment("exp1").apply(FakeRequest());
    Helpers.status(result) should be(Helpers.OK)
  }

}

