import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import akka.util.Timeout
import com.typesafe.config.Config
import controllers.GroupController
import org.ekstep.analytics.api.{Params, Response, ResponseCode}
import org.ekstep.analytics.api.service.GroupAPIService
import org.ekstep.analytics.api.service.GroupAPIService.GetActivityAggregatesRequest
import org.junit.runner.RunWith
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import play.api.Configuration
import play.api.libs.json.Json
import play.api.test.{FakeRequest, Helpers}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

@RunWith(classOf[JUnitRunner])
class GroupControllerSpec extends FlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {

	implicit val system = ActorSystem()
	implicit val timeout: Timeout = 20.seconds
	implicit val mockConfig = mock[Config];
	private val configurationMock = mock[Configuration]
	when(configurationMock.underlying).thenReturn(mockConfig)

	val groupActor = TestActorRef(new GroupAPIService() {
		override def receive: Receive = {
			case GetActivityAggregatesRequest(request: String, config: Config) => sender() ! Response("ekstep.analytics.group.activity.aggregates.get", "1.0", "ts", Params(UUID.randomUUID().toString(), null, null, "successful", null), ResponseCode.OK.toString(), Option(Map()))
		}
	})


	val controller = new GroupController(groupActor, system, configurationMock, Helpers.stubControllerComponents())

	"GroupController getActivityAggregates" should "fetch and return the activity aggregates" in {
		val result = controller.getActivityAggregates().apply(FakeRequest().withJsonBody(Json.parse("""{}""")))
		Helpers.status(result) should be(Helpers.OK)
	}

}
