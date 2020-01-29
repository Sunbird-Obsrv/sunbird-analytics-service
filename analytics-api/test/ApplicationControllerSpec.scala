
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestActorRef
import akka.util.Timeout
import com.typesafe.config.Config
import controllers.Application
import org.ekstep.analytics.api.service._
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import play.api.Configuration
import play.api.libs.json.Json
import play.api.test.{FakeRequest}
import org.ekstep.analytics.framework.util.HTTPClient
import play.api.libs.typedmap.{TypedMap}
import play.api.mvc.{RequestHeader, Result}
import play.api.mvc.Results.Ok
import play.api.routing.{HandlerDef, Router}
import play.api.test.Helpers

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class ApplicationControllerSpec extends FlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {

  implicit val system = ActorSystem()
  implicit val timeout: Timeout = 20.seconds
  implicit val mockConfig = mock[Config];
  private val configurationMock = mock[Configuration]
  private val mockRestUtil = mock[HTTPClient]
  private val healthCheckService = mock[HealthCheckAPIService]
  when(configurationMock.underlying).thenReturn(mockConfig)


  val clientLogAPIActor = TestActorRef(new ClientLogsAPIService() {
    override def receive: Receive = {
      case ClientLogRequest(request: Option[ClientRequestBody]) => {
      }
    }
  })

  val druidHealthActor = TestActorRef(new DruidHealthCheckService(mockRestUtil) {
    override def receive: Receive = {
      case "health" => sender() ! "Ok"
    }
  })

  val controller = new Application(clientLogAPIActor, druidHealthActor, healthCheckService, Helpers.stubControllerComponents(), system, configurationMock)

  "Application" should "test all its APIs " in {

    var result = controller.getDruidHealthStatus().apply(FakeRequest());
    Helpers.status(result) should be (Helpers.OK)

    when(healthCheckService.getHealthStatus()).thenReturn("OK");
    result = controller.checkAPIhealth().apply(FakeRequest());
    Helpers.status(result) should be (Helpers.OK)

    val validRequest = "{\"request\":{\"pdata\":{\"id\":\"contentPlayer\",\"ver\":\"1.0\",\"pid\":\"sunbird.portal\"},\"context\":{\"did\":\"1242-234234-24234-234234\",\"dspec\":{\"os\":\"mac\",\"make\":\"\",\"mem\":0,\"idisk\":\"\",\"edisk\":\"\",\"scrn\":\"\",\"camera\":\"\",\"cpu\":\"\",\"sims\":0,\"uaspec\":{\"agent\":\"\",\"ver\":\"\",\"system\":\"\",\"platform\":\"\",\"raw\":\"\"}},\"extras\":{\"key-123\":\"value-123\",\"key-1234\":\"value-123\",\"key-1235\":\"value-123\"}},\"logs\":[{\"id\":\"13123-123123-12312-3123\",\"ts\":1560346371,\"log\":\"Exception in thread \\\"main\\\" java.lang.NullPointerException\\n        at com.example.myproject.Book.getTitle(Book.java:16)\\n        at com.example.myproject.Author.getBookTitles(Author.java:25)\\n        at com.example.myproject.Bootstrap.main(Bootstrap.java:14)\\n\"},{\"id\":\"13123-123123-12312-3123\",\"ts\":1560346371,\"log\":\"Exception in thread \\\"main\\\" java.lang.NullPointerException\\n        at com.example.myproject.Book.getTitle(Book.java:16)\\n        at com.example.myproject.Author.getBookTitles(Author.java:25)\\n        at com.example.myproject.Bootstrap.main(Bootstrap.java:14)\\n\"}]}}"
    result = controller.logClientErrors().apply(FakeRequest().withJsonBody(Json.parse(validRequest)));
    Helpers.status(result) should be (Helpers.OK)

    val invalidRequest = "{\"request\":{}}"
    result = controller.logClientErrors().apply(FakeRequest().withJsonBody(Json.parse(invalidRequest)));
    Helpers.status(result) should be (Helpers.BAD_REQUEST)

    val invalidJson = "{\"request\":\"test\"}";
    result = controller.logClientErrors().apply(FakeRequest().withJsonBody(Json.parse(invalidJson)));
    Helpers.status(result) should be (Helpers.INTERNAL_SERVER_ERROR)
  }

  it should "test request interceptor in" in {
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    val interceptor = new filter.RequestInterceptor()

    val rh = FakeRequest("GET", "/health?filter=druid,redis&name=MyName&description=Blablabla").withAttrs(TypedMap.empty.updated(Router.Attrs.HandlerDef, new HandlerDef(null, "", "TestController", "GET", null, "", "")));
    val action: (RequestHeader) => Future[Result] = {
      requestHeader =>
        Future.successful(Ok("success"))
    }
    val result = interceptor(action)(rh);
    val requestTime = Integer.parseInt(Helpers.header("Request-Time", result).get);
    Helpers.status(result) should be (Helpers.OK)
    requestTime should be > 0;
  }
}

