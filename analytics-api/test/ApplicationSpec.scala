import org.junit.runner._
import org.specs2.runner._
import play.api.test.Helpers._
import play.api.test._
import play.api.mvc.{RequestHeader, Action, Result}
import play.api.mvc.Results._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Add your spec here.
 * You can mock out a whole application including requests, plugins etc.
 * For more information, consult the wiki.
 */
@RunWith(classOf[JUnitRunner])
class ApplicationSpec extends BaseSpec {

	"Application" should new WithApplication {

		"send 404 on a bad request" in {
			route(app, FakeRequest(GET, "/boum")) must beSome.which(status(_) == NOT_FOUND)
		}

		"return api health status report - successful response" in {
			val response = route(app, FakeRequest(GET, "/health")).get
			status(response) must equalTo(OK)
		}

	}
}
