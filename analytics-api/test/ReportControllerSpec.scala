import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import akka.util.Timeout
import com.typesafe.config.Config
import controllers.ReportController
import org.ekstep.analytics.api.service._
import org.ekstep.analytics.api.util.{PostgresDBUtil, ReportConfig}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import play.api.Configuration
import play.api.libs.json.Json
import play.api.test.{FakeRequest, Helpers}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class ReportControllerSpec extends FlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {

    implicit val system = ActorSystem()
    private val postgresUtilMock = mock[PostgresDBUtil]
    implicit val mockConfig = mock[Config]
    private val configurationMock = mock[Configuration]
    when(configurationMock.underlying).thenReturn(mockConfig)
    implicit val timeout: Timeout = 20.seconds
    val reportConfig = ReportConfig("report-Id", 0L, "desc", "user1", "monthly", Map.empty, 0L, 0L, "submitted", "submitted")
    when(postgresUtilMock.readReport("district_monthly")).thenReturn(Some(reportConfig))
    val reportActor = TestActorRef(new ReportAPIService(postgresUtilMock) {
        override def receive: Receive = {
            case SubmitReportRequest(request: String, config: Config) => sender() ! submitReport(request)(config)
            case GetReportRequest(reportId: String, config: Config) => sender() ! getReport(reportId)(config)
            case GetReportListRequest(request: String, config: Config) => sender() ! getReportList(request)(config)
            case DeactivateReportRequest(reportId: String, config: Config) => sender() ! getReport(reportId)(config)
            case UpdateReportRequest(reportId: String, request: String, config: Config) => sender() ! updateReport(reportId, request)(config)

        }
    })
    val controller = new ReportController(reportActor, system, configurationMock, Helpers.stubControllerComponents())

    "ReportController" should "test the submit report and get report  " in {
        val result = controller.submitReport().apply(FakeRequest().withJsonBody(Json.parse("""{"id":"ekstep.analytics.report.request.submit","ver":"1.0","ts":"2020-03-30T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"reportId":"district_monthly","createdBy":"User1","description":"UniqueDevice district wise monthly","reportSchedule":"Monthly","config":{"reportConfig":{"id":"district_monthly","queryType":"groupBy","dateRange":{"staticInterval":"LastMonth","granularity":"all"},"metrics":[{"metric":"totalUniqueDevices","label":"Total Unique Devices","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastMonth","aggregations":[{"name":"total_unique_devices","type":"cardinality","fieldName":"context_did"}],"dimensions":[{"fieldName":"derived_loc_state","aliasName":"state"},{"fieldName":"derived_loc_district","aliasName":"district"}],"filters":[{"type":"in","dimension":"context_pdata_id","values":["__producerEnv__.diksha.portal","__producerEnv__.diksha.app"]},{"type":"isnotnull","dimension":"derived_loc_state"},{"type":"isnotnull","dimension":"derived_loc_district"}],"descending":"false"}}],"labels":{"state":"State","district":"District","total_unique_devices":"Number of Unique Devices"},"output":[{"type":"csv","metrics":["total_unique_devices"],"dims":["state"],"fileParameters":["id","dims"]}]},"store":"__store__","container":"__container__","key":"druid-reports/"}}}""")))
        Helpers.status(result) should be(Helpers.OK)
    }

    "ReportController" should "test the get report and get report  " in {
        val result = controller.getReport("district_monthly").apply(FakeRequest())
        Helpers.status(result) should be(Helpers.OK)
    }

    "ReportController" should "test the deactivate report " in {
        val result = controller.deactivateReport("district_monthly").apply(FakeRequest())
        Helpers.status(result) should be(Helpers.OK)
    }


    "ReportController" should "test the update report" in {
        val result = controller.updateReport("district_monthly").apply(FakeRequest().withJsonBody(Json.parse("""{"id":"ekstep.analytics.report.request.submit","ver":"1.0","ts":"2020-03-30T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"reportId":"district_monthly","createdBy":"User1","description":"UniqueDevice district wise monthly","reportSchedule":"Monthly","config":{"reportConfig":{"id":"district_monthly","queryType":"groupBy","dateRange":{"staticInterval":"LastMonth","granularity":"all"},"metrics":[{"metric":"totalUniqueDevices","label":"Total Unique Devices","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastMonth","aggregations":[{"name":"total_unique_devices","type":"cardinality","fieldName":"context_did"}],"dimensions":[{"fieldName":"derived_loc_state","aliasName":"state"},{"fieldName":"derived_loc_district","aliasName":"district"}],"filters":[{"type":"in","dimension":"context_pdata_id","values":["__producerEnv__.diksha.portal","__producerEnv__.diksha.app"]},{"type":"isnotnull","dimension":"derived_loc_state"},{"type":"isnotnull","dimension":"derived_loc_district"}],"descending":"false"}}],"labels":{"state":"State","district":"District","total_unique_devices":"Number of Unique Devices"},"output":[{"type":"csv","metrics":["total_unique_devices"],"dims":["state"],"fileParameters":["id","dims"]}]},"store":"__store__","container":"__container__","key":"druid-reports/"}}}""")))
        Helpers.status(result) should be(Helpers.OK)
    }

    "ReportController" should "test the get List Report " in {

        val reportConfig = ReportConfig("report-Id", 0L, "desc", "user1", "monthly", Map.empty, 0L, 0L, "submitted", "submitted")
        when(postgresUtilMock.readReportList(List("ACTIVE", "SUBMITTED"))).thenReturn(List(reportConfig))
        val result = controller.getReportList().apply(FakeRequest().withJsonBody(Json.parse("""{"request":{"filters":{"status":["ACTIVE","SUBMITTED"]}}}""")))
        Helpers.status(result) should be(Helpers.OK)
    }
}
