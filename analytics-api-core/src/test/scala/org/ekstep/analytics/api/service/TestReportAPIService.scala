package org.ekstep.analytics.api.service

import java.util.Date

import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.ekstep.analytics.api.Response
import org.ekstep.analytics.api.util.{EmbeddedPostgresql, PostgresDBUtil}
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.util.JSONUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}

class TestReportAPIService extends FlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {


    implicit val mockFc = mock[FrameworkContext]
    implicit val config = ConfigFactory.load()
    private implicit val system: ActorSystem = ActorSystem("test-actor-system", config)
    private val postgresUtil = new PostgresDBUtil
    val reportApiServiceActorRef = TestActorRef(new ReportAPIService(postgresUtil))
    implicit val executionContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
    implicit val timeout: Timeout = 20.seconds

    override def beforeAll(): Unit = {
        super.beforeAll()
        EmbeddedPostgresql.start()
        EmbeddedPostgresql.createTables()
        EmbeddedPostgresql.execute(
            s"""insert into report_config (report_id, updated_on, report_description, requested_by,
      report_schedule, config, created_on, submitted_on, status, status_msg) values
      ('district_weekly', '${new Date()}', 'District Weekly Description',
        'User1','Weekly' , '{"reportConfig":{"id":"district_weekly","queryType":"groupBy","dateRange":{"staticInterval":"LastMonth","granularity":"all"},"metrics":[{"metric":"totalUniqueDevices","label":"Total Unique Devices","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastMonth","aggregations":[{"name":"total_unique_devices","type":"cardinality","fieldName":"context_did"}],"dimensions":[{"fieldName":"derived_loc_state","aliasName":"state"},{"fieldName":"derived_loc_district","aliasName":"district"}],"filters":[{"type":"in","dimension":"context_pdata_id","values":["__producerEnv__.diksha.portal","__producerEnv__.diksha.app"]},{"type":"isnotnull","dimension":"derived_loc_state"},{"type":"isnotnull","dimension":"derived_loc_district"}],"descending":"false"}}],"labels":{"state":"State","district":"District","total_unique_devices":"Number of Unique Devices"},"output":[{"type":"csv","metrics":["total_unique_devices"],"dims":["state"],"fileParameters":["id","dims"]}]},"store":"__store__","container":"__container__","key":"druid-reports/"}',
        '${new Date()}', '${new Date()}' ,'ACTIVE', 'Report Updated')""")
    }

    override def afterAll(): Unit = {
        super.afterAll()
        EmbeddedPostgresql.close()
    }


    "ReportAPIService" should "return success response for report request" in {
        val postgresUtil = new PostgresDBUtil
        val request = """{"id":"ekstep.analytics.report.request.submit","ver":"1.0","ts":"2020-03-30T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"reportId":"district_monthly","createdBy":"User1","description":"UniqueDevice district wise monthly","reportSchedule":"Monthly","config":{"reportConfig":{"id":"district_monthly","queryType":"groupBy","dateRange":{"staticInterval":"LastMonth","granularity":"all"},"metrics":[{"metric":"totalUniqueDevices","label":"Total Unique Devices","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastMonth","aggregations":[{"name":"total_unique_devices","type":"cardinality","fieldName":"context_did"}],"dimensions":[{"fieldName":"derived_loc_state","aliasName":"state"},{"fieldName":"derived_loc_district","aliasName":"district"}],"filters":[{"type":"in","dimension":"context_pdata_id","values":["__producerEnv__.diksha.portal","__producerEnv__.diksha.app"]},{"type":"isnotnull","dimension":"derived_loc_state"},{"type":"isnotnull","dimension":"derived_loc_district"}],"descending":"false"}}],"labels":{"state":"State","district":"District","total_unique_devices":"Number of Unique Devices"},"output":[{"type":"csv","metrics":["total_unique_devices"],"dims":["state"],"fileParameters":["id","dims"]}]},"store":"__store__","container":"__container__","key":"druid-reports/"}}}"""
        val response = reportApiServiceActorRef.underlyingActor.submitReport(request)
        response.responseCode should be("OK")
        response.params.status should be("successful")
    }

    "ReportAPIService" should "return failed response for data request" in {
        val request = """{"id":"ekstep.analytics.report.request.submit","ver":"1.0","ts":"2020-03-30T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"reportId":"district_weekly","createdBy":"User1","description":"UniqueDevice district wise monthly","reportSchedule":"Monthly","config":{"reportConfig":{"id":"district_monthly","queryType":"groupBy","dateRange":{"staticInterval":"LastMonth","granularity":"all"},"metrics":[{"metric":"totalUniqueDevices","label":"Total Unique Devices","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastMonth","aggregations":[{"name":"total_unique_devices","type":"cardinality","fieldName":"context_did"}],"dimensions":[{"fieldName":"derived_loc_state","aliasName":"state"},{"fieldName":"derived_loc_district","aliasName":"district"}],"filters":[{"type":"in","dimension":"context_pdata_id","values":["__producerEnv__.diksha.portal","__producerEnv__.diksha.app"]},{"type":"isnotnull","dimension":"derived_loc_state"},{"type":"isnotnull","dimension":"derived_loc_district"}],"descending":"false"}}],"labels":{"state":"State","district":"District","total_unique_devices":"Number of Unique Devices"},"output":[{"type":"csv","metrics":["total_unique_devices"],"dims":["state"],"fileParameters":["id","dims"]}]},"store":"__store__","container":"__container__","key":"druid-reports/"}}}"""
        val response = reportApiServiceActorRef.underlyingActor.submitReport(request)
        response.params.status should be("failed")
    }

    "ReportAPIService" should "return response for get report request" in {
        val response = reportApiServiceActorRef.underlyingActor.getReport("district_weekly")
        response.responseCode should be("OK")
    }

    "ReportAPIService" should "return failed response for get report request if report id not available" in {
        val response = reportApiServiceActorRef.underlyingActor.getReport("district_month")
        response.params.status should be("failed")
    }

    "ReportAPIService" should "return the list of reports" in {
        val request = """{"request":{"filters":{"status":["ACTIVE","SUBMITTED"]}}}"""
        val response = reportApiServiceActorRef.underlyingActor.getReportList(request)
        response.responseCode should be("OK")

    }

    "ReportAPIService" should "return empty list of reports" in {
        val request = """{"request":{"filters":{"status":["INACTIVE"]}}}"""
        val response = reportApiServiceActorRef.underlyingActor.getReportList(request)
        response.params.status should be("failed")

    }

    "ReportAPIService" should "should update the report with valid reportId" in {
        val request = """{"id":"ekstep.analytics.report.request.submit","ver":"1.0","ts":"2020-03-30T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"reportId":"district_weekly","createdBy":"User1","description":"UniqueDevice district wise monthly","reportSchedule":"Weekly","config":{"reportConfig":{"id":"district_weekly","queryType":"groupBy","dateRange":{"staticInterval":"LastMonth","granularity":"all"},"metrics":[{"metric":"totalUniqueDevices","label":"Total Unique Devices","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastMonth","aggregations":[{"name":"total_unique_devices","type":"cardinality","fieldName":"context_did"}],"dimensions":[{"fieldName":"derived_loc_state","aliasName":"state"},{"fieldName":"derived_loc_district","aliasName":"district"}],"filters":[{"type":"in","dimension":"context_pdata_id","values":["__producerEnv__.diksha.portal","__producerEnv__.diksha.app"]},{"type":"isnotnull","dimension":"derived_loc_state"},{"type":"isnotnull","dimension":"derived_loc_district"}],"descending":"false"}}],"labels":{"state":"State","district":"District","total_unique_devices":"Number of Unique Devices"},"output":[{"type":"csv","metrics":["total_unique_devices"],"dims":["state"],"fileParameters":["id","dims"]}]},"store":"__store__","container":"__container__","key":"druid-reports/"}}}"""
        val response = reportApiServiceActorRef.underlyingActor.updateReport("district_weekly", request)
        response.params.status should be("successful")
    }

    "ReportAPIService" should "should not update and send error response with invalid reportId" in {
        val request = """{"id":"ekstep.analytics.report.request.submit","ver":"1.0","ts":"2020-03-30T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"reportId":"district_weekly","createdBy":"User1","description":"UniqueDevice district wise monthly","reportSchedule":"Weekly","config":{"reportConfig":{"id":"district_weekly","queryType":"groupBy","dateRange":{"staticInterval":"LastMonth","granularity":"all"},"metrics":[{"metric":"totalUniqueDevices","label":"Total Unique Devices","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastMonth","aggregations":[{"name":"total_unique_devices","type":"cardinality","fieldName":"context_did"}],"dimensions":[{"fieldName":"derived_loc_state","aliasName":"state"},{"fieldName":"derived_loc_district","aliasName":"district"}],"filters":[{"type":"in","dimension":"context_pdata_id","values":["__producerEnv__.diksha.portal","__producerEnv__.diksha.app"]},{"type":"isnotnull","dimension":"derived_loc_state"},{"type":"isnotnull","dimension":"derived_loc_district"}],"descending":"false"}}],"labels":{"state":"State","district":"District","total_unique_devices":"Number of Unique Devices"},"output":[{"type":"csv","metrics":["total_unique_devices"],"dims":["state"],"fileParameters":["id","dims"]}]},"bucket":"dev-data-store","key":"druid-reports/"}}}"""
        val response = reportApiServiceActorRef.underlyingActor.updateReport("district_week", request)
        response.params.status should be("failed")
    }

    "ReportAPIService" should "should not update and send error response with invalid request" in {
        val request = """{"id":"ekstep.analytics.report.request.submit","ver":"1.0","ts":"2020-03-30T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"reportId":"district_weekly","createdBy":"User1","description":"UniqueDevice district wise monthly","reportSchedule":"Weekly","config":{"reportConfig":{"id":"district_weekly","queryType":"groupBy","dateRange":{"staticInterval":"LastMonth","granularity":"all"},"metrics":[{"metric":"totalUniqueDevices","label":"Total Unique Devices","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastMonth","aggregations":[{"name":"total_unique_devices","type":"cardinality","fieldName":"context_did"}],"dimensions":[{"fieldName":"derived_loc_state","aliasName":"state"},{"fieldName":"derived_loc_district","aliasName":"district"}],"filters":[{"type":"in","dimension":"context_pdata_id","values":["__producerEnv__.diksha.portal","__producerEnv__.diksha.app"]},{"type":"isnotnull","dimension":"derived_loc_state"},{"type":"isnotnull","dimension":"derived_loc_district"}],"descending":"false"}}],"labels":{"state":"State","district":"District","total_unique_devices":"Number of Unique Devices"},"output":[{"type":"csv","metrics":["total_unique_devices"],"dims":["state"],"fileParameters":["id","dims"]}]},"bucket":"dev-data-store"}}}"""
        val response = reportApiServiceActorRef.underlyingActor.updateReport("district_weekly", request)
        response.responseCode should be("CLIENT_ERROR")
    }

    "ReportAPIService" should "return error response with all validation errors for report request" in {
        val request = """{"id":"ekstep.analytics.report.request.submit","ver":"1.0","ts":"2020-03-30T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{}}"""
        val response = reportApiServiceActorRef.underlyingActor.submitReport(request)
        response.result.get should be(Map("status" -> "failed", "request.reportId" -> "Report Id should not be  empty",
            "request.createdBy" -> "Created By should not be empty",
            "request.config" -> "Config should not be empty",
            "request.description" -> "Report Description should not empty"))
    }

    "ReportAPIService" should "deactivate the report with valid reportId" in {
        postgresUtil.readReport("district_weekly").get.status should be("ACTIVE")
        val response = reportApiServiceActorRef.underlyingActor.deactivateReport("district_weekly")
        response.params.status should be("successful")
        postgresUtil.readReport("district_weekly").get.status should be("INACTIVE")

    }

    "ReportAPIService" should "failed to deactivate the report with invalid reportId" in {
        val response = reportApiServiceActorRef.underlyingActor.deactivateReport("invalid_id")
        response.params.status should be("failed")
    }


    it should "validate the request body" in {
        var response = reportApiServiceActorRef.underlyingActor.submitReport("""{"id":"ekstep.analytics.report.request.submit","ver":"1.0","ts":"2020-03-30T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"}}""")
        response.responseCode should be("CLIENT_ERROR")
        response.result.get should be(Map("status" -> "failed", "request" -> "Request should not be empty"))

        response = reportApiServiceActorRef.underlyingActor.submitReport("""{"id":"ekstep.analytics.report.request.submit","ver":"1.0","ts":"2020-03-30T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"reportId":"district_weekly","createdBy":"User1","description":"UniqueDevice district wise monthly","reportSchedule":"Weekly","config":{}}}""")
        response.result.get should be(Map("status" -> "failed", "request.config.reportConfig" -> "Report Config  should not be empty"))

        response = reportApiServiceActorRef.underlyingActor.submitReport("""{"id":"ekstep.analytics.report.request.submit","ver":"1.0","ts":"2020-03-30T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"reportId":"district_weekly","createdBy":"User1","description":"UniqueDevice district wise monthly","reportSchedule":"Weekly","config":{"reportConfig":{"id":"district_weekly","queryType":"groupBy","dateRange":{"staticInterval":"LastMonth","granularity":"all"},"metrics":[{"metric":"totalUniqueDevices","label":"Total Unique Devices","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastMonth","aggregations":[{"name":"total_unique_devices","type":"cardinality","fieldName":"context_did"}],"dimensions":[{"fieldName":"derived_loc_state","aliasName":"state"},{"fieldName":"derived_loc_district","aliasName":"district"}],"filters":[{"type":"in","dimension":"context_pdata_id","values":["__producerEnv__.diksha.portal","__producerEnv__.diksha.app"]},{"type":"isnotnull","dimension":"derived_loc_state"},{"type":"isnotnull","dimension":"derived_loc_district"}],"descending":"false"}}],"labels":{"state":"State","district":"District","total_unique_devices":"Number of Unique Devices"},"output":[{"type":"csv","metrics":["total_unique_devices"],"dims":["state"],"fileParameters":["id","dims"]}]},"container":"__container__","key":"druid-reports/"}}}""")
        response.result.get should be(Map("status" -> "failed", "request.config.store" -> "Config Store should not be empty"))

        response = reportApiServiceActorRef.underlyingActor.submitReport("""{"id":"ekstep.analytics.report.request.submit","ver":"1.0","ts":"2020-03-30T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"reportId":"district_weekly","createdBy":"User1","description":"UniqueDevice district wise monthly","reportSchedule":"Weekly","config":{"reportConfig":{"id":"district_weekly","queryType":"groupBy","dateRange":{"staticInterval":"LastMonth","granularity":"all"},"metrics":[{"metric":"totalUniqueDevices","label":"Total Unique Devices","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastMonth","aggregations":[{"name":"total_unique_devices","type":"cardinality","fieldName":"context_did"}],"dimensions":[{"fieldName":"derived_loc_state","aliasName":"state"},{"fieldName":"derived_loc_district","aliasName":"district"}],"filters":[{"type":"in","dimension":"context_pdata_id","values":["__producerEnv__.diksha.portal","__producerEnv__.diksha.app"]},{"type":"isnotnull","dimension":"derived_loc_state"},{"type":"isnotnull","dimension":"derived_loc_district"}],"descending":"false"}}],"labels":{"state":"State","district":"District","total_unique_devices":"Number of Unique Devices"},"output":[{"type":"csv","metrics":["total_unique_devices"],"dims":["state"],"fileParameters":["id","dims"]}]},"store":"__store__","key":"druid-reports/"}}}""")
        response.result.get should be(Map("status" -> "failed", "request.config.container" -> "Config Container should not be empty"))

        response = reportApiServiceActorRef.underlyingActor.submitReport("""{"id":"ekstep.analytics.report.request.submit","ver":"1.0","ts":"2020-03-30T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"reportId":"district_weekly","createdBy":"User1","description":"UniqueDevice district wise monthly","reportSchedule":"Weekly","config":{"reportConfig":{"id":"district_weekly","queryType":"groupBy","dateRange":{"staticInterval":"LastMonth","granularity":"all"},"metrics":[{"metric":"totalUniqueDevices","label":"Total Unique Devices","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastMonth","aggregations":[{"name":"total_unique_devices","type":"cardinality","fieldName":"context_did"}],"dimensions":[{"fieldName":"derived_loc_state","aliasName":"state"},{"fieldName":"derived_loc_district","aliasName":"district"}],"filters":[{"type":"in","dimension":"context_pdata_id","values":["__producerEnv__.diksha.portal","__producerEnv__.diksha.app"]},{"type":"isnotnull","dimension":"derived_loc_state"},{"type":"isnotnull","dimension":"derived_loc_district"}],"descending":"false"}}],"labels":{"state":"State","district":"District","total_unique_devices":"Number of Unique Devices"},"output":[{"type":"csv","metrics":["total_unique_devices"],"dims":["state"],"fileParameters":["id","dims"]}]},"store":"__store__","container":"__container__"}}}""")
        response.result.get should be(Map("status" -> "failed", "request.config.key" -> "Config Key should not be empty"))


    }


    it should "test all exception branches" in {
        import akka.pattern.ask
        val request ="""{"id":"ekstep.analytics.report.request.submit","ver":"1.0","ts":"2020-03-30T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"reportId":"district_monthly","createdBy":"User1","description":"UniqueDevice district wise monthly","reportSchedule":"Monthly","config":{"reportConfig":{"id":"district_monthly","queryType":"groupBy","dateRange":{"staticInterval":"LastMonth","granularity":"all"},"metrics":[{"metric":"totalUniqueDevices","label":"Total Unique Devices","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastMonth","aggregations":[{"name":"total_unique_devices","type":"cardinality","fieldName":"context_did"}],"dimensions":[{"fieldName":"derived_loc_state","aliasName":"state"},{"fieldName":"derived_loc_district","aliasName":"district"}],"filters":[{"type":"in","dimension":"context_pdata_id","values":["__producerEnv__.diksha.portal","__producerEnv__.diksha.app"]},{"type":"isnotnull","dimension":"derived_loc_state"},{"type":"isnotnull","dimension":"derived_loc_district"}],"descending":"false"}}],"labels":{"state":"State","district":"District","total_unique_devices":"Number of Unique Devices"},"output":[{"type":"csv","metrics":["total_unique_devices"],"dims":["state"],"fileParameters":["id","dims"]}]},"store":"__store__","container":"__container__","key":"druid-reports/"}}}"""
        var result = Await.result((reportApiServiceActorRef ? SubmitReportRequest(request, config)).mapTo[Response], 20.seconds)
        result.responseCode should be("OK")

        result = Await.result((reportApiServiceActorRef ? GetReportListRequest("""{"request":{"filters":{"status":["ACTIVE","SUBMITTED"]}}}""", config)).mapTo[Response], 20.seconds)
        val resultMap = result.result.get
        val reportList = JSONUtils.deserialize[List[Response]](JSONUtils.serialize(resultMap.get("reports").get))
        reportList.length should be(1)

        result = Await.result((reportApiServiceActorRef ? UpdateReportRequest("district_monthly", request, config)).mapTo[Response], 20.seconds)
        result.responseCode should be("OK")

        result = Await.result((reportApiServiceActorRef ? GetReportRequest("district-weekly", config)).mapTo[Response], 20.seconds)
        result.params.status should be("failed")

        result = Await.result((reportApiServiceActorRef ? DeactivateReportRequest("district_weekly", config)).mapTo[Response], 20.seconds)
        result.responseCode should be("OK")
    }
}
