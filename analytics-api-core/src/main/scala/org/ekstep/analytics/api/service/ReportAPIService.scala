package org.ekstep.analytics.api.service

import akka.actor.Actor
import com.typesafe.config.Config
import org.ekstep.analytics.api.util.{CommonUtil, JSONUtils}
import org.ekstep.analytics.api.{APIIds, ReportResponse, Response}

object  ReportAPIService {
  case class SubmitReportRequest(request: String, config: Config)
  case class GetReportRequest(reportId: String, config: Config)
  case class UpdateReportRequest(reportId:String,request: String, config: Config)
  case class DeleteReportRequest(reportId:String,config: Config)

  def submitReport(request  : String)(implicit  config: Config) ={


  }

  def deleteReport(request  : String)(implicit  config: Config) ={

  }

  def updateReport(reportId: String,request  : String)(implicit  config: Config) ={

  }


  def getReport(reportId : String)(implicit  commonUtil: Config) : Response = {

    val config  ="{\"reportConfig\":{\"id\":\"district_monthly\",\"queryType\":\"groupBy\",\"dateRange\":{\"staticInterval\":\"LastMonth\",\"granularity\":\"all\"},\"metrics\":[{\"metric\":\"totalUniqueDevices\",\"label\":\"Total Unique Devices\",\"druidQuery\":{\"queryType\":\"groupBy\",\"dataSource\":\"telemetry-events\",\"intervals\":\"LastMonth\",\"aggregations\":[{\"name\":\"total_unique_devices\",\"type\":\"cardinality\",\"fieldName\":\"context_did\"}],\"dimensions\":[{\"fieldName\":\"derived_loc_state\",\"aliasName\":\"state\"},{\"fieldName\":\"derived_loc_district\",\"aliasName\":\"district\"}],\"filters\":[{\"type\":\"in\",\"dimension\":\"context_pdata_id\",\"values\":[\"__producerEnv__.diksha.portal\",\"__producerEnv__.diksha.app\"]},{\"type\":\"isnotnull\",\"dimension\":\"derived_loc_state\"},{\"type\":\"isnotnull\",\"dimension\":\"derived_loc_district\"}],\"descending\":\"false\"}}],\"labels\":{\"state\":\"State\",\"district\":\"District\",\"total_unique_devices\":\"Number of Unique Devices\"},\"output\":[{\"type\":\"csv\",\"metrics\":[\"total_unique_devices\"],\"dims\":[\"state\"],\"fileParameters\":[\"id\",\"dims\"]}]},\"bucket\":\"'$bucket'\",\"key\":\"druid-reports/\"}";
    val response =CommonUtil.OK(APIIds.REPORT_GET_REQUEST, CommonUtil.caseClassToMap(ReportResponse("district_monthly", "UniqueDevice district wise monthly", "sunbird" ,"Monthly"
      , JSONUtils.deserialize[Map[String,Any]](config) , 1585623738000L, 1585623738000L, 1585623738000L, "SUBMITTED", "Report Sucessfully Submitted")))
    response

  }

  def getReportList() : Response ={

    val config  = "{\"reportConfig\":{\"id\":\"district_monthly\",\"queryType\":\"groupBy\",\"dateRange\":{\"staticInterval\":\"LastMonth\",\"granularity\":\"all\"},\"metrics\":[{\"metric\":\"totalUniqueDevices\",\"label\":\"Total Unique Devices\",\"druidQuery\":{\"queryType\":\"groupBy\",\"dataSource\":\"telemetry-events\",\"intervals\":\"LastMonth\",\"aggregations\":[{\"name\":\"total_unique_devices\",\"type\":\"cardinality\",\"fieldName\":\"context_did\"}],\"dimensions\":[{\"fieldName\":\"derived_loc_state\",\"aliasName\":\"state\"},{\"fieldName\":\"derived_loc_district\",\"aliasName\":\"district\"}],\"filters\":[{\"type\":\"in\",\"dimension\":\"context_pdata_id\",\"values\":[\"__producerEnv__.diksha.portal\",\"__producerEnv__.diksha.app\"]},{\"type\":\"isnotnull\",\"dimension\":\"derived_loc_state\"},{\"type\":\"isnotnull\",\"dimension\":\"derived_loc_district\"}],\"descending\":\"false\"}}],\"labels\":{\"state\":\"State\",\"district\":\"District\",\"total_unique_devices\":\"Number of Unique Devices\"},\"output\":[{\"type\":\"csv\",\"metrics\":[\"total_unique_devices\"],\"dims\":[\"state\"],\"fileParameters\":[\"id\",\"dims\"]}]},\"bucket\":\"'$bucket'\",\"key\":\"druid-reports/\"}";
    val config1 = "{\"reportConfig\":{\"id\":\"Desktop-Consumption-Daily-Reports\",\"queryType\":\"groupBy\",\"dateRange\":{\"staticInterval\":\"LastDay\",\"granularity\":\"day\"},\"metrics\":[{\"metric\":\"totalContentDownloadDesktop\",\"label\":\"Total Content Download\",\"druidQuery\":{\"queryType\":\"groupBy\",\"dataSource\":\"telemetry-events\",\"intervals\":\"LastDay\",\"granularity\":\"all\",\"aggregations\":[{\"name\":\"total_content_download_on_desktop\",\"type\":\"count\",\"fieldName\":\"mid\"}],\"dimensions\":[{\"fieldName\":\"content_board\",\"aliasName\":\"state\"}],\"filters\":[{\"type\":\"equals\",\"dimension\":\"context_env\",\"value\":\"downloadManager\"},{\"type\":\"equals\",\"dimension\":\"edata_state\",\"value\":\"COMPLETED\"},{\"type\":\"equals\",\"dimension\":\"context_pdata_id\",\"value\":\"'$producer_env'.diksha.desktop\"},{\"type\":\"equals\",\"dimension\":\"eid\",\"value\":\"AUDIT\"}],\"descending\":\"false\"}},{\"metric\":\"totalContentPlayedDesktop\",\"label\":\"Total time spent in hours\",\"druidQuery\":{\"queryType\":\"groupBy\",\"dataSource\":\"summary-events\",\"intervals\":\"LastDay\",\"granularity\":\"all\",\"aggregations\":[{\"name\":\"total_content_plays_on_desktop\",\"type\":\"count\",\"fieldName\":\"mid\"}],\"dimensions\":[{\"fieldName\":\"collection_board\",\"aliasName\":\"state\"}],\"filters\":[{\"type\":\"equals\",\"dimension\":\"eid\",\"value\":\"ME_WORKFLOW_SUMMARY\"},{\"type\":\"equals\",\"dimension\":\"dimensions_mode\",\"value\":\"play\"},{\"type\":\"equals\",\"dimension\":\"dimensions_type\",\"value\":\"content\"},{\"type\":\"equals\",\"dimension\":\"dimensions_pdata_id\",\"value\":\"'$producer_env'.diksha.desktop\"}],\"descending\":\"false\"}},{\"metric\":\"totalContentPlayedInHourOnDesktop\",\"label\":\"Total Content Download\",\"druidQuery\":{\"queryType\":\"groupBy\",\"dataSource\":\"summary-events\",\"intervals\":\"LastDay\",\"granularity\":\"all\",\"aggregations\":[{\"name\":\"sum__edata_time_spent\",\"type\":\"doubleSum\",\"fieldName\":\"edata_time_spent\"}],\"dimensions\":[{\"fieldName\":\"collection_board\",\"aliasName\":\"state\"}],\"filters\":[{\"type\":\"equals\",\"dimension\":\"eid\",\"value\":\"ME_WORKFLOW_SUMMARY\"},{\"type\":\"equals\",\"dimension\":\"dimensions_mode\",\"value\":\"play\"},{\"type\":\"equals\",\"dimension\":\"dimensions_type\",\"value\":\"content\"},{\"type\":\"equals\",\"dimension\":\"dimensions_pdata_id\",\"value\":\"'$producer_env'.diksha.desktop\"}],\"postAggregation\":[{\"type\":\"arithmetic\",\"name\":\"total_time_spent_in_hours_on_desktop\",\"fields\":{\"leftField\":\"sum__edata_time_spent\",\"rightField\":3600,\"rightFieldType\":\"constant\"},\"fn\":\"/\"}],\"descending\":\"false\"}},{\"metric\":\"totalUniqueDevicesPlayedContentOnDesktop\",\"label\":\"Total Unique Devices On Desktop that played content\",\"druidQuery\":{\"queryType\":\"groupBy\",\"dataSource\":\"summary-events\",\"intervals\":\"LastDay\",\"granularity\":\"all\",\"aggregations\":[{\"name\":\"total_unique_devices_on_desktop_played_content\",\"type\":\"cardinality\",\"fieldName\":\"dimensions_did\"}],\"dimensions\":[{\"fieldName\":\"collection_board\",\"aliasName\":\"state\"}],\"filters\":[{\"type\":\"equals\",\"dimension\":\"eid\",\"value\":\"ME_WORKFLOW_SUMMARY\"},{\"type\":\"equals\",\"dimension\":\"dimensions_mode\",\"value\":\"play\"},{\"type\":\"equals\",\"dimension\":\"dimensions_type\",\"value\":\"content\"},{\"type\":\"equals\",\"dimension\":\"dimensions_pdata_id\",\"value\":\"'$producer_env'.diksha.desktop\"}],\"descending\":\"false\"}}],\"labels\":{\"state\":\"State\",\"total_content_plays_on_desktop\":\"Total Content Played\",\"total_content_download_on_desktop\":\"Total Content Downloads\",\"total_time_spent_in_hours_on_desktop\":\"Total time spent in hours\",\"total_unique_devices_on_desktop_played_content\":\"Total Unique Devices On Desktop that played content\"},\"output\":[{\"type\":\"csv\",\"label\":\"desktop\",\"metrics\":[\"total_content_download_on_desktop\",\"total_time_spent_in_hours_on_desktop\",\"total_content_plays_on_desktop\",\"total_unique_devices_on_desktop_played_content\"],\"dims\":[\"state\"],\"fileParameters\":[\"dims\"]}]},\"bucket\":\"dev-data-store\",\"key\":\"druid-reports/\"}";
    val response =CommonUtil.OK(APIIds.REPORT_GET_REQUEST, CommonUtil.caseClassToMap(List(ReportResponse("district_monthly", "UniqueDevice district wise monthly", "sunbird" ,"Monthly",
      JSONUtils.deserialize[Map[String,Any]](config), 1585623738000L, 1585623738000L, 1585623738000L,
      "SUBMITTED", "Report Sucessfully Submitted"),ReportResponse("district_weekly", "UniqueDevice district wise weekly",
      "sunbird" ,"Monthly", JSONUtils.deserialize[Map[String,Any]](config1),
      1585623738000L, 1585623738000L, 1585623738000L, "ACTIVE", "REPORT ACTIVE"))))
    response
  }
}

class ReportAPIService extends Actor {

  import ReportAPIService._

  def receive = {
    case SubmitReportRequest(request: String, config: Config) => sender() ! submitReport(request)(config)
    case GetReportRequest(reportId: String, config: Config) => sender() ! getReport(reportId)(config)
    case "getReportList" => sender() ! getReportList()
    case DeleteReportRequest(reportId: String, config: Config) => sender() ! getReport(reportId)(config)
    case UpdateReportRequest(reportId: String, request:String ,config: Config) => sender() ! getReport(reportId)(config)

  }
}
