package org.ekstep.analytics.api.service

import akka.actor.Actor
import com.typesafe.config.Config
import javax.inject.Inject
import org.ekstep.analytics.api._
import org.ekstep.analytics.api.util.{CommonUtil, JSONUtils, PostgresDBUtil}

case class SubmitReportRequest(request: String, config: Config)

case class GetReportRequest(reportId: String, config: Config)

case class UpdateReportRequest(reportId: String, request: String, config: Config)

case class DeactivateReportRequest(reportId: String, config: Config)

case class GetReportListRequest(request: String, config: Config)


class ReportAPIService @Inject()(postgresDBUtil: PostgresDBUtil) extends Actor {

    def receive: PartialFunction[Any, Unit] = {
        case SubmitReportRequest(request: String, config: Config) => sender() ! submitReport(request)(config)
        case GetReportRequest(reportId: String, config: Config) => sender() ! getReport(reportId)(config)
        case GetReportListRequest(request: String, config: Config) => sender() ! getReportList(request)(config)
        case DeactivateReportRequest(reportId: String, config: Config) => sender() ! deactivateReport(reportId)(config)
        case UpdateReportRequest(reportId: String, request: String, config: Config) => sender() ! updateReport(reportId, request)(config)

    }

    def submitReport(request: String)(implicit config: Config): Response = {
        val body = JSONUtils.deserialize[ReportRequestBody](request)
        val reportRequest = body.request
        val isValidRequest = validateRequest(reportRequest)
        if ("success".equals(isValidRequest.getOrElse("status", ""))) {
            val report = postgresDBUtil.readReport(reportRequest.reportId)
            report.map { _ =>
                CommonUtil.errorResponse(APIIds.REPORT_SUBMIT_REQUEST, "ReportId already Exists", ResponseCode.OK.toString)
            }.getOrElse({
                postgresDBUtil.saveReportConfig(reportRequest)
                val response = CommonUtil.caseClassToMap(body.request)
                CommonUtil.OK(APIIds.REPORT_SUBMIT_REQUEST, response)
            })
        }
        else {
            CommonUtil.reportErrorResponse(APIIds.REPORT_SUBMIT_REQUEST, isValidRequest, ResponseCode.CLIENT_ERROR.toString)
        }
    }

    def deactivateReport(reportId: String)(implicit config: Config): Response = {
        val report = postgresDBUtil.readReport(reportId)
        report.map { _ =>
            postgresDBUtil.deactivateReport(reportId)
            CommonUtil.OK(APIIds.REPORT_DELETE_REQUEST, Map("result" -> "Successfully DeActivated the Report"))
        }.getOrElse({
            CommonUtil.errorResponse(APIIds.REPORT_DELETE_REQUEST, "no report available with requested reportId", ResponseCode.OK.toString)
        })
    }

    def updateReport(reportId: String, request: String)(implicit config: Config): Response = {
        val body = JSONUtils.deserialize[ReportRequestBody](request)
        val reportRequest = body.request
        val report = postgresDBUtil.readReport(reportId)
        report.map { value =>
            val isValidRequest = validateRequest(reportRequest)
            if ("success".equals(isValidRequest.getOrElse("status", ""))) {
                postgresDBUtil.updateReportConfig(value.reportId, reportRequest)
                CommonUtil.OK(APIIds.REPORT_UPDATE_REQUEST, CommonUtil.caseClassToMap(reportRequest))
            } else {
                CommonUtil.reportErrorResponse(APIIds.REPORT_UPDATE_REQUEST, isValidRequest, ResponseCode.CLIENT_ERROR.toString)
            }
        }.getOrElse({
            CommonUtil.errorResponse(APIIds.REPORT_UPDATE_REQUEST, "no report available with requested reportid", ResponseCode.OK.toString)
        })
    }


    def getReport(reportId: String)(implicit config: Config): Response = {

        val report = postgresDBUtil.readReport(reportId)
        report.map { value =>
            CommonUtil.OK(APIIds.REPORT_GET_REQUEST, CommonUtil.caseClassToMap(value))
        }.getOrElse({
            CommonUtil.errorResponse(APIIds.REPORT_GET_REQUEST, "no report available with requested reportid", ResponseCode.OK.toString)
        })
    }

    def getReportList(request: String)(implicit config: Config): Response = {

        val body = JSONUtils.deserialize[ReportFilter](request)
        val reportList = postgresDBUtil.readReportList(body.request.filters("status"))
        if (reportList.nonEmpty) {
            val response = reportList.map { report =>
                CommonUtil.caseClassToMap(report)
            }
            CommonUtil.OK(APIIds.REPORT_GET_REQUEST, Map("count"-> response.size.asInstanceOf[AnyRef] , "reports" -> response))
        }
        else {
            CommonUtil.errorResponse(APIIds.REPORT_GET_REQUEST, "no report available with requested filters", ResponseCode.OK.toString)
        }
    }


    private def validateRequest(request: ReportRequest)(implicit config: Config): Map[String, String] = {
        val errMap = scala.collection.mutable.Map[String, String]()
        if (null == request) {
            errMap("request") = "Request should not be empty"
        } else {
            if (Option(request.reportId).isEmpty) {
                errMap("request.reportId") = "Report Id should not be  empty"
            }
            if (Option(request.description).isEmpty) {
                errMap("request.description") = "Report Description should not empty"
            }
            if (Option(request.createdBy).isEmpty) {
                errMap("request.createdBy") = "Created By should not be empty"
            }

            if (Option(request.config).isEmpty) {
                errMap("request.config") = "Config should not be empty"
            } else if (request.config.get("reportConfig").isEmpty) {
                errMap("request.config.reportConfig") = "Report Config  should not be empty"
            } else if (request.config.get("store").isEmpty) {
                errMap("request.config.store") = "Config Store should not be empty"
            } else if (request.config.get("container").isEmpty) {
                errMap("request.config.container") = "Config Container should not be empty"
            } else if (request.config.get("key").isEmpty) {
                errMap("request.config.key") = "Config Key should not be empty"
            }
        }
        if (errMap.nonEmpty) errMap += ("status" -> "failed") else errMap += ("status" -> "success")
        errMap.toMap
    }

}
