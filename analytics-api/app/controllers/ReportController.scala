package controllers

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import javax.inject.{Inject, Named}
import org.ekstep.analytics.api._
import org.ekstep.analytics.api.service.ReportAPIService.{SubmitReportRequest, _}
import org.ekstep.analytics.api.util.JSONUtils
import play.api.Configuration
import play.api.libs.json.Json
import play.api.mvc.{Request, _}

import scala.concurrent.ExecutionContext


class ReportController @Inject()(
                                  @Named("report-actor") reportActor: ActorRef,
                                  system: ActorSystem,
                                  configuration: Configuration,
                                  cc: ControllerComponents
                                )(implicit ec: ExecutionContext)
  extends BaseController(cc, configuration) {

    def submitReport() = Action.async { request: Request[AnyContent] =>
        val body: String = Json.stringify(request.body.asJson.get)
        val res = ask(reportActor, SubmitReportRequest(body, config)).mapTo[Response]
        res.map { x =>
            result(x.responseCode, JSONUtils.serialize(x))
        }
    }

    def getReport(reportId: String) = Action.async { request: Request[AnyContent] =>
        val res = ask(reportActor, GetReportRequest(reportId: String, config)).mapTo[Response]
        res.map { x =>
            result(x.responseCode, JSONUtils.serialize(x))
        }
    }

    def getReportList() = Action.async { request: Request[AnyContent] =>
        val res = ask(reportActor, "getReportList").mapTo[Response]
        res.map { x =>
            result(x.responseCode, JSONUtils.serialize(x))
        }
    }

    def updateReport(reportId: String) = Action.async { request: Request[AnyContent] =>
        val body: String = Json.stringify(request.body.asJson.get)
        val res = ask(reportActor, UpdateReportRequest(reportId,body,config)).mapTo[Response]
        res.map { x =>
            result(x.responseCode, JSONUtils.serialize(x))
        }
    }

    def deleteReport(reportId: String) = Action.async { request: Request[AnyContent] =>
        val res = ask(reportActor, DeleteReportRequest(reportId,config)).mapTo[Response]
        res.map { x =>
            result(x.responseCode, JSONUtils.serialize(x))
        }
    }


}
