package controllers

import akka.pattern.ask
import akka.actor.{ActorRef, ActorSystem}
import javax.inject.{Inject, Named}
import org.ekstep.analytics.api.Response
import org.ekstep.analytics.api.service.GroupAPIService.GetActivityAggregatesRequest
import org.ekstep.analytics.api.util.JSONUtils
import play.api.Configuration
import play.api.libs.json.Json
import play.api.mvc.{Request, _}

import scala.concurrent.ExecutionContext

class GroupController @Inject() (
                                  @Named("group-actor") groupActor: ActorRef,
                                  system: ActorSystem,
                                  configuration: Configuration,
                                  cc: ControllerComponents
                                ) (implicit ec: ExecutionContext) extends BaseController(cc, configuration) {

	def getActivityAggregates() = Action.async { request: Request[AnyContent] =>
		val body: String = Json.stringify(request.body.asJson.get)
		val res = ask(groupActor, GetActivityAggregatesRequest(body, config)).mapTo[Response]
		res.map { x => result(x.responseCode, JSONUtils.serialize(x))}
	}

}
