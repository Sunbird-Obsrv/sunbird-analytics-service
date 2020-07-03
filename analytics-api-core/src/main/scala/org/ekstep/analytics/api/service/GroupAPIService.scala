package org.ekstep.analytics.api.service

import akka.actor.Actor
import com.typesafe.config.Config
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.api.{APIIds, GroupApiRequest, GroupRequestBody, Response, ResponseCode}
import org.ekstep.analytics.api.service.GroupAPIService.GetActivityAggregatesRequest
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils


class GroupAPIService extends Actor {
	override def receive: Receive = {
		case GetActivityAggregatesRequest(request: String, config: Config) => sender() ! GroupAPIService.getActivityAggregates(request)(config)
	}
}

object GroupAPIService {

	implicit val className: String = "org.ekstep.analytics.api.service.GroupAPIService"

	case class GetActivityAggregatesRequest(request: String, config: Config)

	def getActivityAggregates(request: String)(implicit config: Config): Response = {
		val body = JSONUtils.deserialize[GroupRequestBody](request)
		val req: GroupApiRequest = body.request
		val isValidRequest = validateRequest(req)
		if (isValidRequest) {
			val result = getMockResponse(req)
			CommonUtil.OK(APIIds.GROUP_GET_ACTIVITY_AGGREGATES_REQUEST, result)
		} else {
			CommonUtil.errorResponse(APIIds.GROUP_GET_ACTIVITY_AGGREGATES_REQUEST, "INVALID_ACTIVITY_AGGREGATES_REQUEST", ResponseCode.CLIENT_ERROR.toString)
		}
	}

	private def validateRequest(request: GroupApiRequest): Boolean = {
		if ((null != request && StringUtils.isNotBlank(request.groupId)) && (StringUtils.isNotBlank(request.activityId) && StringUtils.isNoneBlank(request.activityType))) true else false
	}

	private def getMockResponse(request: GroupApiRequest): Map[String, AnyRef] = {
		val activityMap = Map[String, AnyRef]("id" -> "do_1126981011606323201176", "type" -> "Course", "metadata" -> Map[String, AnyRef](), "agg" -> List(Map[String, AnyRef]("metric" -> "progress", "value" -> 60.asInstanceOf[AnyRef], "lastUpdatedOn" -> "2020-07-02T19:03:54.035+0530")), "members" -> List(Map[String, AnyRef]("id" -> "8454cb21-3ce9-4e30-85b5-fade097880d8", "agg" -> List(Map[String, AnyRef]("metric" -> "progress", "value" -> 50.asInstanceOf[AnyRef], "lastUpdatedOn" -> "2020-07-02T19:03:54.035+0530")))))
		Map[String, AnyRef]("groupId" -> request.groupId, "activity" -> activityMap)
	}

}
