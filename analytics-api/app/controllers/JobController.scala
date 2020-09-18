package controllers

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.routing.FromConfig
import com.fasterxml.jackson.core.JsonProcessingException
import javax.inject.{Inject, Named}
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.api.service._
import org.ekstep.analytics.api.util._
import org.ekstep.analytics.api.{APIIds, ResponseCode, _}
import org.ekstep.analytics.framework.conf.AppConf
import play.api.Configuration
import play.api.libs.json.Json
import play.api.mvc.{Request, Result, _}

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}

/**
  * @author Amit Behera, mahesh
  */

class JobController @Inject() (
                                @Named("job-service-actor") jobAPIActor: ActorRef,
                                system: ActorSystem,
                                configuration: Configuration,
                                cc: ControllerComponents,
                                cacheUtil: CacheUtil,
                                restUtil: APIRestUtil
                              )(implicit ec: ExecutionContext) extends BaseController(cc, configuration) {

  def dataRequest() = Action.async { request: Request[AnyContent] =>
    val body: String = Json.stringify(request.body.asJson.get)
    val channelId = request.headers.get("X-Channel-ID").getOrElse("")
    val authorizedRoles = config.getStringList("ondemand.dataexhaust.roles").toList
    val checkFlag = if (config.getBoolean("dataexhaust.authorization_check")) authorizeDataExhaustRequest(request, authorizedRoles) else (true, None)
    if (checkFlag._1) {
       val res = ask(jobAPIActor, DataRequest(body, channelId, config)).mapTo[Response]
       res.map { x =>
         result(x.responseCode, JSONUtils.serialize(x))
       }
    } else {
       APILogger.log(checkFlag._2.get)
       errResponse(checkFlag._2.get, APIIds.DATA_REQUEST, ResponseCode.FORBIDDEN.toString)
    }
  }

  def getJob(tag: String, requestId: String) = Action.async { request: Request[AnyContent] =>

    val channelId = request.headers.get("X-Channel-ID").getOrElse("")
    val authorizedRoles = config.getStringList("ondemand.dataexhaust.roles").toList
    val checkFlag = if (config.getBoolean("dataexhaust.authorization_check")) authorizeDataExhaustRequest(request, authorizedRoles) else (true, None)
    if (checkFlag._1) {
       val appendedTag = tag + ":" + channelId
       val res = ask(jobAPIActor, GetDataRequest(appendedTag, requestId, config)).mapTo[Response]
       res.map { x =>
          result(x.responseCode, JSONUtils.serialize(x))
        }
    } else {
        APILogger.log(checkFlag._2.get)
        errResponse(checkFlag._2.get, APIIds.GET_DATA_REQUEST, ResponseCode.FORBIDDEN.toString)
    }
  }

  def getJobList(tag: String) = Action.async { request: Request[AnyContent] =>

    val channelId = request.headers.get("X-Channel-ID").getOrElse("")
    val authorizedRoles = config.getStringList("ondemand.dataexhaust.roles").toList
    val checkFlag = if (config.getBoolean("dataexhaust.authorization_check")) authorizeDataExhaustRequest(request, authorizedRoles) else (true, None)
    if (checkFlag._1) {
       val appendedTag = tag + ":" + channelId
       val limit = Integer.parseInt(request.getQueryString("limit").getOrElse(config.getString("data_exhaust.list.limit")))
       val res = ask(jobAPIActor, DataRequestList(appendedTag, limit, config)).mapTo[Response]
       res.map { x =>
          result(x.responseCode, JSONUtils.serialize(x))
       }
    }
    else {
       APILogger.log(checkFlag._2.get)
       errResponse(checkFlag._2.get, APIIds.GET_DATA_REQUEST_LIST, ResponseCode.FORBIDDEN.toString)
    }
  }

  def getTelemetry(datasetId: String) = Action.async { request: Request[AnyContent] =>

    val since = request.getQueryString("since").getOrElse("")
    val from = request.getQueryString("from").getOrElse("")
    val to = request.getQueryString("to").getOrElse("")

    val channelId = request.headers.get("X-Channel-ID").getOrElse("")
    val consumerId = request.headers.get("X-Consumer-ID").getOrElse("")
    val authorizedRoles = config.getStringList("standard.dataexhaust.roles").toList
    val checkFlag = if (config.getBoolean("dataexhaust.authorization_check")) authorizeDataExhaustRequest(request, authorizedRoles, true) else (true, None)
    if (checkFlag._1) {
      APILogger.log(s"Authorization Successfull for X-Consumer-ID='$consumerId' and X-Channel-ID='$channelId'")
      val res = ask(jobAPIActor, ChannelData(channelId, datasetId, from, to, since, config)).mapTo[Response]
      res.map { x =>
        result(x.responseCode, JSONUtils.serialize(x))
      }
    } else {
        APILogger.log(checkFlag._2.get)
        errResponse(checkFlag._2.get, APIIds.CHANNEL_TELEMETRY_EXHAUST, ResponseCode.FORBIDDEN.toString)
    }
  }

  private def errResponse(msg: String, apiId: String, responseCode: String): Future[Result] = {
     val res = CommonUtil.errorResponse(apiId, msg, responseCode)
     Future {
        result(res.responseCode, JSONUtils.serialize(res))
     }
  }

  def refreshCache(cacheType: String) = Action { implicit request =>
      cacheType match {
          case "ConsumerChannel" =>
            cacheUtil.initConsumerChannelCache()
          case "DeviceLocation" =>
            cacheUtil.initDeviceLocationCache()
      }
      result("OK", JSONUtils.serialize(CommonUtil.OK(APIIds.CHANNEL_TELEMETRY_EXHAUST, Map("msg" -> s"$cacheType cache refreshed successfully"))))
  }

  def authorizeDataExhaustRequest(request: Request[AnyContent], authorizedRoles: List[String], superAdminRulesCheck: Boolean = false): (Boolean, Option[String]) = {

    // security enhancements logic
    val channelId = request.headers.get("X-Channel-ID").getOrElse("")
    val consumerId = request.headers.get("X-Consumer-ID").getOrElse("")
    val userAuthToken = request.headers.get("x-authenticated-user-token")
    val userId = request.headers.get("X-Authenticated-Userid").getOrElse("")
    val authBearerToken = request.headers.get("Authorization")
    val userApiUrl = config.getString("user.profile.url")
    if (channelId.nonEmpty) {
        if(userAuthToken.isEmpty) {
            APILogger.log(s"Authorizing $consumerId and $channelId")
            val status = Option(cacheUtil.getConsumerChannelTable().get(consumerId, channelId))
            if (status.getOrElse(0) == 1) (true, None) else (false, Option(s"Given X-Consumer-ID='$consumerId' and X-Channel-ID='$channelId' are not authorized"))
        }
        else {
            var unauthorizedErrMsg = "You are not authorized."
            val headers = Map("x-authenticated-user-token" -> userAuthToken.get)
            val userReadResponse = restUtil.get[Response](userApiUrl + userId, Option(headers))
            APILogger.log("user read response: " + JSONUtils.serialize(userReadResponse))
            if(userReadResponse.responseCode.equalsIgnoreCase("ok")) {
                val userResponse = userReadResponse.result.getOrElse(Map()).getOrElse("response", Map()).asInstanceOf[Map[String, AnyRef]]
                val orgDetails = userResponse.getOrElse("rootOrg", Map()).asInstanceOf[Map[String, AnyRef]]
                val userRoles = userResponse.getOrElse("organisations", List()).asInstanceOf[List[Map[String, AnyRef]]]
                  .map(f => f.getOrElse("roles", List()).asInstanceOf[List[String]]).flatMap(f => f)
                if (userRoles.filter(f => authorizedRoles.contains(f)).size > 0) {
                    if (superAdminRulesCheck) {
                        val userSlug = orgDetails.getOrElse("slug", "").asInstanceOf[String]
                        APILogger.log("header channel: " + channelId + " org slug: " + userSlug)
                        if (channelId.equalsIgnoreCase(userSlug)) return (true, None)
                        else {
                            // get MHRD tenant value from cache
                            val mhrdChannel = cacheUtil.getSuperAdminChannel()
                            val userChannel = orgDetails.getOrElse("channel", "").asInstanceOf[String]
                            APILogger.log("user channel: " + userChannel + " mhrd id: " + mhrdChannel)
                            if (userChannel.equalsIgnoreCase(mhrdChannel)) return (true, None)
                        }
                    }
                    else {
                        val userOrgId = orgDetails.getOrElse("id", "").asInstanceOf[String]
                        APILogger.log("header channel: " + channelId + " org id: " + userOrgId)
                        if (channelId.equalsIgnoreCase(userOrgId)) return (true, None)
                    }
                }
            }
            else { unauthorizedErrMsg = userReadResponse.params.errmsg }
            (false, Option(unauthorizedErrMsg))
        }
    }
    else (false, Option("X-Channel-ID is missing in request header"))
  }
}