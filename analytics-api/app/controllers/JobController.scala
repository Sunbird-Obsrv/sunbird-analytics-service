package controllers

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.routing.FromConfig
import javax.inject.{Inject, Named}
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
    /*
    Case 1:
        - If user-token is null, check with X-Channel-Id and consumer-token(consumer-channel mapping)
        - Process the request if X-Channel-Id matches the channel retrieved from the consumer-channel mapping table
    Case 2:
        - If user-token is not null and valid, check with X-Channel-Id, user-id and user profile
        - Retrieve the user role and channel info from the user profile API
        - X-Channel-Id should match the user Channel and user role should be either ORG_ADMIN or REPORT_ADMIN
    Case 3:
        - If user-token is not null and valid, check with X-Channel-Id, user-id and user profile
        - Retrieve the user role and channel info from the user profile API
        - User channel should match “MHRD” tenant and user role should be either ORG_ADMIN or REPORT_ADMIN
     */
    val channelId = request.headers.get("X-Channel-ID").getOrElse("")
    val consumerId = request.headers.get("X-Consumer-ID").getOrElse("")
    val userId = request.headers.get("X-User-ID").getOrElse("")
    val userAuthToken = request.headers.get("x-authenticated-user-token")
    val apiUrl = config.getString("user.profile.url")
    if (channelId.nonEmpty) {
        if(userAuthToken.isEmpty) {
            APILogger.log(s"Authorizing $consumerId and $channelId")
            val whitelistedConsumers = config.getStringList("channel.data_exhaust.whitelisted.consumers")
            // if consumerId is present in whitelisted consumers, skip auth check
            if (consumerId.nonEmpty && whitelistedConsumers.contains(consumerId)) (true, None)
            else {
                val status = Option(cacheUtil.getConsumerChannelTable().get(consumerId, channelId))
                if (status.getOrElse(0) == 1) (true, None) else (false, Option(s"Given X-Consumer-ID='$consumerId' and X-Channel-ID='$channelId' are not authorized"))
            }
        }
        else {
            val headers = Map("x-authenticated-user-token" -> userAuthToken.get)
            val userData = restUtil.get[Map[String, AnyRef]](apiUrl + userId, Option(headers))
            val userResponse = userData.getOrElse("result", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("response", Map()).asInstanceOf[Map[String, AnyRef]]
            val userChannel = userResponse.getOrElse("channel", "").asInstanceOf[String]
            val userRoles = userResponse.getOrElse("organisations", List()).asInstanceOf[List[Map[String, AnyRef]]]
                  .map(f => f.getOrElse("roles", List()).asInstanceOf[List[String]]).flatMap(f => f)
            if (userRoles.filter(f => authorizedRoles.contains(f)).size > 0) {
                if (superAdminRulesCheck) {
                    // get MHRD tenant value using org search API
                    val orgSearchApiUrl = config.getString("org.search.url")
                    val requestBody = """{"request":{"filters":{"channel":"mhrd"},"offset":0,"limit":1000,"fields":["id"]}}"""
                    val response = restUtil.post[Map[String, AnyRef]](orgSearchApiUrl, requestBody)
                    val mhrdChannel = response.getOrElse("result", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("response", Map()).asInstanceOf[Map[String, AnyRef]]
                      .getOrElse("content", List(Map())).asInstanceOf[List[Map[String, AnyRef]]].head.getOrElse("id", "").asInstanceOf[String]
                    if (userChannel.equalsIgnoreCase(mhrdChannel)) (true, None)
                    else (false, Option("User without super admin access is not authorized"))
                }
                else {
                    if (channelId.equalsIgnoreCase(userChannel)) (true, None)
                    else (false, Option("User with incorrect channel is not authorized"))
                }
            }
            else (false, Option("User without admin role is not authorized"))
        }
    }
    else (false, Option("X-Channel-ID is missing in request header"))
  }
}