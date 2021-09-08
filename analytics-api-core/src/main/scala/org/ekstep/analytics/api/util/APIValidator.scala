package org.ekstep.analytics.api.util

import com.typesafe.config.Config
import org.ekstep.analytics.api.{RequestBody, RequestHeaderData, Response}
import javax.inject.Inject
import javax.inject.Singleton
import scala.collection.JavaConversions._

@Singleton
class APIValidator @Inject()(postgresDBUtil: PostgresDBUtil, restUtil: APIRestUtil, cacheUtil: CacheUtil) {

  implicit val className = "org.ekstep.analytics.api.util.APIValidator"

  def validateSubmitReq(body: RequestBody, datasetSubId: String)(implicit config: Config): Map[String, String] = {
    val datasetdetails = postgresDBUtil.getDatasetBySubId(datasetSubId)
    if (datasetdetails.isEmpty) {
      if (body.request.tag.isEmpty) {
        Map("status" -> "false", "message" -> "tag is empty")
      } else if (body.request.dataset.isEmpty) {
        Map("status" -> "false", "message" -> "dataset is empty")
      } else if (body.request.datasetConfig.isEmpty) {
        Map("status" -> "false", "message" -> "datasetConfig is empty")
      } else {
        Map("status" -> "true")
      }
    } else {
      // To:Do - Validate using json
      //val validationJson = datasetdetails.get.validation_json
      Map("status" -> "true")
    }
  }

  def authorizeDataExhaustRequest(requestHeaderData: RequestHeaderData, datasetSubId: String, superAdminRulesCheck: Boolean = false)(implicit config: Config): (Boolean, Option[String]) = {

    val datasetdetails = postgresDBUtil.getDatasetBySubId(datasetSubId)
    val authorizedRoles = if (datasetdetails.isEmpty) {
      config.getStringList("ondemand.dataexhaust.roles").toList
    } else {
      datasetdetails.get.authorized_roles
    }
    // security enhancements logic
    val channelId = requestHeaderData.channelId
    val consumerId = requestHeaderData.consumerId
    val userId = requestHeaderData.userId
    val userAuthToken = requestHeaderData.userAuthToken
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
