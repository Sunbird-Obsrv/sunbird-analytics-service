package org.ekstep.analytics.api.service

import java.security.MessageDigest
import java.util.Calendar

import akka.actor.Actor
import com.typesafe.config.Config
import javax.inject.{Inject, Singleton}
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.api.util.JobRequest
import org.ekstep.analytics.api.util._
import org.ekstep.analytics.api.{APIIds, JobConfig, JobStats, OutputFormat, _}
import org.ekstep.analytics.framework.util.{HTTPClient, JSONUtils, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, JobStatus}
import org.joda.time.DateTime
import org.sunbird.cloud.storage.conf.AppConf

import scala.collection.mutable.Buffer
import scala.util.Sorting

/**
  * @author mahesh
  */


case class DataRequest(request: String, channel: String, config: Config)

case class GetDataRequest(tag: String, requestId: String, config: Config)

case class DataRequestList(tag: String, limit: Int, config: Config)

case class ChannelData(channel: String, eventType: String, from: String, to: String, since: String, config: Config)

class JobAPIService @Inject()(postgresDBUtil: PostgresDBUtil) extends Actor  {

  implicit val fc = new FrameworkContext();

  def receive = {
    case DataRequest(request: String, channelId: String, config: Config) => sender() ! dataRequest(request, channelId)(config, fc)
    case GetDataRequest(tag: String, requestId: String, config: Config) => sender() ! getDataRequest(tag, requestId)(config, fc)
    case DataRequestList(tag: String, limit: Int, config: Config) => sender() ! getDataRequestList(tag, limit)(config, fc)
    case ChannelData(channel: String, eventType: String, from: String, to: String, since: String, config: Config) => sender() ! getChannelData(channel, eventType, from, to, since)(config, fc)
  }

  implicit val className = "org.ekstep.analytics.api.service.JobAPIService"


  val storageType = AppConf.getStorageType()

  def dataRequest(request: String, channel: String)(implicit config: Config, fc: FrameworkContext): Response = {
    val body = JSONUtils.deserialize[RequestBody](request)
    val isValid = _validateReq(body)
    if ("true".equals(isValid.get("status").get)) {
      val job = upsertRequest(body, channel)
      val response = CommonUtil.caseClassToMap(_createJobResponse(job))
      CommonUtil.OK(APIIds.DATA_REQUEST, response)
    } else {
      CommonUtil.errorResponse(APIIds.DATA_REQUEST, isValid.get("message").get, ResponseCode.CLIENT_ERROR.toString)
    }
  }

  def getDataRequest(tag: String, requestId: String)(implicit config: Config, fc: FrameworkContext): Response = {
    val job = postgresDBUtil.getJobRequest(requestId, tag)
    if (job.isEmpty) {
      CommonUtil.errorResponse(APIIds.GET_DATA_REQUEST, "no job available with the given request_id and tag", ResponseCode.OK.toString)
    } else {
      val jobStatusRes = _createJobResponse(job.get)
      CommonUtil.OK(APIIds.GET_DATA_REQUEST, CommonUtil.caseClassToMap(jobStatusRes))
    }
  }

  def getDataRequestList(tag: String, limit: Int)(implicit config: Config, fc: FrameworkContext): Response = {
    val currDate = DateTime.now()
    val jobRequests = postgresDBUtil.getJobRequestList(tag, limit)
    val result = jobRequests.map { x => _createJobResponse(x) }
    CommonUtil.OK(APIIds.GET_DATA_REQUEST_LIST, Map("count" -> Int.box(jobRequests.size), "jobs" -> result))
  }

  def getChannelData(channel: String, datasetId: String, from: String, to: String, since: String = "")(implicit config: Config, fc: FrameworkContext): Response = {

    val fromDate = if (since.nonEmpty) since else if (from.nonEmpty) from else CommonUtil.getPreviousDay()
    val toDate = if (to.nonEmpty) to else CommonUtil.getToday()

    val isValid = _validateRequest(channel, datasetId, fromDate, toDate)
    if ("true".equalsIgnoreCase(isValid.getOrElse("status", "false"))) {
      val expiry = config.getInt("channel.data_exhaust.expiryMins")
      val loadConfig = config.getObject(s"channel.data_exhaust.dataset").unwrapped()
      val datasetConfig = if (null != loadConfig.get(datasetId)) loadConfig.get(datasetId).asInstanceOf[java.util.Map[String, AnyRef]] else loadConfig.get("default").asInstanceOf[java.util.Map[String, AnyRef]]
      val bucket = datasetConfig.get("bucket").toString
      val basePrefix = datasetConfig.get("basePrefix").toString
      val prefix = basePrefix + datasetId + "/" + channel + "/"
      APILogger.log("prefix: " + prefix)

      val storageKey = config.getString("storage.key.config")
      val storageSecret = config.getString("storage.secret.config")
      val storageService = fc.getStorageService(storageType, storageKey, storageSecret)
      val listObjs = storageService.searchObjectkeys(bucket, prefix, Option(fromDate), Option(toDate), None)
      val calendar = Calendar.getInstance()
      calendar.add(Calendar.MINUTE, expiry)
      val expiryTime = calendar.getTime.getTime
      val expiryTimeInSeconds = expiryTime / 1000
      if (listObjs.size > 0) {
        val res = for (key <- listObjs) yield {
          val dateKey = raw"(\d{4})-(\d{2})-(\d{2})".r.findFirstIn(key).getOrElse("default")
          (dateKey, storageService.getSignedURL(bucket, key, Option(expiryTimeInSeconds.toInt)))
        }
        val periodWiseFiles = res.asInstanceOf[List[(String, String)]].groupBy(_._1).mapValues(_.map(_._2))
        CommonUtil.OK(APIIds.CHANNEL_TELEMETRY_EXHAUST, Map("files" -> res.asInstanceOf[List[(String, String)]].map(_._2), "periodWiseFiles" -> periodWiseFiles, "expiresAt" -> Long.box(expiryTime)))
      } else {
        CommonUtil.OK(APIIds.CHANNEL_TELEMETRY_EXHAUST, Map("files" -> List(), "periodWiseFiles" -> Map(), "expiresAt" -> Long.box(0l)))
      }
    } else {
      APILogger.log("Request Validation FAILED")
      CommonUtil.errorResponse(APIIds.CHANNEL_TELEMETRY_EXHAUST, isValid.getOrElse("message", ""), ResponseCode.CLIENT_ERROR.toString)
    }
  }

  private def upsertRequest(body: RequestBody, channel: String)(implicit config: Config, fc: FrameworkContext): JobRequest = {
    val tag = body.request.tag.getOrElse("")
    val appendedTag = tag + ":" + channel
    val jobId = body.request.dataset.getOrElse("")
    val requestedBy = body.request.requestedBy.getOrElse("")
    val requestId = _getRequestId(tag, jobId, requestedBy, channel)
    val requestConfig = body.request.datasetConfig.getOrElse(Map.empty)
    val encryptionKey = body.request.encryptionKey
    val job = postgresDBUtil.getJobRequest(requestId, appendedTag)
    val iterationCount = if (job.nonEmpty) job.get.iteration.getOrElse(0) else 0
    val jobConfig = JobConfig(appendedTag, requestId, jobId, JobStatus.SUBMITTED.toString(), requestConfig, requestedBy, channel, DateTime.now(), encryptionKey, Option(iterationCount))

    if (job.isEmpty) {
        _saveJobRequest(jobConfig)
    } else if (job.get.status.equalsIgnoreCase("FAILED") || job.get.status.equalsIgnoreCase("SUCCESS")) {
        _updateJobRequest(jobConfig)
    } else {
      job.get
    }
  }

  private def _validateReq(body: RequestBody)(implicit config: Config): Map[String, String] = {
    if (body.request.tag.isEmpty) {
        Map("status" -> "false", "message" -> "tag is empty")
    } else if (body.request.dataset.isEmpty) {
      Map("status" -> "false", "message" -> "dataset is empty")
    } else if (body.request.datasetConfig.isEmpty) {
      Map("status" -> "false", "message" -> "datasetConfig is empty")
    } else {
       Map("status" -> "true")
    }
  }

  private def _createJobResponse(job: JobRequest)(implicit config: Config, fc: FrameworkContext): JobResponse = {
    val storageKey = config.getString("storage.key.config")
    val storageSecret = config.getString("storage.secret.config")
    val storageService = fc.getStorageService(storageType, storageKey, storageSecret)

    val expiry = config.getInt("channel.data_exhaust.expiryMins")
    val bucket = config.getString("data_exhaust.bucket")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.MINUTE, expiry)
    val expiryTime = calendar.getTime.getTime
    val expiryTimeInSeconds = expiryTime / 1000

    val processed = List(JobStatus.COMPLETED.toString(), JobStatus.FAILED.toString).contains(job.status)
    val djs = job.dt_job_submitted
    val djc = job.dt_job_completed
    val stats = if (processed) {
      Option(JobStats(job.dt_job_submitted, djc, job.execution_time))
    } else Option(JobStats(job.dt_job_submitted))
    val request = job.request_data
    val lastupdated = if (djc.getOrElse(0) == 0) job.dt_job_submitted else djc.get
    val downloadUrls = if(job.download_urls.nonEmpty) job.download_urls.get.map{f =>
      val values = f.split("/").toList.drop(4) // 4 - is derived from 2 -> '//' after http, 1 -> uri and 1 -> container
      val objectKey = values.mkString("/")
      APILogger.log("Getting signed URL for - " + objectKey)
      storageService.getSignedURL(bucket, objectKey, Option(expiryTimeInSeconds.toInt))
    } else List[String]()
    JobResponse(job.request_id, job.tag, job.job_id, job.requested_by, job.requested_channel, job.status, lastupdated, request, job.iteration.getOrElse(0), stats, Option(downloadUrls), Option(Long.box(expiryTime)), job.err_message)
  }

  private def _saveJobRequest(jobConfig: JobConfig): JobRequest = {
    postgresDBUtil.saveJobRequest(jobConfig)
    postgresDBUtil.getJobRequest(jobConfig.request_id, jobConfig.tag).get
  }

  private def _updateJobRequest(jobConfig: JobConfig): JobRequest = {
      postgresDBUtil.updateJobRequest(jobConfig)
      postgresDBUtil.getJobRequest(jobConfig.request_id, jobConfig.tag).get
   }

  private def _getRequestId(jobId: String, tag: String, requestedBy: String, requestedChannel: String): String = {
    val key = Array(tag, jobId, requestedBy, requestedChannel).mkString("|")
    MessageDigest.getInstance("MD5").digest(key.getBytes).map("%02X".format(_)).mkString
  }
  private def _validateRequest(channel: String, eventType: String, from: String, to: String)(implicit config: Config): Map[String, String] = {

    APILogger.log("Validating Request", Option(Map("channel" -> channel, "eventType" -> eventType, "from" -> from, "to" -> to)))
    val days = CommonUtil.getDaysBetween(from, to)
    if (CommonUtil.getPeriod(to) > CommonUtil.getPeriod(CommonUtil.getToday))
      return Map("status" -> "false", "message" -> "'to' should be LESSER OR EQUAL TO today's date..")
    else if (0 > days)
      return Map("status" -> "false", "message" -> "Date range should not be -ve. Please check your 'from' & 'to'")
    else if (10 < days)
      return Map("status" -> "false", "message" -> "Date range should be < 10 days")
    else return Map("status" -> "true")
  }
}