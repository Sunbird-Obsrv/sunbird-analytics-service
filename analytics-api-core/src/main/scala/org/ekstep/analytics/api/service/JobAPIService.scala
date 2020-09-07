package org.ekstep.analytics.api.service

import java.security.MessageDigest
import java.util.Calendar

import akka.actor.Actor
import com.typesafe.config.Config
import javax.inject.Inject
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.api.util.JobRequest
import org.ekstep.analytics.api.util._
import org.ekstep.analytics.api.{APIIds, JobStats, OutputFormat, _}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{FrameworkContext, JobStatus}
import org.joda.time.DateTime
import org.sunbird.cloud.storage.conf.AppConf

import scala.collection.mutable.Buffer
import scala.util.Sorting

/**
  * @author mahesh
  */


case class DataRequest(request: String, channel: String, config: Config)

case class GetDataRequest(clientKey: String, requestId: String, config: Config)

case class DataRequestList(clientKey: String, limit: Int, config: Config)

case class ChannelData(channel: String, event_type: String, from: String, to: String, since: String, config: Config)

class JobAPIService @Inject()(postgresDBUtil: PostgresDBUtil) extends Actor  {

  implicit val fc = new FrameworkContext();

  def receive = {
    case DataRequest(request: String, channelId: String, config: Config) => sender() ! dataRequest(request, channelId)(config, fc)
    case GetDataRequest(clientKey: String, requestId: String, config: Config) => sender() ! getDataRequest(clientKey, requestId)(config, fc)
    case DataRequestList(clientKey: String, limit: Int, config: Config) => sender() ! getDataRequestList(clientKey, limit)(config, fc)
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

  def getDataRequest(clientKey: String, requestId: String)(implicit config: Config, fc: FrameworkContext): Response = {
    val job = postgresDBUtil.getJobRequest(requestId, clientKey)
    if (null == job || job.isEmpty) {
      CommonUtil.errorResponse(APIIds.GET_DATA_REQUEST, "no job available with the given request_id and client_key", ResponseCode.OK.toString)
    } else {
      val jobStatusRes = _createJobResponse(job.get)
      CommonUtil.OK(APIIds.GET_DATA_REQUEST, CommonUtil.caseClassToMap(jobStatusRes))
    }
  }

  def getDataRequestList(tag: String, limit: Int)(implicit config: Config, fc: FrameworkContext): Response = {
    val currDate = DateTime.now()
    val jobRequests = postgresDBUtil.getJobRequestList(tag)
    val result = jobRequests.take(limit).map { x => _createJobResponse(x) }
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

      val storageService = fc.getStorageService(storageType)
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
    val jobId = body.request.jobId.getOrElse("")
    val requestedBy = body.request.requestedBy.getOrElse("")
    val requestId = _getRequestId(tag, jobId, requestedBy, channel)
    val jobConfig = body.request.jobConfig.getOrElse(Map.empty)
    val job = postgresDBUtil.getJobRequest(requestId, tag)

    if (null == job || job.isEmpty) {
      _saveJobRequest(requestId, tag, jobId, requestedBy, channel, jobConfig)
    } else {
      job.get
    }
  }

  private def _validateReq(body: RequestBody)(implicit config: Config): Map[String, String] = {
    val outputFormat = body.request.output_format.getOrElse(OutputFormat.JSON)
    if (outputFormat != null && !outputFormat.isEmpty && !(outputFormat.equals(OutputFormat.CSV) || outputFormat.equals(OutputFormat.JSON))) {
        Map("status" -> "false", "message" -> "invalid type. It should be one of [csv, json].")
    } else if (body.request.tag.isEmpty) {
        Map("status" -> "false", "message" -> "tag is empty")
    } else if (body.request.jobId.isEmpty) {
      Map("status" -> "false", "message" -> "jobId is empty")
    } else {
       Map("status" -> "true")
    }
  }

  private def getDateInMillis(date: DateTime): Option[Long] = {
    if (null != date) Option(date.getMillis) else None
  }

  private def _createJobResponse(job: JobRequest)(implicit config: Config, fc: FrameworkContext): JobResponse = {
    val storageService = fc.getStorageService(storageType)

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
    val downladUrls = job.download_urls.getOrElse(List[String]()).map{f => storageService.getSignedURL(bucket, f, Option(expiryTimeInSeconds.toInt)).asInstanceOf[String] }
    JobResponse(job.request_id, job.status, lastupdated, request, job.iteration.getOrElse(0), stats, Option(downladUrls), Option(expiryTimeInSeconds))
  }

  private def _saveJobRequest(requestId: String, tag: String, jobId: String, requestedBy: String, requestedChannel: String, request: Map[String, Any]): JobRequest = {
    val status = JobStatus.SUBMITTED.toString()
    val jobSubmitted = DateTime.now()
    val jobConfig = JobConfig(tag, requestId, jobId, status, request, requestedBy, requestedChannel, jobSubmitted)
    postgresDBUtil.saveJobRequest(jobConfig)
    postgresDBUtil.getJobRequest(requestId, tag).get
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
