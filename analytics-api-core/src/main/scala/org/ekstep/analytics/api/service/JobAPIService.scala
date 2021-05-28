package org.ekstep.analytics.api.service

import java.security.MessageDigest
import java.util.Calendar

import akka.actor.Actor
import com.typesafe.config.Config
import javax.inject.{Inject, Singleton}
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.api.util.CommonUtil.dateFormat
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

case class SearchRequest(request: String, config: Config)

case class GetDataRequest(tag: String, requestId: String, config: Config)

case class DataRequestList(tag: String, limit: Int, config: Config)

case class ChannelData(channel: String, eventType: String, from: Option[String], to: Option[String], since: Option[String], config: Config)

case class PublicData(datasetId: String, from: Option[String], to: Option[String], since: Option[String], date: Option[String], dateRange: Option[String], config: Config)

case class AddDataSet(request: String, config: Config)

case class ListDataSet(config: Config)

class JobAPIService @Inject()(postgresDBUtil: PostgresDBUtil) extends Actor  {

  implicit val fc = new FrameworkContext();

  def receive = {
    case DataRequest(request: String, channelId: String, config: Config) => sender() ! dataRequest(request, channelId)(config, fc)
    case GetDataRequest(tag: String, requestId: String, config: Config) => sender() ! getDataRequest(tag, requestId)(config, fc)
    case DataRequestList(tag: String, limit: Int, config: Config) => sender() ! getDataRequestList(tag, limit)(config, fc)
    case ChannelData(channel: String, eventType: String, from: Option[String], to: Option[String], since: Option[String], config: Config) => sender() ! getChannelData(channel, eventType, from, to, since)(config, fc)
    case PublicData(datasetId: String, from: Option[String], to: Option[String], since: Option[String], date: Option[String], dateRange: Option[String], config: Config) => sender() ! getPublicData(datasetId, from, to, since, date, dateRange)(config, fc)
    case AddDataSet(request: String, config: Config) => sender() ! addDataSet(request)(config, fc)
    case ListDataSet(config: Config) => sender() ! listDataSet()(config, fc)
    case SearchRequest(request: String, config: Config) => sender() ! searchRequest(request)(config, fc)
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

  def searchRequest(request: String)(implicit config: Config, fc: FrameworkContext): Response = {
    val body = JSONUtils.deserialize[RequestBody](request)
    val isValid = _validateSearchReq(body)
    if ("true".equals(isValid("status"))) {
      val limit = body.request.limit.getOrElse(config.getInt("dataset.request.search.limit"))
      val jobRequests = postgresDBUtil.searchJobRequest(body.request.filters.getOrElse(Map()), limit)
      val requestsCount = postgresDBUtil.getJobRequestsCount(body.request.filters.getOrElse(Map()))
      val result = jobRequests.map { x => _createJobResponse(x) }
      CommonUtil.OK(APIIds.SEARCH_DATA_REQUEST, Map("count" -> Int.box(requestsCount), "jobs" -> result))
    } else
      CommonUtil.errorResponse(APIIds.SEARCH_DATA_REQUEST, isValid("message"), ResponseCode.CLIENT_ERROR.toString)
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

  def getChannelData(channel: String, datasetId: String, from: Option[String], to: Option[String], since: Option[String] = None)(implicit config: Config, fc: FrameworkContext): Response = {

    val objectLists = getExhaustObjectKeys(Option(channel), datasetId, from, to,since)
    val isValid = objectLists._1
    val listObjs = objectLists._2

    if ("true".equalsIgnoreCase(isValid.getOrElse("status", "false"))) {
      val expiry = config.getInt("channel.data_exhaust.expiryMins")
      val calendar = Calendar.getInstance()
      calendar.add(Calendar.MINUTE, expiry)
      val expiryTime = calendar.getTime.getTime

      if (listObjs.size > 0) {
        val periodWiseFiles = listObjs.asInstanceOf[List[(String, String)]].groupBy(_._1).mapValues(_.map(_._2))
        CommonUtil.OK(APIIds.CHANNEL_TELEMETRY_EXHAUST, Map("files" -> listObjs.asInstanceOf[List[(String, String)]].map(_._2), "periodWiseFiles" -> periodWiseFiles, "expiresAt" -> Long.box(expiryTime)))
      } else {
        CommonUtil.OK(APIIds.CHANNEL_TELEMETRY_EXHAUST, Map("files" -> List(), "periodWiseFiles" -> Map(), "expiresAt" -> Long.box(0l)))
      }
    } else {
      APILogger.log("Request Validation FAILED")
      CommonUtil.errorResponse(APIIds.CHANNEL_TELEMETRY_EXHAUST, isValid.getOrElse("message", ""), ResponseCode.CLIENT_ERROR.toString)
    }
  }

  def getPublicData(datasetId: String, from: Option[String] = None, to: Option[String] = None, since: Option[String] = None, date: Option[String] = None, dateRange: Option[String] = None)(implicit config: Config, fc: FrameworkContext): Response = {

    val isDatasetValid = _validateDataset(datasetId)

    if ("true".equalsIgnoreCase(isDatasetValid.getOrElse("status", "false"))) {
      val dates: Option[DateRange] = if (dateRange.nonEmpty) Option(CommonUtil.getIntervalRange(dateRange.get)) else None

      if (dates.nonEmpty && dates.get.from.isEmpty) {
        APILogger.log("Request Validation FAILED for data range field")
        val availableIntervals = CommonUtil.getAvailableIntervals()
        CommonUtil.errorResponse(APIIds.PUBLIC_TELEMETRY_EXHAUST, s"Provided dateRange ${dateRange.get} is not valid. Please use any one from this list - $availableIntervals", ResponseCode.CLIENT_ERROR.toString)
      }
      else {
        val computedFrom = if (dates.nonEmpty) Option(dates.get.from) else if (date.nonEmpty) date else from
        val computedTo =  if (dates.nonEmpty) Option(dates.get.to) else if (date.nonEmpty) date else to

        val objectLists = getExhaustObjectKeys(None, datasetId, computedFrom, computedTo, since, true)
        val isValid = objectLists._1
        val listObjs = objectLists._2

        if ("true".equalsIgnoreCase(isValid.getOrElse("status", "false"))) {
          if (listObjs.size > 0) {
            val periodWiseFiles = listObjs.asInstanceOf[List[(String, String)]].groupBy(_._1).mapValues(_.map(_._2))
            CommonUtil.OK(APIIds.PUBLIC_TELEMETRY_EXHAUST, Map("files" -> listObjs.asInstanceOf[List[(String, String)]].map(_._2), "periodWiseFiles" -> periodWiseFiles))
          } else {
            CommonUtil.OK(APIIds.PUBLIC_TELEMETRY_EXHAUST, Map("message" -> "Files are not available for requested date. Might not yet generated. Please come back later"))
          }
        } else {
          APILogger.log("Request Validation FAILED")
          CommonUtil.errorResponse(APIIds.PUBLIC_TELEMETRY_EXHAUST, isValid.getOrElse("message", ""), ResponseCode.CLIENT_ERROR.toString)
        }
      }
    }
    else {
      APILogger.log("Request Validation FAILED for invalid datasetId")
      CommonUtil.errorResponse(APIIds.PUBLIC_TELEMETRY_EXHAUST, isDatasetValid.getOrElse("message", ""), ResponseCode.CLIENT_ERROR.toString)
    }

  }

  def addDataSet(request: String)(implicit config: Config, fc: FrameworkContext): Response = {
    val body = JSONUtils.deserialize[RequestBody](request)
    val isValid = _validateDatasetReq(body)
    if ("true".equals(isValid.get("status").get)) {
      val dataset = upsertDatasetRequest(body)
      val response = CommonUtil.caseClassToMap(_createDatasetResponse(dataset))
      CommonUtil.OK(APIIds.ADD_DATASET_REQUEST, Map("message" -> s"Dataset ${dataset.dataset_id} added successfully"))
    } else {
      CommonUtil.errorResponse(APIIds.ADD_DATASET_REQUEST, isValid.get("message").get, ResponseCode.CLIENT_ERROR.toString)
    }
  }

  def listDataSet()(implicit config: Config, fc: FrameworkContext): Response = {
    val datasets = postgresDBUtil.getDatasetList()
    val result = datasets.map { x => _createDatasetResponse(x) }
    CommonUtil.OK(APIIds.LIST_DATASET, Map("count" -> Int.box(datasets.size), "datasets" -> result))
  }

  private def getExhaustObjectKeys(channel: Option[String], datasetId: String, from: Option[String], to: Option[String], since: Option[String] = None, isPublic: Boolean = false)(implicit config: Config, fc: FrameworkContext): (Map[String, String], List[(String, String)]) = {

    val fromDate = if (since.nonEmpty) since.get else if (from.nonEmpty) from.get else CommonUtil.getPreviousDay()
    val toDate = if (to.nonEmpty) to.get else CommonUtil.getToday()

    val isValid = _validateRequest(channel, datasetId, fromDate, toDate, isPublic)
    if ("true".equalsIgnoreCase(isValid.getOrElse("status", "false"))) {
      val loadConfig = if (isPublic) config.getObject(s"public.data_exhaust.dataset").unwrapped() else config.getObject(s"channel.data_exhaust.dataset").unwrapped()
      val datasetConfig = if (null != loadConfig.get(datasetId)) loadConfig.get(datasetId).asInstanceOf[java.util.Map[String, AnyRef]] else loadConfig.get("default").asInstanceOf[java.util.Map[String, AnyRef]]
      val bucket = datasetConfig.get("bucket").toString
      val basePrefix = datasetConfig.get("basePrefix").toString
      val prefix = if(channel.isDefined) basePrefix + datasetId + "/" + channel.get + "/" else basePrefix + datasetId + "/"
      APILogger.log("prefix: " + prefix)

      val storageKey = if (isPublic) config.getString("public.storage.key.config") else config.getString("storage.key.config")
      val storageSecret = if (isPublic) config.getString("public.storage.secret.config") else config.getString("storage.secret.config")
      val storageService = fc.getStorageService(storageType, storageKey, storageSecret)
      val listObjs = storageService.searchObjectkeys(bucket, prefix, Option(fromDate), Option(toDate), None)
      if (listObjs.size > 0) {
        val res = for (key <- listObjs) yield {
          val dateKey = raw"(\d{4})-(\d{2})-(\d{2})".r.findFirstIn(key).getOrElse("default")
          if (isPublic) {
            (dateKey, getCDNURL(key))
          }
          else {
            val expiry = config.getInt("channel.data_exhaust.expiryMins")
            (dateKey, storageService.getSignedURL(bucket, key, Option((expiry * 60))))
          }
        }
        return (isValid, res)
      }
    }
    return (isValid, List())
  }

  private def upsertRequest(body: RequestBody, channel: String)(implicit config: Config, fc: FrameworkContext): JobRequest = {
    val tag = body.request.tag.getOrElse("")
    val appendedTag = tag + ":" + channel
    val jobId = body.request.dataset.getOrElse("")
    val requestedBy = body.request.requestedBy.getOrElse("")
    val submissionDate = DateTime.now().toString("yyyy-MM-dd")
    val requestId = _getRequestId(tag, jobId, requestedBy, channel, submissionDate)
    val requestConfig = body.request.datasetConfig.getOrElse(Map.empty)
    val encryptionKey = body.request.encryptionKey
    val job = postgresDBUtil.getJobRequest(requestId, appendedTag)
    val iterationCount = if (job.nonEmpty) job.get.iteration.getOrElse(0) else 0
    val jobConfig = JobConfig(appendedTag, requestId, jobId, JobStatus.SUBMITTED.toString(), requestConfig, requestedBy, channel, DateTime.now(), encryptionKey, Option(iterationCount))

    if (job.isEmpty) {
        _saveJobRequest(jobConfig)
    } else if (job.get.status.equalsIgnoreCase("FAILED")) {
        _updateJobRequest(jobConfig)
    } else {
      job.get
    }
  }

  private def upsertDatasetRequest(body: RequestBody)(implicit config: Config, fc: FrameworkContext): DatasetRequest = {

    val datasetId = body.request.dataset.get
    val datasetConf = body.request.datasetConfig.getOrElse(Map.empty)
    val datasetType = body.request.datasetType.get
    val visibility = body.request.visibility.get
    val version = body.request.version.get
    val authorizedRoles = body.request.authorizedRoles.get
    val sampleRequest = body.request.sampleRequest
    val sampleResponse = body.request.sampleResponse
    val availableFrom = if(body.request.availableFrom.nonEmpty) dateFormat.parseDateTime(body.request.availableFrom.get) else DateTime.now()

    val datasetConfig = DatasetConfig(datasetId, datasetType, datasetConf, visibility, version, authorizedRoles, sampleRequest, sampleResponse, availableFrom)
    val datasetdetails = postgresDBUtil.getDataset(datasetId)
    if (datasetdetails.isEmpty) {
      _saveDatasetRequest(datasetConfig)
    } else {
      _updateDatasetRequest(datasetConfig)
    }
  }

  private def _validateReq(body: RequestBody)(implicit config: Config): Map[String, String] = {
    val batchLimit = config.getInt("data_exhaust.batch.limit")
    if (body.request.tag.isEmpty) {
        Map("status" -> "false", "message" -> "tag is empty")
    } else if (body.request.dataset.isEmpty) {
      Map("status" -> "false", "message" -> "dataset is empty")
    } else if (body.request.datasetConfig.isEmpty) {
      Map("status" -> "false", "message" -> "datasetConfig is empty")
    } else {
      val batchId = body.request.datasetConfig.get.get("batchId")
      val batches = if (batchId.nonEmpty) List(batchId.get.asInstanceOf[String]) else body.request.datasetConfig.get.getOrElse("batchFilter", List[String]()).asInstanceOf[List[String]]
      val searchFilter = body.request.datasetConfig.get.get("searchFilter")
      if(batches.isEmpty && searchFilter.isEmpty) {
        Map("status" -> "false", "message" -> "Request should have either of batchId, batchFilter or searchFilter")
      }
      else if (batches.length > batchLimit)
        Map("status" -> "false", "message" -> s"Number of batches in request exceeded. It should be within $batchLimit")
      else
        Map("status" -> "true")
    }
  }

  private def _validateDatasetReq(body: RequestBody)(implicit config: Config): Map[String, String] = {
    if (body.request.dataset.isEmpty) {
      Map("status" -> "false", "message" -> "dataset is empty")
    } else if (body.request.datasetType.isEmpty) {
      Map("status" -> "false", "message" -> "datasetType is empty")
    } else if (body.request.version.isEmpty) {
      Map("status" -> "false", "message" -> "version is empty")
    } else if (body.request.visibility.isEmpty) {
      Map("status" -> "false", "message" -> "visibility is empty")
    } else if (body.request.authorizedRoles.isEmpty) {
      Map("status" -> "false", "message" -> "authorizedRoles is empty")
    } else {
      Map("status" -> "true")
    }
  }

  private def _validateSearchReq(body: RequestBody)(implicit config: Config): Map[String, String] = {
    import scala.collection.JavaConverters._
    val filters: List[String] = Option(config.getStringList("dataset.request.search.filters").asScala.toList).getOrElse(List("dataset", "requestedDate", "status", "channel"))
    if (body.request.filters.nonEmpty) {
      val isPresets: List[Boolean] = filters.map(param => body.request.filters.getOrElse(Map()).contains(param))
      if (isPresets.contains(true)) Map("status" -> "true") else Map("status" -> "false", "message" -> "Unsupported filters")
    } else {
      Map("status" -> "false", "message" -> "Filters are empty")
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

    val processed = List("SUCCESS", "FAILED").contains(job.status)
    val djs = job.dt_job_submitted
    val djc = job.dt_job_completed
    val stats = if (processed) {
      Option(JobStats(job.dt_job_submitted, djc, job.execution_time))
    } else Option(JobStats(job.dt_job_submitted))
    val request = job.request_data
    val lastupdated = if (djc.getOrElse(0) == 0) job.dt_job_submitted else djc.get
    val downloadUrls = if(processed && job.download_urls.nonEmpty) job.download_urls.get.map{f =>
      val values = f.split("/").toList.drop(4) // 4 - is derived from 2 -> '//' after http, 1 -> uri and 1 -> container
      val objectKey = values.mkString("/")
      APILogger.log("Getting signed URL for - " + objectKey)
      storageService.getSignedURL(bucket, objectKey, Option((expiry * 60)))
    } else List[String]()
    JobResponse(job.request_id, job.tag, job.job_id, job.requested_by, job.requested_channel, job.status, lastupdated, request, job.iteration.getOrElse(0), stats, Option(downloadUrls), Option(Long.box(expiryTime)), job.err_message)
  }

  private def _createDatasetResponse(dataset: DatasetRequest)(implicit config: Config, fc: FrameworkContext): DatasetResponse = {

    DatasetResponse(dataset.dataset_id, dataset.dataset_type, dataset.dataset_config, dataset.visibility, dataset.version,
      dataset.authorized_roles, dataset.sample_request, dataset.sample_response, dateFormat.print(new DateTime(dataset.available_from.get)))
  }

  private def _saveJobRequest(jobConfig: JobConfig): JobRequest = {
    postgresDBUtil.saveJobRequest(jobConfig)
    postgresDBUtil.getJobRequest(jobConfig.request_id, jobConfig.tag).get
  }

  private def _updateJobRequest(jobConfig: JobConfig): JobRequest = {
      postgresDBUtil.updateJobRequest(jobConfig)
      postgresDBUtil.getJobRequest(jobConfig.request_id, jobConfig.tag).get
  }

  private def _saveDatasetRequest(datasetConfig: DatasetConfig): DatasetRequest = {
    postgresDBUtil.saveDatasetRequest(datasetConfig)
    postgresDBUtil.getDataset(datasetConfig.dataset_id).get
  }

  private def _updateDatasetRequest(datasetConfig: DatasetConfig): DatasetRequest = {
    postgresDBUtil.updateDatasetRequest(datasetConfig)
    postgresDBUtil.getDataset(datasetConfig.dataset_id).get
  }

  def _getRequestId(jobId: String, tag: String, requestedBy: String, requestedChannel: String, submissionDate: String): String = {
    val key = Array(tag, jobId, requestedBy, requestedChannel, submissionDate).mkString("|")
    MessageDigest.getInstance("MD5").digest(key.getBytes).map("%02X".format(_)).mkString
  }
  private def _validateRequest(channel: Option[String], datasetId: String, from: String, to: String, isPublic: Boolean = false)(implicit config: Config): Map[String, String] = {

    APILogger.log("Validating Request", Option(Map("channel" -> channel, "datasetId" -> datasetId, "from" -> from, "to" -> to)))
    val days = CommonUtil.getDaysBetween(from, to)
    val expiryMonths = config.getInt("public.data_exhaust.expiryMonths")
    val maxInterval = if (isPublic) config.getInt("public.data_exhaust.max.interval.days") else 10
    if (CommonUtil.getPeriod(to) > CommonUtil.getPeriod(CommonUtil.getToday))
      return Map("status" -> "false", "message" -> "'to' should be LESSER OR EQUAL TO today's date..")
    else if (0 > days)
      return Map("status" -> "false", "message" -> "Date range should not be -ve. Please check your 'from' & 'to'")
    else if (maxInterval < days)
      return Map("status" -> "false", "message" -> s"Date range should be < $maxInterval days")
    else if (isPublic && (CommonUtil.getPeriod(from) <  CommonUtil.getPeriod(CommonUtil.dateFormat.print(new DateTime().minusMonths(expiryMonths)))))
      return Map("status" -> "false", "message" -> s"Date range cannot be older than $expiryMonths months")
    else return Map("status" -> "true")
  }

  def _validateDataset(datasetId: String)(implicit config: Config): Map[String, String] = {
    val validDatasets = config.getStringList("public.data_exhaust.datasets")
    if (validDatasets.contains(datasetId)) Map("status" -> "true") else Map("status" -> "false", "message" -> s"Provided dataset is invalid. Please use any one from this list - $validDatasets")
  }

  def getCDNURL(key: String)(implicit config: Config): String = {
    val cdnHost = config.getString("cdn.host")
    cdnHost + "/" + key
  }
}