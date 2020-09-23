package org.ekstep.analytics.api.util

import java.util.Date

import javax.inject._
import org.ekstep.analytics.api.{JobConfig, ReportRequest}
import org.joda.time.DateTime
import scalikejdbc._
import collection.JavaConverters._

@Singleton
class PostgresDBUtil {

    private lazy val db = AppConfig.getString("postgres.db")
    private lazy val url = AppConfig.getString("postgres.url")
    private lazy val user = AppConfig.getString("postgres.user")
    private lazy val pass = AppConfig.getString("postgres.pass")

    Class.forName("org.postgresql.Driver")
    ConnectionPool.singleton(s"$url$db", user, pass)

    implicit val session: AutoSession = AutoSession

    def read(sqlString: String): List[ConsumerChannel] = {
        SQL(sqlString).map(rs => ConsumerChannel(rs)).list().apply()
    }

    def readLocation(sqlString: String): List[DeviceLocation] = {
        SQL(sqlString).map(rs => DeviceLocation(rs)).list().apply()
    }

    def readGeoLocationCity(sqlString: String): List[GeoLocationCity] = {
        SQL(sqlString).map(rs => GeoLocationCity(rs)).list().apply()
    }

    def readGeoLocationRange(sqlString: String): List[GeoLocationRange] = {
        SQL(sqlString).map(rs => GeoLocationRange(rs)).list().apply()
    }


    def saveReportConfig(reportRequest: ReportRequest): String = {
        val config = JSONUtils.serialize(reportRequest.config)
        sql"""insert into ${ReportConfig.table}(report_id, updated_on, report_description, requested_by,
             report_schedule, config, created_on, submitted_on, status, status_msg) values
              (${reportRequest.reportId}, ${new Date()}, ${reportRequest.description},
              ${reportRequest.createdBy},${reportRequest.reportSchedule} , CAST($config AS JSON),
              ${new Date()}, ${new Date()} ,'ACTIVE', 'REPORT SUCCESSFULLY ACTIVATED')""".update().apply().toString
    }


    def updateReportConfig(reportId: String, reportRequest: ReportRequest): String = {
        val config = JSONUtils.serialize(reportRequest.config)
        val q =
            sql"""update ${ReportConfig.table} set updated_on =${new Date()} ,
             report_description = ${reportRequest.description}, requested_by = ${reportRequest.createdBy} ,
             report_schedule = ${reportRequest.reportSchedule} , config = ($config::JSON) ,
               status = 'ACTIVE' , status_msg = 'REPORT SUCCESSFULLY ACTIVATED'  where report_id =$reportId"""
        q.update().apply().toString
    }

    def readReport(reportId: String): Option[ReportConfig] = {
        sql"select * from ${ReportConfig.table} where report_id = ${reportId}".map(rc => ReportConfig(rc)).first().apply()
    }

    def deactivateReport(reportId: String) = {
        sql"update ${ReportConfig.table} set updated_on =${new Date()}, status='INACTIVE',status_msg = 'REPORT DEACTIVATED' where report_id=$reportId".update().apply()
    }


    def readReportList(status: List[Any]): List[ReportConfig] = {
        sql"""select * from ${ReportConfig.table} where status IN ($status)""".map(rs => ReportConfig(rs)).list().apply()
    }

    def getJobRequest(requestId: String, tag: String): Option[JobRequest] = {
        sql"""select * from ${JobRequest.table} where request_id = $requestId and tag = $tag""".map(rs => JobRequest(rs)).first().apply()
    }

    def getJobRequestList(tag: String, limit: Int): List[JobRequest] = {
        sql"""select * from ${JobRequest.table} where tag = $tag limit $limit""".map(rs => JobRequest(rs)).list().apply()
    }

    def saveJobRequest(jobRequest: JobConfig) = {
        val requestData = JSONUtils.serialize(jobRequest.dataset_config)
        val encryptionKey = jobRequest.encryption_key.getOrElse(null)
        val query = sql"""insert into ${JobRequest.table} ("tag", "request_id", "job_id", "status", "request_data", "requested_by", "requested_channel", "dt_job_submitted", "encryption_key", "iteration") values
              (${jobRequest.tag}, ${jobRequest.request_id}, ${jobRequest.dataset}, ${jobRequest.status},
              CAST($requestData AS JSON), ${jobRequest.requested_by}, ${jobRequest.requested_channel},
              ${new Date()}, ${encryptionKey}, ${jobRequest.iteration.getOrElse(0)})"""
        query.update().apply().toString
    }

    def updateJobRequest(jobRequest: JobConfig) = {
        val requestData = JSONUtils.serialize(jobRequest.dataset_config)
        val encryptionKey = jobRequest.encryption_key.getOrElse(null)
        val query = sql"""update ${JobRequest.table} set dt_job_submitted =${new Date()} ,
              job_id =${jobRequest.dataset}, status =${jobRequest.status}, request_data =CAST($requestData AS JSON),
              requested_by =${jobRequest.requested_by}, requested_channel =${jobRequest.requested_channel},
              encryption_key =${encryptionKey}, iteration =${jobRequest.iteration.getOrElse(0)}
              where tag =${jobRequest.tag} and request_id =${jobRequest.request_id}"""
        query.update().apply().toString
    }

    def checkConnection = {
        try {
            val conn = ConnectionPool.borrow()
            conn.close()
            true
        } catch {
            case ex: Exception =>
                ex.printStackTrace()
                false
        }
    }
}

case class DeviceLocation(geonameId: Int, continentName: String, countryCode: String, countryName: String, stateCode: String,
                          state: String, subDivsion2: String, city: String,
                          stateCustom: String, stateCodeCustom: String, districtCustom: String) {
    def this() = this(0, "", "", "", "", "", "", "", "", "", "")

    def toMap() = Map("geoname_id" -> geonameId.toString(), "continent_name" -> continentName,
        "country_code" -> countryCode, "country_name" -> countryName, "state_code" -> stateCode,
        "state" -> state, "city" -> city, "state_custom" -> stateCustom, "state_code_custom" -> stateCodeCustom,
        "district_custom" -> districtCustom)
}

object DeviceLocation extends SQLSyntaxSupport[DeviceLocation] {
    def apply(rs: WrappedResultSet) = new DeviceLocation(
        rs.int("geoname_id"),
        rs.string("continent_name"),
        rs.string("country_code"),
        rs.string("country_name"),
        rs.string("state_code"),
        rs.string("state"),
        rs.string("sub_div_2"),
        rs.string("city"),
        rs.string("state_custom"),
        rs.string("state_code_custom"),
        rs.string("district_custom"))
}

case class GeoLocationCity(geoname_id: Int, subdivision_1_name: String, subdivision_2_custom_name: String) {
    def this() = this(0, "", "")
}

object GeoLocationCity extends SQLSyntaxSupport[GeoLocationCity] {
    def apply(rs: WrappedResultSet) = new GeoLocationCity(
        rs.int("geoname_id"),
        rs.string("subdivision_1_name"),
        rs.string("subdivision_2_custom_name"))
}

case class GeoLocationRange(network_start_integer: Long, network_last_integer: Long, geoname_id: Int) {
    def this() = this(0, 0, 0)
}

object GeoLocationRange extends SQLSyntaxSupport[GeoLocationRange] {
    def apply(rs: WrappedResultSet) = new GeoLocationRange(
        rs.long("network_start_integer"),
        rs.long("network_last_integer"),
        rs.int("geoname_id"))
}

case class ReportConfig(reportId: String, updatedOn: Long, reportDescription: String, requestedBy: String,
                        reportSchedule: String, config: Map[String, Any], createdOn: Long, submittedOn: Long, status: String, status_msg: String) {
    def this() = this("", 0, "", "", "", Map.empty, 0, 0, "", "")
}

object ReportConfig extends SQLSyntaxSupport[ReportConfig] {
    override val tableName = AppConfig.getString("postgres.table.report_config.name")
    override val columns = Seq("report_id", "updated_on", "report_description", "requested_by", "report_schedule", "config",
        "created_on", "submitted_on", "status", "status_msg")
    override val useSnakeCaseColumnName = false

    def apply(rs: WrappedResultSet) = new ReportConfig(
        rs.string("report_id"),
        rs.timestamp("updated_on").getTime,
        rs.string("report_description"),
        rs.string("requested_by"),
        rs.string("report_schedule"),
        JSONUtils.deserialize[Map[String, Any]](rs.string("config")),
        rs.timestamp("created_on").getTime,
        rs.timestamp("submitted_on").getTime,
        rs.string("status"),
        rs.string("status_msg")
    )
}

case class JobRequest(tag: String, request_id: String, job_id: String, status: String,
                      request_data: Map[String, Any], requested_by: String, requested_channel: String,
                      dt_job_submitted: Long , download_urls: Option[List[String]], dt_file_created: Option[Long],
                      dt_job_completed: Option[Long], execution_time: Option[Long], err_message: Option[String], iteration: Option[Int]) {
    def this() = this("", "", "", "", Map[String, Any](), "", "", 0, None, None, None, None, None, None)
}

object JobRequest extends SQLSyntaxSupport[JobRequest] {
    override val tableName = AppConfig.getString("postgres.table.job_request.name")
    override val columns = Seq("tag", "request_id", "job_id", "status", "request_data", "requested_by",
        "requested_channel", "dt_job_submitted", "download_urls", "dt_file_created", "dt_job_completed", "execution_time", "err_message", "iteration")
    override val useSnakeCaseColumnName = false

    def apply(rs: WrappedResultSet) = new JobRequest(
        rs.string("tag"),
        rs.string("request_id"),
        rs.string("job_id"),
        rs.string("status"),
        JSONUtils.deserialize[Map[String, Any]](rs.string("request_data")),
        rs.string("requested_by"),
        rs.string("requested_channel"),
        rs.timestamp("dt_job_submitted").getTime,
        if(rs.arrayOpt("download_urls").nonEmpty) Option(rs.array("download_urls").getArray.asInstanceOf[Array[String]].toList) else None,
        if(rs.timestampOpt("dt_file_created").nonEmpty) Option(rs.timestamp("dt_file_created").getTime) else None,
        if(rs.timestampOpt("dt_job_completed").nonEmpty) Option(rs.timestamp("dt_job_completed").getTime) else None,
        rs.longOpt("execution_time"),
        rs.stringOpt("err_message"),
        rs.intOpt("iteration")
    )
}