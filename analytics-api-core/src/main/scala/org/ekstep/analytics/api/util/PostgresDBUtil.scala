package org.ekstep.analytics.api.util

import java.util.Date

import javax.inject._
import org.ekstep.analytics.api.ReportRequest
import scalikejdbc._

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
              ${new Date()}, ${new Date()} ,'SUBMITTED', 'REPORT SUCCESSFULLY SUBMITTED')""".update().apply().toString
    }


    def updateReportConfig(reportId: String, reportRequest: ReportRequest): String = {
        val config = JSONUtils.serialize(reportRequest.config)
        val q =
            sql"""update ${ReportConfig.table} set updated_on =${new Date()} ,
             report_description = ${reportRequest.description}, requested_by = ${reportRequest.createdBy} ,
             report_schedule = ${reportRequest.reportSchedule} , config = ($config::JSON) ,
               status = 'SUBMITTED' , status_msg = 'REPORT SUCCESSFULLY SUBMITTED'  where report_id =$reportId"""
        q.update().apply().toString
    }

    def readReport(reportId: String): Option[ReportConfig] = {
        sql"select * from ${ReportConfig.table} where report_id = ${reportId}".map(rc => ReportConfig(rc)).first().apply()
    }

    def deleteReport(reportId: String) = {
        sql"delete from ${ReportConfig.table} where report_id=$reportId".execute().apply()

    }


    def readReportList(status: List[Any]): List[ReportConfig] = {
        sql"""select * from ${ReportConfig.table} where status IN ($status)""".map(rs => ReportConfig(rs)).list().apply()
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