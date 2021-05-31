package org.ekstep.analytics.api.util

import org.ekstep.analytics.api.{DatasetConfig, JobConfig, ReportRequest}
import org.joda.time.DateTime
import scalikejdbc._

import java.sql.{DriverManager, PreparedStatement, ResultSet, Timestamp}
import java.util.Date
import javax.inject._
import scala.collection.mutable.ListBuffer

@Singleton
class PostgresDBUtil {

    private lazy val db = AppConfig.getString("postgres.db")
    private lazy val url = AppConfig.getString("postgres.url")
    private lazy val user = AppConfig.getString("postgres.user")
    private lazy val pass = AppConfig.getString("postgres.pass")
    Class.forName("org.postgresql.Driver")
    ConnectionPool.singleton(s"$url$db", user, pass)

    implicit val session: AutoSession = AutoSession

    private lazy val dbc = DriverManager.getConnection(s"$url$db", user, pass);// ConnectionPool.borrow()

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


    def saveReportConfig(reportRequest: ReportRequest) = {
        val config = JSONUtils.serialize(reportRequest.config)
        val table = ReportConfig.tableName
        val insertQry = s"INSERT INTO $table (report_id, updated_on, report_description, requested_by, report_schedule, config, created_on, submitted_on, status, status_msg) values (?, ?, ?, ?, ?, ?::json, ?, ?, ?, ?)";
        val pstmt: PreparedStatement = dbc.prepareStatement(insertQry);
        pstmt.setString(1, reportRequest.reportId);
        pstmt.setTimestamp(2, new Timestamp(new DateTime().getMillis));
        pstmt.setString(3, reportRequest.description);
        pstmt.setString(4, reportRequest.createdBy);
        pstmt.setString(5, reportRequest.reportSchedule);
        pstmt.setString(6, config);
        pstmt.setTimestamp(7, new Timestamp(new DateTime().getMillis));
        pstmt.setTimestamp(8, new Timestamp(new DateTime().getMillis));
        pstmt.setString(9, "ACTIVE");
        pstmt.setString(10, "REPORT SUCCESSFULLY ACTIVATED");
        pstmt.execute()
    }


    def updateReportConfig(reportId: String, reportRequest: ReportRequest) = {
        val config = JSONUtils.serialize(reportRequest.config)
        val table = ReportConfig.tableName
        val insertQry = s"update $table set updated_on = ?, report_description = ?, requested_by = ?, report_schedule = ?, config = ?::JSON , status = 'ACTIVE' , status_msg = 'REPORT SUCCESSFULLY ACTIVATED'  where report_id = ?";
        val pstmt: PreparedStatement = dbc.prepareStatement(insertQry);
        pstmt.setTimestamp(1, new Timestamp(new DateTime().getMillis));
        pstmt.setString(2, reportRequest.description);
        pstmt.setString(3, reportRequest.createdBy);
        pstmt.setString(4, reportRequest.reportSchedule);
        pstmt.setString(5, config);
        pstmt.setString(6, reportId);
        pstmt.execute()
    }

    def readReport(reportId: String): Option[ReportConfig] = {
        sql"select * from ${ReportConfig.table} where report_id = ${reportId}".map(rc => ReportConfig(rc)).first().apply()
    }

    def deactivateReport(reportId: String) = {
        val table = ReportConfig.tableName
        val query = s"update $table set updated_on = ?, status='INACTIVE',status_msg = 'REPORT DEACTIVATED' where report_id=?";
        val pstmt: PreparedStatement = dbc.prepareStatement(query);
        pstmt.setTimestamp(1, new Timestamp(new DateTime().getMillis));
        pstmt.setString(2, reportId);
        pstmt.execute()
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

    def getJobRequestsCount(filters: Map[String, AnyRef]): Int = {
        val (whereClause, whereClauseValues): (List[String], List[AnyRef]) = createSearchWhereClause(getSearchQueryColumns(filters))
        val prepareStatements: PreparedStatement = dbc.prepareStatement(s"SELECT count(*) FROM job_request where ${whereClause.mkString(""" and """)}")
        val prepareStatement = updateStatement(prepareStatements, whereClauseValues)
        val rs = prepareStatement.executeQuery()
        var count: Int = 0
        while (rs.next()) {
            count = rs.getInt("count")
        }
        count
    }

    def searchJobRequest(filters: Map[String, AnyRef], limit: Int): List[JobRequest] = {
        val (whereClause, whereClauseValues): (List[String], List[AnyRef]) = createSearchWhereClause(getSearchQueryColumns(filters))
        val prepareStatements: PreparedStatement = dbc.prepareStatement(s"SELECT * FROM job_request where ${whereClause.mkString(""" and """)} order by dt_job_submitted DESC LIMIT $limit ")
        val prepareStatement = updateStatement(prepareStatements, whereClauseValues)
        val rs = prepareStatement.executeQuery()
        val result = new ListBuffer[JobRequest]()
        while (rs.next()) {
            result += JobRequest(rs = rs)
        }
        result.toList
    }

    private def updateStatement(prepareStatement: PreparedStatement, whereClauseValues: List[AnyRef]): PreparedStatement = {
        for ((value, ind) <- whereClauseValues.view.zip(Stream from 1)) {
            value match {
                case date: Date =>
                    prepareStatement.setDate(ind, new java.sql.Date(date.getTime))
                case _ =>
                    prepareStatement.setString(ind, value.toString)
            }
        }
        prepareStatement
    }

    private def createSearchWhereClause(params: Map[String, AnyRef]): (List[String], List[AnyRef]) = {
        import java.text.SimpleDateFormat
        val df = new SimpleDateFormat("yyyy-MM-dd")
        val whereClause = new ListBuffer[String]()
        val whereClauseValues = new ListBuffer[AnyRef]()
        params.map {
            case (col, value) =>
                if (col == "dt_job_submitted") {
                    whereClause += "date(dt_job_submitted) = ?::DATE"
                    whereClauseValues += df.parse(value.toString)
                } else {
                    whereClause += s"$col = ?"
                    whereClauseValues += value
                }
        }
        (whereClause.toList, whereClauseValues.toList)
    }

    private def getSearchQueryColumns(filters: Map[String, AnyRef]): Map[String, String] = {
        val requestedDate: String = filters.getOrElse("requestedDate", null).asInstanceOf[String]
        val dataset: String = filters.get("dataset").orNull.asInstanceOf[String]
        val status: String = filters.get("status").orNull.asInstanceOf[String]
        val requested_channel: String = filters.get("channel").orNull.asInstanceOf[String]
        Map("job_id" -> dataset, "status" -> status, "requested_channel" -> requested_channel, "dt_job_submitted" -> requestedDate).filter(_._2 != null)
    }

    def getDataset(datasetId: String): Option[DatasetRequest] = {
        sql"""select * from ${DatasetRequest.table} where dataset_id = $datasetId""".map(rs => DatasetRequest(rs)).first().apply()
    }

    def getDatasetList(): List[DatasetRequest] = {
        sql"""select * from ${DatasetRequest.table}""".map(rs => DatasetRequest(rs)).list().apply()
    }

    def saveJobRequest(jobRequest: JobConfig) = {
        val requestData = JSONUtils.serialize(jobRequest.dataset_config)
        val encryptionKey = jobRequest.encryption_key.getOrElse(null)
        val table = JobRequest.tableName
        val insertQry = s"INSERT INTO $table (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, encryption_key, iteration) values (?, ?, ?, ?, ?::json, ?, ?, ?, ?, ?)";
        val pstmt: PreparedStatement = dbc.prepareStatement(insertQry);
        pstmt.setString(1, jobRequest.tag);
        pstmt.setString(2, jobRequest.request_id);
        pstmt.setString(3, jobRequest.dataset);
        pstmt.setString(4, jobRequest.status);
        pstmt.setString(5, requestData);
        pstmt.setString(6, jobRequest.requested_by);
        pstmt.setString(7, jobRequest.requested_channel);
        pstmt.setTimestamp(8, new Timestamp(new DateTime().getMillis));
        pstmt.setString(9, encryptionKey);
        pstmt.setInt(10, jobRequest.iteration.getOrElse(0));
        pstmt.execute()
    }

    def updateJobRequest(jobRequest: JobConfig) = {
        val requestData = JSONUtils.serialize(jobRequest.dataset_config)
        val encryptionKey = jobRequest.encryption_key.getOrElse(null)
        val table = JobRequest.tableName
        val insertQry = s"UPDATE $table set dt_job_submitted =? , job_id =?, status =?, request_data =?::json, requested_by =?, requested_channel =?, encryption_key =?, iteration =? where tag =? and request_id =?"
        val pstmt: PreparedStatement = dbc.prepareStatement(insertQry);
        pstmt.setTimestamp(1, new Timestamp(new DateTime().getMillis));
        pstmt.setString(2, jobRequest.dataset);
        pstmt.setString(3, jobRequest.status);
        pstmt.setString(4, requestData);
        pstmt.setString(5, jobRequest.requested_by);
        pstmt.setString(6, jobRequest.requested_channel);
        pstmt.setString(7, encryptionKey);
        pstmt.setInt(8, jobRequest.iteration.getOrElse(0));
        pstmt.setString(9, jobRequest.tag);
        pstmt.setString(10, jobRequest.request_id);
        pstmt.execute()
    }

    def saveDatasetRequest(datasetRequest: DatasetConfig) = {
        val datasetConfig = JSONUtils.serialize(datasetRequest.dataset_config)
        val table = DatasetRequest.tableName
        val insertQry = s"INSERT INTO $table (dataset_id, dataset_config, visibility, dataset_type, version, authorized_roles, available_from, sample_request, sample_response) values (?, ?::json, ?, ?, ?, ?, ?, ?, ?)";
        val pstmt: PreparedStatement = dbc.prepareStatement(insertQry);
        pstmt.setString(1, datasetRequest.dataset_id);
        pstmt.setString(2, datasetConfig);
        pstmt.setString(3, datasetRequest.visibility);
        pstmt.setString(4, datasetRequest.dataset_type);
        pstmt.setString(5, datasetRequest.version);
        val authorizedRoles = datasetRequest.authorized_roles.toArray.asInstanceOf[Array[Object]];
        pstmt.setArray(6, dbc.createArrayOf("text", authorizedRoles));
        pstmt.setTimestamp(7, new Timestamp(datasetRequest.available_from.getMillis));
        pstmt.setString(8, datasetRequest.sample_request.getOrElse(""));
        pstmt.setString(9, datasetRequest.sample_response.getOrElse(""));
        pstmt.execute()
    }

    def updateDatasetRequest(datasetRequest: DatasetConfig) = {
        val table = DatasetRequest.tableName
        val updateQry = s"UPDATE $table SET available_from = ?, dataset_type=?, dataset_config=?::json, visibility=?, version=?, authorized_roles=?, sample_request=?, sample_response=? WHERE dataset_id=?";
        val datasetConfig = JSONUtils.serialize(datasetRequest.dataset_config)
        val pstmt: PreparedStatement = dbc.prepareStatement(updateQry);
        pstmt.setTimestamp(1, new Timestamp(datasetRequest.available_from.getMillis));
        pstmt.setString(2, datasetRequest.dataset_type);
        pstmt.setString(3, datasetConfig);
        pstmt.setString(4, datasetRequest.visibility);
        pstmt.setString(5, datasetRequest.version);
        val authorizedRoles = datasetRequest.authorized_roles.toArray.asInstanceOf[Array[Object]];
        pstmt.setArray(6, dbc.createArrayOf("text", authorizedRoles));
        dbc.createArrayOf("text", authorizedRoles)
        pstmt.setString(7, datasetRequest.sample_request.getOrElse(""));
        pstmt.setString(8, datasetRequest.sample_response.getOrElse(""));
        pstmt.setString(9, datasetRequest.dataset_id);
        pstmt.execute()
    }

    //Experiment
    def getExperimentDefinition(expId: String): Option[ExperimentDefinition] = {
        sql"""select * from ${ExperimentDefinition.table} where exp_id = $expId""".map(rs => ExperimentDefinition(rs)).first().apply()
    }

    def saveExperimentDefinition(expRequests: Array[ExperimentDefinition]) = {
        val table = ExperimentDefinition.tableName
        expRequests.map { expRequest =>
            val query = s"INSERT INTO $table (exp_id, exp_name, status, exp_description, exp_data, updated_on, created_by, updated_by, created_on, status_message, criteria, stats) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            val pstmt: PreparedStatement = dbc.prepareStatement(query);
            pstmt.setString(1, expRequest.exp_id);
            pstmt.setString(2, expRequest.exp_name);
            pstmt.setString(3, expRequest.status.get);
            pstmt.setString(4, expRequest.exp_description);
            pstmt.setString(5, expRequest.exp_data);
            pstmt.setTimestamp(6, new Timestamp(expRequest.updated_on.get.getMillis));
            pstmt.setString(7, expRequest.created_by);
            pstmt.setString(8, expRequest.updated_by);
            pstmt.setTimestamp(9, new Timestamp(expRequest.created_on.get.getMillis));
            pstmt.setString(10, expRequest.status_message.get);
            pstmt.setString(11, expRequest.criteria);
            pstmt.setString(12, expRequest.stats.getOrElse(""));
            pstmt.execute()
        }
    }

    def updateExperimentDefinition(expRequests: Array[ExperimentDefinition]) = {
        val table = ExperimentDefinition.tableName
        expRequests.map { expRequest =>
            val query = s"UPDATE $table set exp_name =?, status =?, exp_description =?, exp_data =?, updated_on =?, created_by =?, updated_by =?, created_on =?, status_message =?, criteria =?, stats =? where exp_id =?";
            val pstmt: PreparedStatement = dbc.prepareStatement(query);
            pstmt.setString(1, expRequest.exp_name);
            pstmt.setString(2, expRequest.status.get);
            pstmt.setString(3, expRequest.exp_description);
            pstmt.setString(4, expRequest.exp_data);
            pstmt.setTimestamp(5, new Timestamp(expRequest.updated_on.get.getMillis));
            pstmt.setString(6, expRequest.created_by);
            pstmt.setString(7, expRequest.updated_by);
            pstmt.setTimestamp(8, new Timestamp(expRequest.created_on.get.getMillis));
            pstmt.setString(9, expRequest.status_message.get);
            pstmt.setString(10, expRequest.criteria);
            pstmt.setString(11, expRequest.stats.getOrElse(""));
            pstmt.setString(12, expRequest.exp_id);
            pstmt.execute()
        }
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

    def apply(rs: ResultSet) = new JobRequest(tag = rs.getString("tag"),
        request_id = rs.getString("request_id"),
        job_id = rs.getString("job_id"),
        status = rs.getString("status"),
        request_data = JSONUtils.deserialize[Map[String, Any]](rs.getString("request_data")),
        requested_by = rs.getString("requested_by"),
        requested_channel = rs.getString("requested_channel"),
        dt_job_submitted = rs.getTimestamp("dt_job_submitted").getTime,
        download_urls = if (rs.getArray("download_urls") != null) Some(rs.getArray("download_urls").getArray.asInstanceOf[Array[String]].toList) else None,
        dt_file_created = if (rs.getTimestamp("dt_file_created") != null) Some(rs.getTimestamp("dt_file_created").getTime) else None,
        dt_job_completed = if (rs.getTimestamp("dt_job_completed") != null) Some(rs.getTimestamp("dt_job_completed").getTime) else None,
        execution_time = Some(rs.getLong("execution_time")),
        err_message = Some(rs.getString("err_message")),
        iteration = Some(rs.getInt("iteration"))
    )
}

case class DatasetRequest(dataset_id: String, dataset_config: Map[String, Any], visibility: String, dataset_type: String,
                          version: String , authorized_roles: List[String], available_from: Option[Long],
                          sample_request: Option[String], sample_response: Option[String]) {
    def this() = this("", Map[String, Any](), "", "", "", List(""), None, None, None)
}

object DatasetRequest extends SQLSyntaxSupport[DatasetRequest] {
    override val tableName = AppConfig.getString("postgres.table.dataset_metadata.name")
    override val columns = Seq("dataset_id", "dataset_config", "visibility", "dataset_type", "version",
        "authorized_roles", "available_from", "sample_request", "sample_response")
    override val useSnakeCaseColumnName = false

    def apply(rs: WrappedResultSet) = new DatasetRequest(
        rs.string("dataset_id"),
        JSONUtils.deserialize[Map[String, Any]](rs.string("dataset_config")),
        rs.string("visibility"),
        rs.string("dataset_type"),
        rs.string("version"),
        rs.array("authorized_roles").getArray.asInstanceOf[Array[String]].toList,
        if(rs.timestampOpt("available_from").nonEmpty) Option(rs.timestamp("available_from").getTime) else None,
        rs.stringOpt("sample_request"),
        rs.stringOpt("sample_response")
    )
}

case class ExperimentDefinition(exp_id: String, exp_name: String, exp_description: String, created_by: String,
                                updated_by: String, updated_on: Option[DateTime], created_on: Option[DateTime], criteria: String,
                                exp_data: String, status: Option[String], status_message: Option[String], stats: Option[String]) {
    def this() = this("", "", "", "", "", None, None, "", "", None, None, None)
}

object ExperimentDefinition extends SQLSyntaxSupport[ExperimentDefinition] {
    override val tableName = AppConfig.getString("postgres.table.experiment_definition.name")
    override val columns = Seq("exp_id", "exp_name", "exp_description", "created_by", "updated_by", "updated_on",
        "created_on", "criteria", "exp_data", "status", "status_message", "stats")
    override val useSnakeCaseColumnName = false

    def apply(rs: WrappedResultSet) = new ExperimentDefinition(
        rs.string("exp_id"),
        rs.string("exp_name"),
        rs.string("exp_description"),
        rs.string("created_by"),
        rs.string("updated_by"),
        if(rs.timestampOpt("updated_on").nonEmpty) Option(new DateTime(rs.timestampOpt("updated_on").get.getTime)) else None,
        if(rs.timestampOpt("created_on").nonEmpty) Option(new DateTime(rs.timestampOpt("created_on").get.getTime)) else None,
        rs.string("criteria"),
        rs.string("exp_data"),
        rs.stringOpt("status"),
        rs.stringOpt("status_message"),
        rs.stringOpt("stats")
    )
}