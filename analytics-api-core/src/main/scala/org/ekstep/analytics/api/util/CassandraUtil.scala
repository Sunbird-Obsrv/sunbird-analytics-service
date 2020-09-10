package org.ekstep.analytics.api.util

import akka.actor.Actor
import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.{QueryBuilder => QB}
import org.ekstep.analytics.api.{Constants, ExperimentDefinition}
import org.ekstep.analytics.framework.util.JobLogger
import org.joda.time.DateTime

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

object CassandraUtil {

    case class GetJobRequest(requestId: String, clientId: String)
    case class SaveJobRequest(jobRequest: Array[JobRequest])

    implicit val className = "DBUtil"
    val host = AppConfig.getString("spark.cassandra.connection.host")
    val port = AppConfig.getInt("spark.cassandra.connection.port")
    val cluster = {
        Cluster.builder()
            .addContactPoint(host)
            .withPort(port)
            .withoutJMXReporting()
            .build()
    }
    var session = cluster.connect()

    //Experiment
    def getExperimentDefinition(expId: String): Option[ExperimentDefinition] = {
        val query = QB.select().from(Constants.PLATFORM_DB, Constants.EXPERIMENT_TABLE).allowFiltering()
          .where(QB.eq("exp_id", expId))
        val resultSet = session.execute(query)
        val job = resultSet.asScala.map(row => expRowToCaseClass(row)).toArray
        job.headOption
    }

    def saveExperimentDefinition(expRequests: Array[ExperimentDefinition]) = {
        import scala.collection.JavaConversions._
        
        expRequests.map { expRequest =>
            val stats = scala.collection.JavaConversions.mapAsJavaMap(expRequest.stats.getOrElse(Map[String, Long]()));
            var query = QB.insertInto(Constants.PLATFORM_DB, Constants.EXPERIMENT_TABLE).value("exp_id", expRequest.expId)
              .value("exp_name", expRequest.expName).value("status", expRequest.status.get).value("exp_description", expRequest.expDescription)
              .value("exp_data", expRequest.data).value("updated_on", setDateColumn(expRequest.udpatedOn).orNull)
              .value("created_by", expRequest.createdBy).value("updated_by", expRequest.updatedBy)
              .value("created_on", setDateColumn(expRequest.createdOn).orNull).value("status_message", expRequest.status_msg.get)
              .value("criteria", expRequest.criteria).value("stats", stats)

            session.execute(query)
        }
    }

    def expRowToCaseClass(row: Row): ExperimentDefinition = {
        import scala.collection.JavaConversions._
        val statsMap = row.getMap("stats", classOf[String], classOf[java.lang.Long])
        val stats = mapAsScalaMap(statsMap).toMap
        ExperimentDefinition(row.getString("exp_id"), row.getString("exp_name"),
            row.getString("exp_description"), row.getString("created_by"), row.getString("updated_by"),
            getExpDateColumn(row, "updated_on"), getExpDateColumn(row, "created_on"),
            row.getString("criteria"), row.getString("exp_data"),
            Option(row.getString("status")), Option(row.getString("status_message")), Option(stats.asInstanceOf[Map[String, Long]])
        )
    }

    def getDateColumn(row: Row, column: String): Option[DateTime] = if (null == row.getObject(column)) None else Option(new DateTime(row.getTimestamp("dt_job_submitted")))

    def getExpDateColumn(row: Row, column: String): Option[DateTime] = if (null == row.getObject(column)) None else Option(new DateTime(row.getTimestamp(column)))

    def setDateColumn(date: Option[DateTime]): Option[Long] = {
        val timestamp = date.getOrElse(null)
        if (null == timestamp) None else Option(timestamp.getMillis())
    }

    sys.ShutdownHookThread {
        session.close()
        JobLogger.log("Closing the cassandra session")
    }

    def checkCassandraConnection(): Boolean = {
        try {
            if (null != session && !session.isClosed()) true else false
        } catch {
            // $COVERAGE-OFF$ Disabling scoverage as the below code cannot be covered
            // TODO: Need to get confirmation from amit.
            case ex: Exception =>
                false
            // $COVERAGE-ON$    
        }
    }
}
