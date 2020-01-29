package org.ekstep.analytics.api.service

import javax.inject.Singleton
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.api.util.CassandraUtil
import org.ekstep.analytics.api.util.ElasticsearchService
import org.ekstep.analytics.api.util.JSONUtils
import org.ekstep.analytics.api.util.PostgresDBUtil
import org.ekstep.analytics.api.util.RedisUtil

case class ServiceHealthReport(name: String, healthy: Boolean, message: Option[String] = None)
case class GetHealthStatus()

@Singleton
class HealthCheckAPIService {

    lazy val redisUtil = new RedisUtil();
  
    def getHealthStatus(): String = {

        val checks = getChecks()
        val healthy = checks.forall { x => x.healthy == true }
        val result = Map[String, AnyRef](
            "name" -> "analytics-platform-api",
            "healthy" -> Boolean.box(healthy),
            "checks" -> checks);
        val response = CommonUtil.OK("ekstep.analytics-api.health", result)
        JSONUtils.serialize(response);
    }

    private def checkCassandraConnection(): Boolean = {
        try {
            CassandraUtil.checkCassandraConnection
        } catch {
            // $COVERAGE-OFF$ Disabling scoverage as the below code cannot be covered
            // TODO: Need to get confirmation from amit.
            case ex: Exception =>
                false
            // $COVERAGE-ON$    
        }
    }

    private def checkRedisConnection(): Boolean = {
        redisUtil.checkConnection
    }

    private def checkPostgresConnection(): Boolean = {
        val postgresDB = new PostgresDBUtil()
        postgresDB.checkConnection
    }

    private def checkElasticsearchConnection(): Boolean = {
        val es = new ElasticsearchService()
        es.healthCheck
    }

    private def getChecks(): Array[ServiceHealthReport] = {
        try {
            val cassandraStatus = ServiceHealthReport("Cassandra Database", checkCassandraConnection())
            val postgresStatus = ServiceHealthReport("Postgres Database", checkPostgresConnection())
            val redisStatus = ServiceHealthReport("Redis Database", checkRedisConnection())
            val ESStatus = ServiceHealthReport("Elasticsearch Database", checkElasticsearchConnection())
            Array(cassandraStatus, postgresStatus, redisStatus, ESStatus);
        } catch {
            // $COVERAGE-OFF$ Disabling scoverage as the below code cannot be covered
            case ex: Exception =>
                val DBStatus = ServiceHealthReport("Database Health", false, Option(ex.getMessage))
                Array(DBStatus)
            // $COVERAGE-ON$
        }
    }
    
}