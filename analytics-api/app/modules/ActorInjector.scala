package modules

import akka.actor.Props
import akka.routing.FromConfig
import com.google.inject.AbstractModule
import org.ekstep.analytics.api.service.{DeviceProfileService, _}
import org.ekstep.analytics.api.util.APILogger
import play.api.libs.concurrent.AkkaGuiceSupport

class ActorInjector extends AbstractModule with AkkaGuiceSupport {
  override def configure(): Unit = {
    val actorConfig = new FromConfig()
    // Actor Binding
    bindActor[DeviceRegisterService](name = "device-register-actor", _.withRouter(actorConfig))
    bindActor[DeviceProfileService](name = "device-profile-actor", _.withRouter(actorConfig))
    bindActor[ExperimentAPIService](name = "experiment-actor")
    bindActor[SaveMetricsActor](name = "save-metrics-actor")
    bindActor[CacheRefreshActor](name = "cache-refresh-actor")
    bindActor[JobAPIService](name = "job-service-actor")
    bindActor[ClientLogsAPIService](name = "client-log-actor")
    bindActor[DruidHealthCheckService](name = "druid-health-actor")
    bindActor[ReportAPIService](name = "report-actor")

    // Services
    APILogger.init("org.ekstep.analytics-api")
  }
}
