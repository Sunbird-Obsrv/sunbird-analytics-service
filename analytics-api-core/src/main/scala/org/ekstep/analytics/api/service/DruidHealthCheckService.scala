package org.ekstep.analytics.api.service

import akka.actor.Actor
import javax.inject.{Inject, Singleton}
import org.ekstep.analytics.api.util.{APILogger, APIRestUtil}
import org.ekstep.analytics.framework.conf.AppConf

class DruidHealthCheckService @Inject()(restUtil: APIRestUtil) extends Actor {

  implicit val className = "org.ekstep.analytics.api.service.DruidHealthCheckService"
  val apiUrl = AppConf.getConfig("druid.coordinator.host") + AppConf.getConfig("druid.healthcheck.url")

  def receive = {
    case "health" => sender() ! getStatus
  }

  def getStatus: String = {
    val healthreport: StringBuilder = new StringBuilder()
    try {
      val response = restUtil.get[Map[String, Double]](apiUrl)
      response.map { data =>
        healthreport.append("http_druid_health_check_status{datasource=\"")
          .append(data._1).append("\"} ")
          .append(data._2).append("\n")
      }
      healthreport.toString()
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        APILogger.log("DruidHealthCheckAPI failed due to " + ex.getMessage)
        healthreport.toString()
    }
  }
}
