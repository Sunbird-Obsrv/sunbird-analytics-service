package org.ekstep.analytics.api.service.experiment

import akka.actor.Actor
import akka.pattern.pipe
import com.typesafe.config.{Config, ConfigFactory}
import javax.inject.Inject
import org.ekstep.analytics.api.util.{APILogger, ElasticsearchService, JSONUtils, RedisUtil}
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import org.ekstep.analytics.api.util.AppConfig

case class ExperimentRequest(deviceId: Option[String], userId: Option[String], url: Option[String], producer: Option[String])
case class ExperimentData(id: String, name: String, startDate: String, endDate: String, key: String, expType: String, userId: String, deviceId: String, userIdMod: Long, deviceIdMod: Long)

class ExperimentService @Inject()(redisUtil: RedisUtil, elasticsearchService :ElasticsearchService) extends Actor {

  implicit val ec: ExecutionContext = context.system.dispatchers.lookup("experiment-actor")
  implicit val className: String = "org.ekstep.analytics.api.service.experiment.ExperimentService"
  val databaseIndex: Int = AppConfig.getInt("redis.experimentIndex")
  val emptyValueExpirySeconds: Int = AppConfig.getInt("experimentService.redisEmptyValueExpirySeconds")
  val NoExperimentAssigned = "NO_EXPERIMENT_ASSIGNED"

  def receive: Receive = {
    case ExperimentRequest(deviceId, userId, url, producer) => {
      val senderActor = sender()
      val result = getExperiment(deviceId, userId, url, producer)
      result.pipeTo(senderActor)
    }
  }

  def getExperiment(deviceId: Option[String], userId: Option[String], url: Option[String], producer: Option[String]): Future[Option[ExperimentData]] = {
    val key = keyGen(deviceId, userId, url, producer)
    val jedisConnection: Jedis = redisUtil.getConnection(databaseIndex)
    
    try {
      val experimentCachedData = Option(jedisConnection.get(key))
  
      experimentCachedData.map {
        expData =>
          if (NoExperimentAssigned.equals(expData)) {
            APILogger.log("", Option(Map("comments" -> s"No experiment assigned for key $key")), "ExperimentService")
            Future(None)
          } else
            Future(resolveExperiment(JSONUtils.deserialize[ExperimentData](expData)))
      }.getOrElse {
        val data = searchExperiment(deviceId, userId, url, producer)
        data.map { result =>
          result.map { res =>
            jedisConnection.set(key, JSONUtils.serialize(res))
            resolveExperiment(res)
          }.getOrElse {
            jedisConnection.setex(key, emptyValueExpirySeconds, NoExperimentAssigned)
            None
          }
        }
      }
    } finally {
      jedisConnection.close();
    }

  }

  def resolveExperiment(data: ExperimentData): Option[ExperimentData] = {
    val typeResolver = ExperimentResolver.getResolver(data.expType)
    APILogger.log(s"resolving experiment for userID: ${data.userId}, deviceId: ${data.deviceId}, experiment resolver: ${typeResolver.getType}")
    if (typeResolver.resolve(data)) Some(data) else None
  }


  def searchExperiment(deviceId: Option[String], userId: Option[String], url: Option[String], producer: Option[String]): Future[Option[ExperimentData]]  = {
    val fields = getFieldsMap(deviceId, userId, url, producer)
    elasticsearchService.searchExperiment(fields)
  }

  def keyGen(deviceId: Option[String], userId: Option[String], url: Option[String], producer: Option[String]): String = {
    // key format "deviceId:userId:url:producer"
    List(
      deviceId.getOrElse("NA"),
      userId.getOrElse("NA"),
      url.getOrElse("NA"),
      producer.getOrElse("NA")
    ).mkString(":").toString
  }

  def getFieldsMap(deviceId: Option[String], userId: Option[String], url: Option[String], producer: Option[String]): Map[String, String] = {
    val value: mutable.Map[String, String] = mutable.Map()
    if (deviceId.isDefined) value += ("deviceId" -> deviceId.get)
    if (userId.isDefined) value += ("userId" -> userId.get)
    if (url.isDefined) value += ("url" -> url.get)
    value.toMap
  }

}
