package org.ekstep.analytics.api.service

import akka.actor.{Actor, ActorRef}
import com.google.common.net.InetAddresses
import com.google.common.primitives.UnsignedInts
import com.typesafe.config.Config
import is.tagomor.woothee.Classifier
import javax.inject.{Inject, Named}
import org.apache.logging.log4j.LogManager
import org.ekstep.analytics.api.util._
import org.joda.time.{DateTime, DateTimeZone}
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global

case class RegisterDevice(did: String, headerIP: String, ip_addr: Option[String] = None, fcmToken: Option[String] = None, producer: Option[String] = None, dspec: Option[String] = None, uaspec: Option[String] = None, first_access: Option[Long] = None, user_declared_state: Option[String] = None, user_declared_district: Option[String] = None)
case class DeviceProfileLog(device_id: String, location: DeviceLocation, device_spec: Option[Map[String, AnyRef]] = None, uaspec: Option[String] = None, fcm_token: Option[String] = None, producer_id: Option[String] = None, first_access: Option[Long] = None, user_declared_state: Option[String] = None, user_declared_district: Option[String] = None)
case class DeviceProfile(userDeclaredLocation: Option[Location], ipLocation: Option[Location])
case class Location(state: String, district: String)

sealed trait DeviceRegisterStatus
case object DeviceRegisterSuccesfulAck extends DeviceRegisterStatus
case object DeviceRegisterFailureAck extends DeviceRegisterStatus

class DeviceRegisterService @Inject() (@Named("save-metrics-actor") saveMetricsActor: ActorRef, config: Config,
                                       redisUtil: RedisUtil, kafkaUtil: KafkaUtil) extends Actor {

  implicit val className: String = "DeviceRegisterService"
  val metricsActor: ActorRef = saveMetricsActor
  val deviceDatabaseIndex: Int = config.getInt("redis.deviceIndex")
  val deviceTopic: String = AppConfig.getString("kafka.device.register.topic")
  private val logger = LogManager.getLogger("device-logger")
  private val enableDebugLogging = config.getBoolean("device.api.enable.debug.log")

  override def preStart { println("Starting DeviceRegisterService") }

  override def postStop {
    redisUtil.closePool()
    kafkaUtil.close()
    println("DeviceRegisterService stopped successfully")
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    println(s"Restarting DeviceRegisterActor: $message")
    reason.printStackTrace()
    super.preRestart(reason, message)
  }

  def receive = {
    case deviceRegDetails: RegisterDevice =>
      try {
        metricsActor.tell(IncrementApiCalls, ActorRef.noSender)
        val result = registerDevice(deviceRegDetails)
        sender() ! result
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
          val errorMessage = s"DeviceRegisterAPI failed due to ${ex.getMessage}"
          APILogger.log("", Option(Map("type" -> "api_access", "params" -> List(Map("status" -> 500, "method" -> "POST",
            "rid" -> "registerDevice", "title" -> "registerDevice")), "data" -> errorMessage)), "registerDevice")
          throw ex
      }
  }

  def registerDevice(registrationDetails: RegisterDevice): Option[DeviceRegisterStatus] = {
    val validIp = if (registrationDetails.headerIP.startsWith("192")) registrationDetails.ip_addr.getOrElse("") else registrationDetails.headerIP
    if (validIp.nonEmpty) {
      val location = resolveLocation(validIp)

      // logging metrics
      if (location.state != null && location.state.nonEmpty) {
        APILogger.log("", Option(Map("comments" -> s"Location resolved for ${registrationDetails.did} to state: ${location.state}, city: ${location.city}, district: ${location.districtCustom}")), "registerDevice")
        metricsActor.tell(IncrementLocationDbSuccessCount, ActorRef.noSender)
      } else {
        APILogger.log("", Option(Map("comments" -> s"Location is not resolved for ${registrationDetails.did}")), "registerDevice")
        metricsActor.tell(IncrementLocationDbMissCount, ActorRef.noSender)
      }

      val deviceSpec: Map[String, AnyRef] = registrationDetails.dspec match {
        case Some(value) => JSONUtils.deserialize[Map[String, AnyRef]](value)
        case None        => Map()
      }

      // Add device profile to redis cache
      val deviceProfileMap = getDeviceProfileMap(registrationDetails, location)
      val jedisConnection: Jedis = redisUtil.getConnection(deviceDatabaseIndex)
      try {
        Option(jedisConnection.hmset(registrationDetails.did, deviceProfileMap.asJava))
      } finally {
        jedisConnection.close()
      }

      APILogger.log(s"Redis-cache updated for did: ${registrationDetails.did}", None, "registerDevice")

      val deviceProfileLog = DeviceProfileLog(registrationDetails.did, location, Option(deviceSpec),
        registrationDetails.uaspec, registrationDetails.fcmToken, registrationDetails.producer, registrationDetails.first_access,
        registrationDetails.user_declared_state, registrationDetails.user_declared_district)

      logDeviceRegisterEvent(deviceProfileLog)
      Option(DeviceRegisterSuccesfulAck)
    }
    Option(DeviceRegisterFailureAck)

  }

  def logDeviceRegisterEvent(deviceProfileLog: DeviceProfileLog) = Future {
    val deviceRegisterLogEvent = generateDeviceRegistrationLogEvent(deviceProfileLog)
    kafkaUtil.send(deviceRegisterLogEvent, deviceTopic)
    metricsActor.tell(IncrementLogDeviceRegisterSuccessCount, ActorRef.noSender)
  }

  def resolveLocation(ipAddress: String): DeviceLocation = {
    val ipAddressInt: Long = UnsignedInts.toLong(InetAddresses.coerceToInteger(InetAddresses.forString(ipAddress)))
    metricsActor.tell(IncrementLocationDbHitCount, ActorRef.noSender)
    IPLocationCache.getDeviceLocation(ipAddressInt);
  }

  def parseUserAgent(uaspec: Option[String]): Option[String] = {
    uaspec.map {
      userAgent =>
        val uaspecMap = Classifier.parse(userAgent)
        val parsedUserAgentMap = Map("agent" -> uaspecMap.get("name"), "ver" -> uaspecMap.get("version"),
          "system" -> uaspecMap.get("os"), "raw" -> userAgent)
        val uaspecStr = JSONUtils.serialize(parsedUserAgentMap)
        uaspecStr
    }
  }

  def generateDeviceRegistrationLogEvent(result: DeviceProfileLog): String = {

    val uaspecStr = parseUserAgent(result.uaspec)
    val currentTime = DateTime.now(DateTimeZone.UTC).getMillis

    val deviceProfile: Map[String, Any] =
      Map(
        "device_id" -> result.device_id,
        "country_code" -> result.location.countryCode,
        "country" -> result.location.countryName,
        "state_code" -> result.location.stateCode,
        "state" -> result.location.state,
        "city" -> result.location.city,
        "state_custom" -> result.location.stateCustom,
        "state_code_custom" -> result.location.stateCodeCustom,
        "district_custom" -> result.location.districtCustom,
        "device_spec" -> result.device_spec.map(x => JSONUtils.serialize(x.mapValues(_.toString))).orNull,
        "uaspec" -> uaspecStr.orNull,
        "fcm_token" -> result.fcm_token.orNull,
        "producer_id" -> result.producer_id.orNull,
        "api_last_updated_on" -> currentTime,
        "first_access" -> currentTime,
        "user_declared_state" -> result.user_declared_state,
        "user_declared_district" -> result.user_declared_district)
    JSONUtils.serialize(deviceProfile)
  }

  def getDeviceProfileMap(registrationDetails: RegisterDevice, deviceLocation: DeviceLocation): Map[String, String] = {
    // skipping firstaccess - handled in samza job
    val dataMap =
      Map(
        "devicespec" -> registrationDetails.dspec.getOrElse(""),
        "uaspec" -> parseUserAgent(registrationDetails.uaspec).getOrElse(""),
        "fcm_token" -> registrationDetails.fcmToken.getOrElse(""),
        "producer" -> registrationDetails.producer.getOrElse(""),
        "user_declared_state" -> registrationDetails.user_declared_state.getOrElse(""),
        "user_declared_district" -> registrationDetails.user_declared_district.getOrElse(""))

    (dataMap ++ deviceLocation.toMap()).filter(data => data._2 != null && data._2.nonEmpty)
  }

}
