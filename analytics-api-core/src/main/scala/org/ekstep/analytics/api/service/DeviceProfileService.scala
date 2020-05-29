package org.ekstep.analytics.api.service

import akka.actor.Actor
import com.google.common.net.InetAddresses
import com.google.common.primitives.UnsignedInts
import com.typesafe.config.Config
import javax.inject.Inject
import org.ekstep.analytics.api.util._
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._

case class DeviceProfileRequest(did: String, headerIP: String)

class DeviceProfileService @Inject() (config: Config, redisUtil: RedisUtil) extends Actor {

  implicit val className: String = "DeviceProfileService"
  val deviceDatabaseIndex: Int = config.getInt("redis.deviceIndex")

  override def preStart { println("starting DeviceProfileService") }

  override def postStop {
    redisUtil.closePool()
    println("DeviceProfileService stopped successfully")
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    println(s"Restarting DeviceProfileActor: $message")
    reason.printStackTrace()
    super.preRestart(reason, message)
  }

  def receive = {
    case deviceProfile: DeviceProfileRequest =>
      try {
        val result = getDeviceProfile(deviceProfile)
        sender() ! result
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
          val errorMessage = "Get DeviceProfileAPI failed due to " + ex.getMessage
          APILogger.log("", Option(Map("type" -> "api_access", "params" -> List(Map("status" -> 500, "method" -> "POST",
            "rid" -> "getDeviceProfile", "title" -> "getDeviceProfile")), "data" -> errorMessage)), "getDeviceProfile")
          throw ex;
      }
  }

  def getDeviceProfile(deviceProfileRequest: DeviceProfileRequest): Option[DeviceProfile] = {

    if (deviceProfileRequest.headerIP.nonEmpty) {

      val ipLocationFromH2 = resolveLocation(deviceProfileRequest.headerIP)
      val did = deviceProfileRequest.did

      // logging resolved location details
      if (ipLocationFromH2.state.nonEmpty) {
        APILogger.log("", Option(Map("comments" -> s"IP Location resolved for $did to state: ${ipLocationFromH2.state}, district: ${ipLocationFromH2.districtCustom}")), "getDeviceProfile")
      } else {
        APILogger.log("", Option(Map("comments" -> s"IP Location is not resolved for $did")), "getDeviceProfile")
      }

      val jedisConnection: Jedis = redisUtil.getConnection(deviceDatabaseIndex)
      val deviceLocation = try {
        Option(jedisConnection.hgetAll(did).asScala.toMap)
      } finally {
        jedisConnection.close()
      }

      val userDeclaredLoc = if (deviceLocation.nonEmpty && deviceLocation.get.getOrElse("user_declared_state", "").nonEmpty) {
        Option(Location(deviceLocation.get("user_declared_state"), deviceLocation.get("user_declared_district")))
      } else None

      userDeclaredLoc.foreach { declaredLocation =>
        APILogger.log("", Option(Map("comments" -> s"[did: $did, user_declared_state: ${declaredLocation.state}, user_declared_district: ${declaredLocation.district}")), "DeviceProfileService")
      }

      Some(DeviceProfile(userDeclaredLoc, Option(Location(ipLocationFromH2.state, ipLocationFromH2.districtCustom))))
    } else {
      None
    }
  }

  def resolveLocation(ipAddress: String): DeviceStateDistrict = {
    val ipAddressInt: Long = UnsignedInts.toLong(InetAddresses.coerceToInteger(InetAddresses.forString(ipAddress)))
    IPLocationCache.getIpLocation(ipAddressInt);
  }

}

