package org.ekstep.analytics.api.util

import java.sql.Timestamp

import scala.math.Ordering
import scala.util.Try

import org.ekstep.analytics.api.Params
import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import com.google.common.collect.HashBasedTable
import com.google.common.collect.Table
import com.typesafe.config.Config

import de.sciss.fingertree.RangedSeq
import javax.inject.Inject
import javax.inject.Singleton
import scalikejdbc._

case class ContentResult(count: Int, content: Array[Map[String, AnyRef]])

case class ContentResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: ContentResult)

case class LanguageResult(languages: Array[Map[String, AnyRef]])

case class LanguageResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: LanguageResult)

// TODO: Need to refactor this file. Reduce case classes, combine objects. Proper error handling.

@Singleton
class CacheUtil @Inject()(postgresDB: PostgresDBUtil) {

  implicit val className = "org.ekstep.analytics.api.util.CacheUtil"

  private var contentListMap: Map[String, Map[String, AnyRef]] = Map();
  private var recommendListMap: Map[String, Map[String, AnyRef]] = Map();
  private var languageMap: Map[String, String] = Map();
  private var cacheTimestamp: Long = 0L;
  private val consumerChannelTable: Table[String, String, Integer] = HashBasedTable.create();

  def initCache()(implicit config: Config) {}

  def initConsumerChannelCache()(implicit config: Config) {

    APILogger.log("Refreshing ChannelConsumer Cache")
    consumerChannelTable.clear()
    lazy val tableName = config.getString("postgres.table_name")

    val sql = s"select * from $tableName"
    Try {
      postgresDB.read(sql).map {
        consumerChannel =>
          consumerChannelTable.put(consumerChannel.consumerId, consumerChannel.channel, consumerChannel.status)
      }
      APILogger.log("ChannelConsumer Cache Refreshed Successfully!!")
    }.recover {
      case ex: Throwable =>
        APILogger.log(s"Failed to refresh ChannelConsumer Cache: ${ex.getMessage}")
        ex.printStackTrace()
    }
  }
  
  def initDeviceLocationCache()(implicit config: Config) {

    APILogger.log("Refreshing DeviceLocation Cache")
    val geoLocationCityTableName: String = config.getString("postgres.table.geo_location_city.name")
    val geoLocationCityIpv4TableName: String = config.getString("postgres.table.geo_location_city_ipv4.name")

    val devLocQuery = s"select geoname_id, continent_name, country_iso_code country_code, country_name, subdivision_1_iso_code state_code, subdivision_1_name state, subdivision_2_name sub_div_2, city_name city, subdivision_1_custom_name state_custom, subdivision_1_custom_code state_code_custom, subdivision_2_custom_name district_custom from $geoLocationCityTableName"
    val ipRangeQuery = s"select network_start_integer, network_last_integer, geoname_id from $geoLocationCityIpv4TableName"

    Try {
      var sq:RangedSeq[((Long, Long), Int),Long] = RangedSeq()(_._1, Ordering.Long)
      val geoLocations = postgresDB.readGeoLocationRange(ipRangeQuery)
      
      geoLocations.map {
        loc =>
          sq = sq.+((loc.network_start_integer, loc.network_last_integer) -> loc.geoname_id)
      }
      
      IPLocationCache.setRangeTree(sq);
      
      val devLocs = postgresDB.readLocation(devLocQuery);
      if(null != devLocs && devLocs.size > 0) {
        val devLocMap = devLocs.map(f => (f.geonameId, f)).toMap
        IPLocationCache.setGeoLocMap(devLocMap);
        println("Device geo locations count after refreshing: " + devLocs.size)
      } else {
        println("No device geo locations count after refreshing");
        IPLocationCache.setGeoLocMap(Map[Int, DeviceLocation]());
      }
      println("Range Tree geo locations count after refreshing: " + geoLocations.size)
      
      APILogger.log(s"DeviceLocation Cache Refreshed Successfully!! Range tree records: ${geoLocations.size}")
    }.recover {
      case ex: Throwable =>
        APILogger.log(s"Failed to refresh DeviceLocation Cache: ${ex.getMessage}")
        ex.printStackTrace()
    }
  }
  
  def getConsumerChannelTable()(implicit config: Config): Table[String, String, Integer] = {
    if (consumerChannelTable.size() > 0)
      consumerChannelTable
    else {
      initConsumerChannelCache()
      consumerChannelTable
    }
  }

  def validateCache()(implicit config: Config) {

    val timeAtStartOfDay = DateTime.now(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis;
    if (cacheTimestamp < timeAtStartOfDay) {
      println("cacheTimestamp:" + cacheTimestamp, "timeAtStartOfDay:" + timeAtStartOfDay, " ### Resetting content cache...### ");
      if (!contentListMap.isEmpty) contentListMap.empty;
      if (!recommendListMap.isEmpty) recommendListMap.empty;
      if (!languageMap.isEmpty) languageMap.empty;
      initCache();
    }
  }
}

object IPLocationCache {
  
  private var sq:RangedSeq[((Long, Long), Int),Long] = _;
  private var devLocMap: Map[Int, DeviceLocation] = _;
  
  def setRangeTree(sq:RangedSeq[((Long, Long), Int),Long]) = {
    this.sq = sq;
  }
  
  def setGeoLocMap(map: Map[Int, DeviceLocation]) {
    this.devLocMap = map;
  }
  
  def getIpLocation(ipAddressInt: Long) : DeviceStateDistrict = {
    val range = sq.find(ipAddressInt)
    if(range.nonEmpty) {
      val dl = this.devLocMap.get(range.get._2)
      if(dl.nonEmpty) DeviceStateDistrict(dl.get.stateCustom, dl.get.districtCustom) else DeviceStateDistrict("", "")
    } else {
      DeviceStateDistrict("", "")
    }
  }
  
  def getDeviceLocation(ipAddressInt: Long) : DeviceLocation = {
    val range = sq.find(ipAddressInt)
    if(range.nonEmpty) {
      val dl = this.devLocMap.get(range.get._2)
      if(dl.nonEmpty) dl.get else new DeviceLocation()
    } else {
      new DeviceLocation()
    }
  }
}

case class DeviceStateDistrict(state: String, districtCustom: String) {
    def this() = this("", "")
}

case class ConsumerChannel(consumerId: String, channel: String, status: Int, createdBy: String, createdOn: Timestamp, updatedOn: Timestamp)

// $COVERAGE-OFF$ cannot be covered since it is dependent on client library
object ConsumerChannel extends SQLSyntaxSupport[ConsumerChannel] {
  def apply(rs: WrappedResultSet) = new ConsumerChannel(
    rs.string("consumer_id"), rs.string("channel"), rs.int("status"),
    rs.string("created_by"), rs.timestamp("created_on"), rs.timestamp("updated_on"))
}
// $COVERAGE-ON$
