package org.ekstep.analytics.api.util

import org.ekstep.analytics.api.BaseSpec
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterAll
import java.util.Date

class TestPostgresDBUtil extends FlatSpec with Matchers with BeforeAndAfterAll {

  "PostgresDBUtil" should "execute queries" in {
    
    //consumer_id VARCHAR(100), channel VARCHAR(20), status INTEGER, created_by VARCHAR(100), created_on TIMESTAMP, updated_on TIMESTAMP
    EmbeddedPostgresql.start()
    EmbeddedPostgresql.createTables()
    EmbeddedPostgresql.execute("INSERT INTO geo_location_city_ipv4 (geoname_id, network_start_integer, network_last_integer) VALUES (1234, 1781746350, 1781746370);")
    EmbeddedPostgresql.execute("INSERT INTO geo_location_city (geoname_id, continent_name, country_iso_code, country_name, subdivision_1_iso_code, subdivision_1_name, subdivision_2_name, city_name, subdivision_1_custom_name, subdivision_1_custom_code, subdivision_2_custom_name) VALUES (1234, 'Asia', 'IN', 'India', 'KA', 'Karnataka', '', 'Bangalore', 'Karnataka', '29', 'Bangalore');")
    EmbeddedPostgresql.execute("INSERT INTO consumer_channel (consumer_id, channel, status, created_by, created_on, updated_on) VALUES('1234567', '56789', 1, 'sunbird', '2017-08-19 14:22:11.802755+0530', '2017-08-19 14:22:11.802755+0530');")
    EmbeddedPostgresql.execute("SET TIME ZONE 'UTC';");
    val pgUtil = new PostgresDBUtil();
    pgUtil.checkConnection should be (true)
    
    val geoLocations = pgUtil.readGeoLocationRange("select network_start_integer, network_last_integer, geoname_id from geo_location_city_ipv4");
    geoLocations should not be (null)
    geoLocations.size should be (1);
    geoLocations.head.geoname_id should be (1234)
    geoLocations.head.network_start_integer should be (1781746350)
    geoLocations.head.network_last_integer should be (1781746370)

    val geoLocCity = pgUtil.readGeoLocationCity("select geoname_id, subdivision_1_name, subdivision_2_custom_name from geo_location_city")
    geoLocCity should not be (null)
    geoLocCity.size should be (1);
    
    geoLocCity.head.geoname_id should be (1234)
    geoLocCity.head.subdivision_1_name should be ("Karnataka")
    geoLocCity.head.subdivision_2_custom_name should be ("Bangalore")
    
    val devLoc = pgUtil.readLocation("select geoname_id, continent_name, country_iso_code country_code, country_name, subdivision_1_iso_code state_code, subdivision_1_name state, subdivision_2_name sub_div_2, city_name city, subdivision_1_custom_name state_custom, subdivision_1_custom_code state_code_custom, subdivision_2_custom_name district_custom from  geo_location_city")
    devLoc should not be (null)
    devLoc.size should be (1);
    
    devLoc.head.geonameId should be (1234)
    devLoc.head.state should be ("Karnataka")
    devLoc.head.districtCustom should be ("Bangalore")
    
    val channel = pgUtil.read("select * from consumer_channel");
    channel should not be (null)
    channel.size should be (1);
    channel.head.channel should be ("56789")
    channel.head.consumerId should be ("1234567")
    channel.head.createdBy should be ("sunbird")
    channel.head.status should be (1)
    channel.head.createdOn.getTime should be (1503132731802L)
    channel.head.updatedOn.getTime should be (1503132731802L)
    
    new GeoLocationCity();
    new GeoLocationRange();
    
    EmbeddedPostgresql.close();
  }
}