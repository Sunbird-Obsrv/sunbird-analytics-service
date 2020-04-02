package org.ekstep.analytics.api.util

import java.sql.{ResultSet, Statement}

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import java.sql.Connection

object EmbeddedPostgresql {
  
  var pg: EmbeddedPostgres = null;
  var connection: Connection = null;
  var stmt: Statement = null;

  def start() {
    pg = EmbeddedPostgres.builder().setPort(5432).start()
    connection = pg.getPostgresDatabase().getConnection()
    stmt = connection.createStatement()
  }
  
  def createTables(): Boolean = {
    val query1 = "CREATE TABLE IF NOT EXISTS geo_location_city_ipv4 (geoname_id INTEGER, network_start_integer BIGINT, network_last_integer BIGINT)"
    val query2 = "CREATE TABLE IF NOT EXISTS geo_location_city(geoname_id INTEGER UNIQUE, locale_code VARCHAR(3), continent_code VARCHAR(3), continent_name VARCHAR(100), country_iso_code VARCHAR(5), country_name VARCHAR(100), subdivision_1_iso_code VARCHAR(50), subdivision_1_name VARCHAR(100), subdivision_2_iso_code VARCHAR(50), subdivision_2_name VARCHAR(100), city_name VARCHAR(100), metro_code VARCHAR(10), time_zone VARCHAR(50), is_in_european_union SMALLINT, subdivision_1_custom_code VARCHAR(50), subdivision_1_custom_name VARCHAR(100), subdivision_2_custom_code VARCHAR(50), subdivision_2_custom_name VARCHAR(100))"
    val query3 = "CREATE TABLE IF NOT EXISTS consumer_channel(consumer_id VARCHAR(100), channel VARCHAR(20), status INTEGER, created_by VARCHAR(100), created_on TIMESTAMPTZ, updated_on TIMESTAMPTZ)"
    val query4 = "CREATE TABLE report_config(report_id text, updated_on timestamptz,report_description text,requested_by text,report_schedule text,config json,created_on timestamptz,submitted_on timestamptz,status text,status_msg text,PRIMARY KEY(report_id));"

    execute(query1)
    execute(query2)
    execute(query3)
    execute(query4)
  }

  def execute(sqlString: String): Boolean = {
    stmt.execute(sqlString)
  }

  def executeQuery(sqlString: String): ResultSet = {
    stmt.executeQuery(sqlString)
  }

  def close() {
    stmt.close()
    connection.close()
    pg.close()
  }
}