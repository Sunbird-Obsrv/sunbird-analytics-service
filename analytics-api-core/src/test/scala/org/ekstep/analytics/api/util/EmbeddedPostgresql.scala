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
    val query4 = "CREATE TABLE IF NOT EXISTS report_config(report_id text, updated_on timestamptz,report_description text,requested_by text,report_schedule text,config json,created_on timestamptz,submitted_on timestamptz,status text,status_msg text,PRIMARY KEY(report_id));"
    val query5 = "CREATE TABLE IF NOT EXISTS job_request(tag VARCHAR(100), request_id VARCHAR(50), job_id VARCHAR(50), status VARCHAR(50), request_data json, requested_by VARCHAR(50), requested_channel VARCHAR(50), dt_job_submitted TIMESTAMP, download_urls text[], dt_file_created TIMESTAMP, dt_job_completed TIMESTAMP, execution_time INTEGER, err_message VARCHAR(100), iteration INTEGER, encryption_key VARCHAR(50), PRIMARY KEY (tag, request_id));"
    val query6 = "CREATE TABLE IF NOT EXISTS experiment_definition (exp_id VARCHAR(50), created_by VARCHAR(50), created_on TIMESTAMP, criteria VARCHAR(100), exp_data VARCHAR(300), exp_description VARCHAR(200), exp_name VARCHAR(50), stats VARCHAR(300), status VARCHAR(50), status_message VARCHAR(50), updated_by VARCHAR(50), updated_on TIMESTAMP, PRIMARY KEY(exp_id));"
    val query7 = "CREATE TABLE IF NOT EXISTS dataset_metadata(dataset_id VARCHAR(50), dataset_config json, visibility VARCHAR(50), dataset_type VARCHAR(50), version VARCHAR(10), authorized_roles text[], available_from TIMESTAMP, sample_request VARCHAR(300), sample_response VARCHAR(500), validation_json json, druid_query json, limits json, supported_formats VARCHAR(20), exhaust_type VARCHAR(50), PRIMARY KEY (dataset_id));"

    execute(query1)
    execute(query2)
    execute(query3)
    execute(query4)
    execute(query5)
    execute(query6)
    execute(query7)
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