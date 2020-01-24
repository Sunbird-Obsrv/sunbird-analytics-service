package org.ekstep.analytics.api.util

import com.typesafe.config.ConfigFactory

object AppConfig {

  implicit val className = "org.ekstep.analytics.framework.conf.AppConf";

  val defaultConf = ConfigFactory.load();
  val envConf = ConfigFactory.systemEnvironment();
  val conf = envConf.withFallback(defaultConf);

  def getString(key: String): String = {
    conf.getString(key);
  }

  def getInt(key: String): Int = {
    conf.getInt(key);
  }
  
  def getDouble(key: String): Double = {
    conf.getDouble(key);
  }
}