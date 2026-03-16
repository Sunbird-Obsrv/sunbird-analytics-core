package org.ekstep.analytics.util

import com.typesafe.config.{Config, ConfigFactory}

object AppConfig {

  val defaultConfig: Config = ConfigFactory.load()
  val envConfig: Config = ConfigFactory.systemEnvironment()
  val config: Config = envConfig.withFallback(defaultConfig)

  def getConfig(key: String): String = {
    if (config.hasPath(key))
      config.getString(key)
    else throw new ConfigurationException("CONFIG_NOT_FOUND", "Configuration for key [" + key + "] Not Found.")
  }

  def getSystemConfig(key: String): String = {
    val sysKey=key.replaceAll("\\.","_")
    if (config.hasPath(sysKey))
      config.getString(sysKey)
    else throw new ConfigurationException("CONFIG_NOT_FOUND", "Configuration for key [" + sysKey + "] Not Found.")
  }

  def getConfig(): Config = {
    config
  }

  def getServiceType(): String = {
    getConfig("media_service_type")
  }
}

class ConfigurationException(var errorCode: String = null, msg: String, ex: Exception = null) extends Exception(msg, ex)
