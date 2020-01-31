package org.ekstep.analytics.framework

import ing.wbaa.druid.DruidConfig
import ing.wbaa.druid.client.DruidClient
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.conf.AppConf
import org.sunbird.cloud.storage.factory.{ StorageConfig, StorageServiceFactory }

import scala.collection.mutable.Map
import org.ekstep.analytics.framework.util.HadoopFileUtil

class FrameworkContext {

  var dc: DruidClient = null;
  var storageContainers: Map[String, BaseStorageService] = Map();
  val fileUtil = new HadoopFileUtil();

  def initialize(storageServices: Option[Array[(String, String, String)]]) {
    dc = DruidConfig.DefaultConfig.client;
    if (storageServices.nonEmpty) {
      storageServices.get.foreach(f => {
        getStorageService(f._1, f._2, f._3);
      })
    }
  }

  def getStorageService(storageType: String): BaseStorageService = {
    getStorageService(storageType, storageType, storageType);
  }

  def getHadoopFileUtil(): HadoopFileUtil = {
    return fileUtil;
  }

  def getStorageService(storageType: String, storageKey: String, storageSecret: String): BaseStorageService = {
    if (!storageContainers.contains(storageType + "|" + storageKey)) {
      storageContainers.put(storageType, StorageServiceFactory.getStorageService(StorageConfig(storageType, AppConf.getStorageKey(storageKey), AppConf.getStorageSecret(storageSecret))));
    }
    storageContainers.get(storageType).get
  }

  def setDruidClient(druidClient: DruidClient) {
    dc = druidClient;
  }

  def getDruidClient(): DruidClient = {
    if (null == dc) {
      dc = DruidConfig.DefaultConfig.client;
    }
    return dc;
  }

  def shutdownDruidClient() = {
    if (dc != null) dc.actorSystem.terminate()
  }

  def shutdownStorageService() = {
    if (null != storageContainers) {
      storageContainers.foreach(f => f._2.closeContext());
    }
  }

  def closeContext() = {
    shutdownDruidClient();
    shutdownStorageService();
  }

}