package org.ekstep.analytics.framework

import com.ing.wbaa.druid.{DruidConfig, QueryHost}
import com.ing.wbaa.druid.client.DruidClient
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.sunbird.cloud.storage.{IStorageService, StorageConfig => SdkStorageConfig, StorageServiceFactory}

import scala.collection.mutable.Map
import org.ekstep.analytics.framework.util.HadoopFileUtil
import org.apache.spark.util.LongAccumulator
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.fetcher.{AkkaHttpClient, AkkaHttpUtil, DruidDataFetcher}

class FrameworkContext {

  var dc: DruidClient = null;
  var drc: DruidClient = null;
  var storageContainers: Map[String, IStorageService] = Map();
  val fileUtil = new HadoopFileUtil();
  
  var inputEventsCount: LongAccumulator = _
  var outputEventsCount: LongAccumulator = _

  def initialize(storageServices: Option[Array[(String, String, String)]]) {
    dc = DruidConfig.DefaultConfig.client;
    if (storageServices.nonEmpty) {
      storageServices.get.foreach(f => {
        getStorageService(f._1, f._2, f._3);
      })
    }
  }

  def getStorageService(storageType: String): IStorageService = {
    getStorageService(storageType, storageType, storageType);
  }

  def getHadoopFileUtil(): HadoopFileUtil = {
    return fileUtil;
  }

  def newStorageService(storageType: String, storageKey: String, storageSecret: String): IStorageService = {
    val storageEndpoint = AppConf.getConfig("cloud_storage_endpoint_with_protocol")
    val storageRegion = AppConf.getConfig("cloud_storage_region")
    if ("s3".equalsIgnoreCase(storageType) && !"".equalsIgnoreCase(storageEndpoint)) {
      val s3Config = SdkStorageConfig.builder(SdkStorageConfig.StorageType.CEPHS3)
        .authType(SdkStorageConfig.AuthType.ACCESS_KEY)
        .storageKey(AppConf.getConfig(storageKey))
        .storageSecret(AppConf.getConfig(storageSecret))
        .endPoint(storageEndpoint)
        .build()
      StorageServiceFactory.getStorageService(s3Config)
    } else if ("oci".equalsIgnoreCase(storageType) && !"".equalsIgnoreCase(storageEndpoint)) {
      val ociConfig = SdkStorageConfig.builder(SdkStorageConfig.StorageType.OCI)
        .authType(SdkStorageConfig.AuthType.ACCESS_KEY)
        .storageKey(AppConf.getConfig(storageKey))
        .storageSecret(AppConf.getConfig(storageSecret))
        .endPoint(storageEndpoint)
        .region(storageRegion)
        .build()
      StorageServiceFactory.getStorageService(ociConfig)
    } else if ("aws".equalsIgnoreCase(storageType)) {
      val awsConfigBuilder = SdkStorageConfig.builder(SdkStorageConfig.StorageType.AWS)
      val webIdentityTokenFile = System.getenv("AWS_WEB_IDENTITY_TOKEN_FILE")
      val roleArn = System.getenv("AWS_ROLE_ARN")
      val useIAM = webIdentityTokenFile != null && !webIdentityTokenFile.isEmpty && roleArn != null && !roleArn.isEmpty
      // Only set region if explicitly configured; otherwise let the SDK auto-detect from
      // AWS_DEFAULT_REGION / AWS_REGION env vars (automatically set in EKS/OIDC pods)
      if (storageRegion != null && !storageRegion.isEmpty) awsConfigBuilder.region(storageRegion)
      // For IAM/OIDC, do NOT set a custom endpoint — the SDK must use the standard AWS regional
      // endpoint for SigV4 signing to work correctly with temporary IRSA credentials.
      // A custom endpoint (e.g. https://s3.us-east-1.amazonaws.com) forces customEndpoint=true
      // in AwsStorageService, which causes signature mismatch with session-token-based auth.
      if (!useIAM && storageEndpoint != null && !storageEndpoint.isEmpty) awsConfigBuilder.endPoint(storageEndpoint)
      if (useIAM) {
        awsConfigBuilder.authType(SdkStorageConfig.AuthType.IAM)
      } else {
        awsConfigBuilder.authType(SdkStorageConfig.AuthType.ACCESS_KEY)
          .storageKey(AppConf.getConfig(storageKey))
          .storageSecret(AppConf.getConfig(storageSecret))
      }
      StorageServiceFactory.getStorageService(awsConfigBuilder.build())
    } else {
      val sdkType = storageType.toLowerCase match {
        case "azure" => SdkStorageConfig.StorageType.AZURE
        case "gcloud" => SdkStorageConfig.StorageType.GCLOUD
        case "oci"   => SdkStorageConfig.StorageType.OCI
        case _       => SdkStorageConfig.StorageType.CEPHS3
      }
      val config = SdkStorageConfig.builder(sdkType)
        .authType(SdkStorageConfig.AuthType.ACCESS_KEY)
        .storageKey(AppConf.getConfig(storageKey))
        .storageSecret(AppConf.getConfig(storageSecret))
        .build()
      StorageServiceFactory.getStorageService(config)
    }
  }

  def getStorageService(storageType: String, storageKey: String, storageSecret: String): IStorageService = {
    if("local".equals(storageType)) {
      return null;
    }
    if (!storageContainers.contains(storageType + "|" + storageKey)) {
      storageContainers.put(storageType + "|" + storageKey, newStorageService(storageType, storageKey, storageSecret))
    }
    storageContainers(storageType + "|" + storageKey)
  }

  def setDruidClient(druidClient: DruidClient, druidRollupClient: DruidClient) {
    dc = druidClient;
    drc = druidRollupClient;
  }

  def getDruidClient(): DruidClient = {
    if (null == dc) {
      dc = DruidConfig.DefaultConfig.client;
    }
    return dc;
  }

  def getDruidRollUpClient(): DruidClient = {
    if (null == drc) {
      val conf = DruidConfig.DefaultConfig
      drc = DruidConfig.apply(
        Seq(QueryHost(AppConf.getConfig("druid.rollup.host"), AppConf.getConfig("druid.rollup.port").toInt)),
        conf.secure,
        conf.url,conf.healthEndpoint,conf.datasource,conf.responseParsingTimeout,conf.clientBackend,
        conf.clientConfig,conf.scanQueryLegacyMode,conf.zoneId,conf.system).client
    }
    return drc;
  }

  def getAkkaHttpUtil(): AkkaHttpClient = {
      AkkaHttpUtil
  }

  def shutdownDruidClient() = {
    if (dc != null) dc.actorSystem.terminate()
  }

  def shutdownDruidRollUpClien() = {
    if (drc != null) drc.actorSystem.terminate()
  }

  def shutdownStorageService() = {
    if (storageContainers.nonEmpty) {
      storageContainers.foreach(f => f._2.close());
    }
  }

  def closeContext() = {
    shutdownDruidClient();
    shutdownDruidRollUpClien();
    shutdownStorageService();
  }

  def loadData(spark: SparkSession, settings: scala.collection.Map[String, String], url: String, schema: StructType): DataFrame = {
    if (schema.nonEmpty) {
      spark.read.schema(schema).format(url).options(settings).load()
    }
    else {
      spark.read.format(url).options(settings).load()
    }
  }

}