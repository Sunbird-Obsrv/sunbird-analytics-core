package org.ekstep.analytics.framework.util
import org.apache.spark.SparkContext
import org.sunbird.cloud.storage.conf.AppConf

trait ICloudStorageProvider {
  def setConf(sc: SparkContext, storageKey: Option[String], storageSecret: Option[String]): Unit
}



object CloudStorageProviders {
  implicit val className: String = "org.ekstep.analytics.framework.util.CloudStorageProvider"
  private val providerMap: Map[String, ICloudStorageProvider] = Map("s3" -> S3Provider, "azure" -> AzureProvider, "gcp" -> GcpProvider, "oci" -> OCIProvider)
  def setSparkCSPConfigurations(sc: SparkContext, csp: String, storageKey: Option[String], storageSecret: Option[String]): Unit = {
    providerMap.get(csp.toLowerCase()).foreach { provider =>
      provider.setConf(sc, storageKey, storageSecret)
    }
  }
}
object S3Provider extends ICloudStorageProvider {
  implicit val className: String = "org.ekstep.analytics.framework.util.S3Provider"
  override def setConf(sc: SparkContext, storageKey: Option[String], storageSecret: Option[String]): Unit = {
    JobLogger.log("Configuring S3 Access Key & Secret Key to SparkContext")
    val key = storageKey.filter(_.nonEmpty).map(value => AppConf.getConfig(value)).getOrElse(AppConf.getAwsKey())
    val secret = storageSecret.filter(_.nonEmpty).map(value => AppConf.getConfig(value)).getOrElse(AppConf.getAwsSecret())
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", key)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secret)
    val storageEndpoint = AppConf.getConfig("cloud_storage_endpoint")
    if (storageEndpoint.nonEmpty) {
      sc.hadoopConfiguration.set("fs.s3n.endpoint", storageEndpoint)
    }
  }
}

object AzureProvider extends ICloudStorageProvider {
  implicit val className: String = "org.ekstep.analytics.framework.util.AzureProvider"
  override def setConf(sc: SparkContext, storageKey: Option[String], storageSecret: Option[String]): Unit = {
    JobLogger.log("Configuring Azure Access Key & Secret Key to SparkContext")
    val key = storageKey.filter(_.nonEmpty).map(value => AppConf.getConfig(value)).getOrElse(AppConf.getStorageKey("azure"))
    val secret = storageSecret.filter(_.nonEmpty).map(value => AppConf.getConfig(value)).getOrElse(AppConf.getStorageSecret("azure"))
    sc.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    sc.hadoopConfiguration.set("fs.azure.account.key." + key + ".blob.core.windows.net", secret)
    sc.hadoopConfiguration.set("fs.azure.account.keyprovider." + key + ".blob.core.windows.net", "org.apache.hadoop.fs.azure.SimpleKeyProvider")
  }
}
object GcpProvider extends ICloudStorageProvider {
  implicit val className: String = "org.ekstep.analytics.framework.util.GcpProvider"
  override def setConf(sc: SparkContext, storageKey: Option[String], storageSecret: Option[String]): Unit = {
    JobLogger.log("Configuring GCP Access Key & Secret Key to SparkContext")
    val key = storageKey.filter(_.nonEmpty).map(value => AppConf.getConfig(value)).getOrElse(AppConf.getStorageKey("gcloud"))
    val secret = storageSecret.filter(_.nonEmpty).map(value => AppConf.getConfig(value)).getOrElse(AppConf.getStorageSecret("gcloud"))
    sc.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    sc.hadoopConfiguration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    sc.hadoopConfiguration.set("fs.gs.auth.service.account.email",  key)
    sc.hadoopConfiguration.set("fs.gs.auth.service.account.private.key", secret)
    sc.hadoopConfiguration.set("fs.gs.auth.service.account.private.key.id", AppConf.getConfig("gcloud_private_secret_id"))
  }
}

object OCIProvider extends ICloudStorageProvider {
  implicit val className: String = "org.ekstep.analytics.framework.util.OCIProvider"
  override def setConf(sc: SparkContext, storageKey: Option[String], storageSecret: Option[String]): Unit = {
    val key = storageKey.filter(_.nonEmpty).map(value => AppConf.getConfig(value)).getOrElse(AppConf.getStorageKey("oci"))
    val secret = storageSecret.filter(_.nonEmpty).map(value => AppConf.getConfig(value)).getOrElse(AppConf.getStorageSecret("oci"))
    JobLogger.log("Configuring OCI Access Key & Secret Key to SparkContext")
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", key);
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secret);
    val storageEndpoint = AppConf.getConfig("cloud_storage_endpoint_with_protocol")
    if (storageEndpoint.nonEmpty) {
      sc.hadoopConfiguration.set("fs.s3n.endpoint", storageEndpoint)
    }
  }
}