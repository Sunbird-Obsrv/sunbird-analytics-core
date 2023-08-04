package org.ekstep.analytics.framework.util
import org.apache.spark.SparkContext
import org.sunbird.cloud.storage.conf.AppConf

trait ICloudStorageProvider {
  def setConf(sc: SparkContext, storageKey: Option[String], storageSecret: Option[String]): Unit
}

object CloudStorageProviders {
  implicit val className: String = "org.ekstep.analytics.framework.util.CloudStorageProvider"
  private val providerMap: Map[String, Class[_ <: ICloudStorageProvider]] = Map("s3" -> classOf[S3Provider], "azure" -> classOf[AzureProvider], "gcp" -> classOf[GcpProvider], "oci" -> classOf[OCIProvider])
  def setSparkCSPConfigurations(sc: SparkContext, csp: String, storageKey: Option[String], storageSecret: Option[String]): Unit = {
    providerMap.get(csp.toLowerCase()).foreach { providerClass =>
      val providerConstructor = providerClass.getDeclaredConstructor()
      val providerInstance:ICloudStorageProvider = providerConstructor.newInstance()
      providerInstance.setConf(sc, storageKey, storageSecret)
    }
  }


}
class S3Provider extends ICloudStorageProvider {
  implicit val className: String = "org.ekstep.analytics.framework.util.S3Provider"
  override def setConf(sc: SparkContext, storageKey: Option[String], storageSecret: Option[String]): Unit = {
    JobLogger.log("Configuring S3 Access Key & Secret Key to SparkContext")
    val key = storageKey.filter(_.nonEmpty).map(AppConf.getConfig).getOrElse(AppConf.getAwsKey())
    val secret = storageSecret.filter(_.nonEmpty).map(AppConf.getConfig).getOrElse(AppConf.getAwsSecret())
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", key)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secret)
    val storageEndpoint = AppConf.getConfig("cloud_storage_endpoint")
    if (storageEndpoint.nonEmpty) {
      sc.hadoopConfiguration.set("fs.s3n.endpoint", storageEndpoint)
    }
  }
}

class AzureProvider extends ICloudStorageProvider {
  implicit val className: String = "org.ekstep.analytics.framework.util.AzureProvider"
  override def setConf(sc: SparkContext, storageKey: Option[String], storageSecret: Option[String]): Unit = {
    JobLogger.log("Configuring Azure Access Key & Secret Key to SparkContext")
    val key = storageKey.filter(_.nonEmpty).map(AppConf.getConfig).getOrElse(AppConf.getStorageKey("azure"))
    val secret = storageSecret.filter(_.nonEmpty).map(AppConf.getConfig).getOrElse(AppConf.getStorageSecret("azure"))
    sc.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    sc.hadoopConfiguration.set("fs.azure.account.key." + key + ".blob.core.windows.net", secret)
    sc.hadoopConfiguration.set("fs.azure.account.keyprovider." + key + ".blob.core.windows.net", "org.apache.hadoop.fs.azure.SimpleKeyProvider")
  }
}
class GcpProvider extends ICloudStorageProvider {
  implicit val className: String = "org.ekstep.analytics.framework.util.GcpProvider"
  override def setConf(sc: SparkContext, storageKey: Option[String], storageSecret: Option[String]): Unit = {
    JobLogger.log("Configuring GCP Access Key & Secret Key to SparkContext")
    val key = storageKey.filter(_.nonEmpty).map(AppConf.getConfig).getOrElse(AppConf.getStorageKey("gcloud"))
    val secret = storageSecret.filter(_.nonEmpty).map(AppConf.getConfig).getOrElse(AppConf.getStorageSecret("gcloud"))
    sc.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    sc.hadoopConfiguration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    sc.hadoopConfiguration.set("fs.gs.auth.service.account.email",  key)
    sc.hadoopConfiguration.set("fs.gs.auth.service.account.private.key", secret)
    sc.hadoopConfiguration.set("fs.gs.auth.service.account.private.key.id", AppConf.getConfig("gcloud_private_secret_id"))
  }
}

class OCIProvider extends ICloudStorageProvider {
  implicit val className: String = "org.ekstep.analytics.framework.util.OCIProvider"
  override def setConf(sc: SparkContext, storageKey: Option[String], storageSecret: Option[String]): Unit = {
    val key = storageKey.filter(_.nonEmpty).map(AppConf.getConfig).getOrElse(AppConf.getStorageKey("oci"))
    val secret = storageSecret.filter(_.nonEmpty).map(AppConf.getConfig).getOrElse(AppConf.getStorageSecret("oci"))
    JobLogger.log("Configuring OCI Access Key & Secret Key to SparkContext")
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", key);
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secret);
  }
}