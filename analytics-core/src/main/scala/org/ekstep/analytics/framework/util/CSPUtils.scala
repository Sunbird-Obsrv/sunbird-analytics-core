package org.ekstep.analytics.framework.util
import org.apache.spark.SparkContext
import org.sunbird.cloud.storage.conf.AppConf

trait ICloudStorageProvider {
  def setConf(sc: SparkContext): Unit
}

object CloudStorageProviders {
  implicit val className: String = "org.ekstep.analytics.framework.util.CloudStorageProvider"
  private val providerMap: Map[String, Class[_ <: ICloudStorageProvider]] = Map("S3" -> classOf[S3Provider], "azure" -> classOf[AzureProvider], "gcp" -> classOf[GcpProvider])
  def setSparkCSPConfigurations(sc: SparkContext, csp: String): Unit = {
    providerMap.get(csp.toLowerCase()).foreach { providerClass =>
      val providerConstructor = providerClass.getDeclaredConstructor()
      val providerInstance:ICloudStorageProvider = providerConstructor.newInstance()
      providerInstance.setConf(sc)
    }
  }
}
class S3Provider extends ICloudStorageProvider {
  override def setConf(sc: SparkContext): Unit = {
    implicit val className: String = "org.ekstep.analytics.framework.util.S3Provider"
    JobLogger.log("Configuring S3 AccessKey& SecrateKey to SparkContext")
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", AppConf.getAwsKey())
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", AppConf.getAwsSecret())
    val storageEndpoint = AppConf.getConfig("cloud_storage_endpoint")
    if (storageEndpoint.nonEmpty) {
      sc.hadoopConfiguration.set("fs.s3n.endpoint", storageEndpoint)
    }
  }
}

class AzureProvider extends ICloudStorageProvider {
  override def setConf(sc: SparkContext): Unit = {
    val accName = AppConf.getStorageKey("azure")
    val accKey = AppConf.getStorageSecret("azure")
    sc.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    sc.hadoopConfiguration.set("fs.azure.account.key." + accName + ".blob.core.windows.net", accKey)
    sc.hadoopConfiguration.set("fs.azure.account.keyprovider." + accName + ".blob.core.windows.net", "org.apache.hadoop.fs.azure.SimpleKeyProvider")
  }
}
class GcpProvider extends ICloudStorageProvider {
  override def setConf(sc: SparkContext): Unit = {
    sc.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    sc.hadoopConfiguration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    sc.hadoopConfiguration.set("fs.gs.auth.service.account.email", AppConf.getStorageKey("gcloud"))
    sc.hadoopConfiguration.set("fs.gs.auth.service.account.private.key", AppConf.getStorageSecret("gcloud"))
    sc.hadoopConfiguration.set("fs.gs.auth.service.account.private.key.id", AppConf.getConfig("gcloud_private_secret_id"))
  }
}
