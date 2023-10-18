package org.ekstep.analytics.framework.storage

import org.jclouds.ContextBuilder
import org.jclouds.blobstore.{BlobStore, BlobStoreContext}
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.Model.Blob
import org.sunbird.cloud.storage.factory.StorageConfig
import java.util.Properties


class CustomOCIStorageService(config: StorageConfig) extends BaseStorageService {
  private val overrides = new Properties()

  overrides.setProperty("jclouds.provider", "s3")
  overrides.setProperty("jclouds.endpoint", config.endPoint.get)
  overrides.setProperty("jclouds.s3.virtual-host-buckets", "false")
  overrides.setProperty("jclouds.strip-expect-header", "true")
  overrides.setProperty("jclouds.regions", config.region.get)
  overrides.setProperty("jclouds.s3.signer-version", "4")
  // var context: BlobStoreContext = ContextBuilder.newBuilder("aws-s3").credentials(config.storageKey, config.storageSecret).overrides(overrides).endpoint(config.endPoint.get).buildView(classOf[BlobStoreContext])

  var context = ContextBuilder.newBuilder("aws-s3")
    .credentials(config.storageKey, config.storageSecret)
    .overrides(overrides)
    .endpoint(config.endPoint.get).buildView(classOf[BlobStoreContext])

  var blobStore: BlobStore = context.getBlobStore

  override def getPaths(container: String, objects: List[Blob]): List[String] = {
    objects.map{f => "s3n://" + container + "/" + f.key}
  }
}