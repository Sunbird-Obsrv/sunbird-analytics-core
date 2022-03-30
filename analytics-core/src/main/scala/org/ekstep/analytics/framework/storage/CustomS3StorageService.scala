package org.ekstep.analytics.framework.storage

import org.jclouds.ContextBuilder
import org.jclouds.blobstore.{BlobStore, BlobStoreContext}
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.Model.Blob
import org.sunbird.cloud.storage.factory.StorageConfig


class CustomS3StorageService(config: StorageConfig) extends BaseStorageService {
  var context: BlobStoreContext = ContextBuilder.newBuilder("s3").endpoint(config.endPoint.get).credentials(config.storageKey, config.storageSecret).buildView(classOf[BlobStoreContext])
  var blobStore: BlobStore = context.getBlobStore

  override def getPaths(container: String, objects: List[Blob]): List[String] = {
    objects.map{f => "s3n://" + container + "/" + f.key}
  }
}