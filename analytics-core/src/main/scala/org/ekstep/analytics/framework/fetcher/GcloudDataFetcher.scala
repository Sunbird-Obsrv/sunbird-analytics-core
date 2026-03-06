package org.ekstep.analytics.framework.fetcher

import org.ekstep.analytics.framework.{FrameworkContext, Query}
import org.ekstep.analytics.framework.exception.DataFetcherException
import scala.collection.JavaConverters._

object GcloudDataFetcher {

  @throws(classOf[DataFetcherException])
  def getObjectKeys(queries: Array[Query])(implicit fc: FrameworkContext): Array[String] = {

    val keys = for(query <- queries) yield {
      val paths = if(query.folder.isDefined && query.endDate.isDefined && query.folder.getOrElse("false").equals("true")) {
        Array("gs://"+getBucket(query.bucket) + "/" + getPrefix(query.prefix) + query.endDate.getOrElse(""))
      } else {
        getKeys(query);
      }
      if(query.excludePrefix.isDefined) {
        paths.filter { x => !x.contains(query.excludePrefix.get) }
      } else {
        paths
      }
    }
    keys.flatMap { x => x.map { x => x } }
  }

  private def getKeys(query: Query)(implicit fc: FrameworkContext) : Array[String] = {
    val storageService = fc.getStorageService("gcloud", "gcloud_client_key", "gcloud_private_secret");
    val keys = storageService.searchObjects(getBucket(query.bucket), getPrefix(query.prefix), query.startDate.orNull, query.endDate.orNull, query.delta.map(Integer.valueOf(_)).orNull, query.datePattern.getOrElse("yyyy-MM-dd"))
    storageService.getPaths(getBucket(query.bucket), keys).asScala.toArray
  }

  private def getBucket(bucket: Option[String]) : String = {
    bucket.getOrElse("telemetry-data-store");
  }

  private def getPrefix(prefix: Option[String]) : String = {
    prefix.getOrElse("raw/");
  }
}