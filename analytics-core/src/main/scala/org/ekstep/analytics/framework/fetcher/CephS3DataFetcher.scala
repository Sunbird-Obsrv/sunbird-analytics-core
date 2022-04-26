package org.ekstep.analytics.framework.fetcher

import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.{FrameworkContext, Query}
import org.ekstep.analytics.framework.exception.DataFetcherException
import org.ekstep.analytics.framework.storage.CustomS3StorageService
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}

/**
 * @author Santhosh
 */
object CephS3DataFetcher {

    @throws(classOf[DataFetcherException])
    def getObjectKeys(queries: Array[Query])(implicit fc: FrameworkContext): Array[String] = {

        val keys = for(query <- queries) yield {
            val paths = if(query.folder.isDefined && query.endDate.isDefined && query.folder.getOrElse("false").equals("true")) {
                Array("s3n://" + getBucket(query.bucket) + "/" + getPrefix(query.prefix) + query.endDate.get)
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
        val storageService = fc.getStorageService("s3", AppConf.getConfig("storage.key.config"), AppConf.getConfig("storage.secret.config"))
        val keys = storageService.searchObjects(getBucket(query.bucket), getPrefix(query.prefix), query.startDate, query.endDate, query.delta, query.datePattern.getOrElse("yyyy-MM-dd"))
        storageService.getPaths(getBucket(query.bucket), keys).toArray
    }

    private def getBucket(bucket: Option[String]) : String = {
        bucket.getOrElse("telemetry-data-store");
    }

    private def getPrefix(prefix: Option[String]) : String = {
        prefix.getOrElse("raw/");
    }

}