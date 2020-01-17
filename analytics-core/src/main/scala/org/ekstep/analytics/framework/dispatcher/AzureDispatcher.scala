package org.ekstep.analytics.framework.dispatcher

import java.io.FileWriter
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.util.{CommonUtil, JobLogger}
import org.sunbird.cloud.storage.conf.AppConf
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}
import org.ekstep.analytics.framework.Level
import scala.concurrent.Await
import org.ekstep.analytics.framework.FrameworkContext

object AzureDispatcher extends IDispatcher {

    implicit val className = "org.ekstep.analytics.framework.dispatcher.AzureDispatcher"

    @throws(classOf[DispatcherException])
    def dispatch(events: Array[String], config: Map[String, AnyRef])(implicit fc: FrameworkContext): Array[String] = {
        var filePath = config.getOrElse("filePath", null).asInstanceOf[String];
        val bucket = config.getOrElse("bucket", null).asInstanceOf[String];
        val key = config.getOrElse("key", null).asInstanceOf[String];
        val zip = config.getOrElse("zip", false).asInstanceOf[Boolean];
        val isPublic = config.getOrElse("public", false).asInstanceOf[Boolean];

        if (null == bucket || null == key) {
            throw new DispatcherException("'bucket' & 'key' parameters are required to send output to azure")
        }
        var deleteFile = false;
        if (null == filePath) {
            filePath = AppConf.getConfig("spark_output_temp_dir") + "output-" + System.currentTimeMillis() + ".log";
            val fw = new FileWriter(filePath, true);
            events.foreach { x => { fw.write(x + "\n"); } };
            fw.close();
            deleteFile = true;
        }
        val finalPath = if (zip) CommonUtil.gzip(filePath) else filePath;
        val storageService = fc.getStorageService("azure");
        storageService.upload(bucket, finalPath, key, Option(isPublic), None, None, None);
        storageService.closeContext();
        if (deleteFile) CommonUtil.deleteFile(filePath);
        if (zip) CommonUtil.deleteFile(finalPath);
        events;
    }

    def dispatch(config: Map[String, AnyRef], events: RDD[String])(implicit sc: SparkContext, fc: FrameworkContext) = {

//        dispatch(events.collect(), config);
        val bucket = config.getOrElse("bucket", null).asInstanceOf[String];
        val key = config.getOrElse("key", null).asInstanceOf[String];
        val isPublic = config.getOrElse("public", false).asInstanceOf[Boolean];

        if (null == bucket || null == key) {
            throw new DispatcherException("'bucket' & 'key' parameters are required to send output to azure")
        }
        events.saveAsTextFile("wasb://" + bucket + "@" + AppConf.getStorageKey(AppConf.getStorageType()) + ".blob.core.windows.net/" + key);
    }

    def dispatchDirectory(config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext) = {
        val dirPath = config.getOrElse("dirPath", null).asInstanceOf[String]
        val bucket = config.getOrElse("bucket", null).asInstanceOf[String]
        val key = config.getOrElse("key", null).asInstanceOf[String]
        val isPublic = config.getOrElse("public", false).asInstanceOf[Boolean]

        if (null == bucket || null == key || dirPath == null) {
            throw new DispatcherException("'local file path', 'bucket' & 'key' parameters are required to upload directory to azure")
        }

        val storageService = fc.getStorageService("azure");
        val uploadMsg = storageService.upload(bucket, dirPath, key, Option(true), Option(1), Option(3), None)
        storageService.closeContext();
        JobLogger.log("Successfully Uploaded files", Option(Map("filesUploaded" -> "")), Level.INFO)
        CommonUtil.deleteDirectory(dirPath)
    }


}
