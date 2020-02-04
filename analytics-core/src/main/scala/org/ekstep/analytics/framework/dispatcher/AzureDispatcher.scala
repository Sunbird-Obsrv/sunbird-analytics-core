package org.ekstep.analytics.framework.dispatcher

import java.io.FileWriter
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.util.{ CommonUtil, JobLogger }
import org.sunbird.cloud.storage.conf.AppConf
import org.sunbird.cloud.storage.factory.{ StorageServiceFactory }
import org.ekstep.analytics.framework.Level
import scala.concurrent.Await
import org.ekstep.analytics.framework.FrameworkContext
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.fs.Path
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.StorageConfig

object AzureDispatcher extends HadoopDispatcher with IDispatcher {

  implicit val className = "org.ekstep.analytics.framework.dispatcher.AzureDispatcher"

  override def dispatch(config: Map[String, AnyRef], events: RDD[String])(implicit sc: SparkContext, fc: FrameworkContext) = {

    val bucket = config.getOrElse("bucket", null).asInstanceOf[String];
    val key = config.getOrElse("key", null).asInstanceOf[String];

    if (null == bucket || null == key) {
      throw new DispatcherException("'bucket' & 'key' parameters are required to send output to azure")
    }

    val srcFile = CommonUtil.getAzureFile(bucket, "_tmp/" + key);
    val destFile = CommonUtil.getAzureFile(bucket, key);

    dispatchData(srcFile, destFile, sc.hadoopConfiguration, events)
  }

  override def dispatch(events: RDD[String], config: StorageConfig)(implicit sc: SparkContext, fc: FrameworkContext) = {
    val bucket = config.container;
    val key = config.fileName;

    if (null == bucket || null == key || bucket.isEmpty() || key.isEmpty()) {
      throw new DispatcherException("'bucket' & 'key' parameters are required to send output to azure")
    }

    val srcFile = CommonUtil.getAzureFile(bucket, "_tmp/" + key, config.accountKey.getOrElse("azure_storage_key"));
    val destFile = CommonUtil.getAzureFile(bucket, key, config.accountKey.getOrElse("azure_storage_key"));

    dispatchData(srcFile, destFile, sc.hadoopConfiguration, events)
  }

}
