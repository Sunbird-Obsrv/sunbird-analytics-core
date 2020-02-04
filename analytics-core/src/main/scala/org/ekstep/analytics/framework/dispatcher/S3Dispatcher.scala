package org.ekstep.analytics.framework.dispatcher

import org.ekstep.analytics.framework.exception.DispatcherException
import java.io.FileWriter
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.Level._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.sunbird.cloud.storage.factory.StorageServiceFactory
import org.sunbird.cloud.storage.conf.AppConf
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.StorageConfig

/**
 * @author Santhosh
 */
object S3Dispatcher extends HadoopDispatcher with IDispatcher {

  implicit val className = "org.ekstep.analytics.framework.dispatcher.S3Dispatcher"

  override def dispatch(config: Map[String, AnyRef], events: RDD[String])(implicit sc: SparkContext, fc: FrameworkContext): Unit = {

    val bucket = config.getOrElse("bucket", null).asInstanceOf[String];
    val key = config.getOrElse("key", null).asInstanceOf[String];
    val isPublic = config.getOrElse("public", false).asInstanceOf[Boolean];

    if (null == bucket || null == key) {
      throw new DispatcherException("'bucket' & 'key' parameters are required to send output to S3")
    }

    val srcFile = CommonUtil.getS3File(bucket, "_tmp/" + key);
    val destFile = CommonUtil.getS3File(bucket, key);
    dispatchData(srcFile, destFile, sc.hadoopConfiguration, events)
  }

  override def dispatch(events: RDD[String], config: StorageConfig)(implicit sc: SparkContext, fc: FrameworkContext): Unit = {
    val bucket = config.container;
    val key = config.fileName;

    dispatch(Map[String, AnyRef]("bucket" -> bucket, "key" -> key), events);
  }

}