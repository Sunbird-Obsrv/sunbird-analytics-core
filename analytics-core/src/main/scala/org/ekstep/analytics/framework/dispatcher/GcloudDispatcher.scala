package org.ekstep.analytics.framework.dispatcher

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.{FrameworkContext, StorageConfig}
import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.util.CommonUtil

object GcloudDispatcher  extends HadoopDispatcher with IDispatcher {

  implicit val className = "org.ekstep.analytics.framework.dispatcher.GcloudDispatcher"

  override def dispatch(config: Map[String, AnyRef], events: RDD[String])(implicit sc: SparkContext, fc: FrameworkContext): Unit = {

    val bucket = config.getOrElse("bucket", null).asInstanceOf[String];
    val key = config.getOrElse("key", null).asInstanceOf[String];
    val isPublic = config.getOrElse("public", false).asInstanceOf[Boolean];

    if (null == bucket || null == key) {
      throw new DispatcherException("'bucket' & 'key' parameters are required to send output to GCloud")
    }

    val srcFile = CommonUtil.getGCloudFile(bucket, "_tmp/" + key);
    val destFile = CommonUtil.getGCloudFile(bucket, key);
    dispatchData(srcFile, destFile, sc.hadoopConfiguration, events)
  }

  override def dispatch(events: RDD[String], config: StorageConfig)(implicit sc: SparkContext, fc: FrameworkContext): Unit = {
    val bucket = config.container;
    val key = config.fileName;

    dispatch(Map[String, AnyRef]("bucket" -> bucket, "key" -> key), events);
  }

}
