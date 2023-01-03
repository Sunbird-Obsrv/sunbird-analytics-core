package org.ekstep.analytics.framework.dispatcher

import org.ekstep.analytics.framework.exception.DispatcherException
import java.io.FileWriter
import org.ekstep.analytics.framework.OutputDispatcher
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.commons.lang3.StringUtils
import java.nio.file.Files
import java.nio.file.Paths
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.FrameworkContext
import java.io.File
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileUtil
import org.apache.commons.io.FileUtils
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.StorageConfig

/**
 * @author Santhosh
 */
object FileDispatcher extends HadoopDispatcher with IDispatcher {

  implicit val className = "org.ekstep.analytics.framework.dispatcher.FileDispatcher";

  override def dispatch(config: Map[String, AnyRef], events: RDD[String])(implicit sc: SparkContext, fc: FrameworkContext): Unit = {
    val filePath = config.getOrElse("file", null).asInstanceOf[String];
    if (null == filePath) {
      throw new DispatcherException("'file' parameter is required to send output to file");
    }

    val path = new File(filePath);
    val index = path.getPath.lastIndexOf(path.getName);
    val prefix = path.getPath.substring(0, index)

    dispatchData(prefix + "_tmp/" + path.getName, filePath, sc.hadoopConfiguration, events)
  }

  override def dispatch(events: RDD[String], config: StorageConfig)(implicit sc: SparkContext, fc: FrameworkContext): Unit = {
    val file = config.fileName;
    dispatch(Map[String, AnyRef]("file" -> file), events);
  }

}