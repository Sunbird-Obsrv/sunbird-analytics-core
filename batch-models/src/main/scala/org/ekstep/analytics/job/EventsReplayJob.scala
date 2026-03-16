package org.ekstep.analytics.job

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}

import scala.collection.Map

object EventsReplayJob extends optional.Application with IJob {

  implicit val className = "org.ekstep.analytics.job.EventsReplayJob"
  implicit val fc = new FrameworkContext()

  def name(): String = "EventsReplayJob"

  override def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None): Unit = {

    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val sparkContext = if (sc.isEmpty) CommonUtil.getSparkContext(JobContext.parallelization, jobConfig.appName.getOrElse(jobConfig.model)) else sc.get
    val jobName = jobConfig.appName.getOrElse(name)
    JobLogger.init(jobName)
    JobLogger.start(jobName + " Started executing", Option(Map("config" -> config, "model" -> name)))
    val totalEvents = process(jobConfig)
    JobLogger.end(jobName + " Completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name, "outputEvents" -> totalEvents)))
    CommonUtil.closeSparkContext()
  }

  def getInputData(config: JobConfig)(implicit mf: Manifest[String], sc: SparkContext): RDD[String] = {

    fc.inputEventsCount = sc.longAccumulator("InputEventsCount");
    DataFetcher.fetchBatchData[String](config.search).cache()
  }

  def dispatchData(jobConfig: JobConfig, data: RDD[String])(implicit sc: SparkContext): Long = {
    OutputDispatcher.dispatch(jobConfig.output, data)
  }

  def process(jobConfig: JobConfig)(implicit sc: SparkContext): Long = {

    val data = getInputData(jobConfig)
    dispatchData(jobConfig, data)
  }

}