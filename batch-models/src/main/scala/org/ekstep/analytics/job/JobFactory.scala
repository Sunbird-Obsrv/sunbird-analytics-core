package org.ekstep.analytics.job

import scala.reflect.runtime.universe
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.framework.exception.JobNotFoundException
import org.ekstep.analytics.job.metrics.MetricsAuditJob
import org.ekstep.analytics.job.summarizer.{DruidQueryProcessor, ExperimentDefinitionJob, MonitorSummarizer, WorkFlowSummarizer}
import org.ekstep.analytics.job.updater.ContentRatingUpdater
import scala.reflect.runtime.universe

/**
 * @author Santhosh
 */

object JobFactory {
  @throws(classOf[JobNotFoundException])
  def getJob(jobType: String): IJob = {
    jobType.toLowerCase() match {
      case "monitor-job-summ" =>
        MonitorSummarizer
      case "wfs" =>
        WorkFlowSummarizer
      case "telemetry-replay" =>
        EventsReplayJob
      case "summary-replay" =>
        EventsReplayJob
      case "content-rating-updater" =>
        ContentRatingUpdater
      case "experiment" =>
        ExperimentDefinitionJob
      case "audit-metrics-report" =>
        MetricsAuditJob
      case "druid_reports" =>
        DruidQueryProcessor
      case _ =>
        reflectModule(jobType);
    }
  }

  def reflectModule(jobClass: String): IJob = {
    try {
      val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
      val module = runtimeMirror.staticModule(jobClass)
      val obj = runtimeMirror.reflectModule(module)
      obj.instance.asInstanceOf[IJob]
    } catch {
      case ex: Exception =>
        throw new JobNotFoundException("Unknown job type found")
    }
  }

}
