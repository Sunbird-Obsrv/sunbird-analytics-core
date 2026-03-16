package org.ekstep.analytics.job.updater

import org.ekstep.analytics.framework.JobDriver
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.updater.UpdateContentRating
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.framework.FrameworkContext

// object ContentRatingUpdater extends optional.Application with IJob {
object ContentRatingUpdater extends optional.Application with IJob {

  implicit val className = "org.ekstep.analytics.job.ContentRatingUpdater"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    JobLogger.log("Started executing Job")
    implicit val sparkContext: SparkContext = sc.orNull
    JobDriver.run("batch", config, UpdateContentRating);
    JobLogger.log("Job Completed.")
  }
}