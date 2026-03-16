package org.ekstep.analytics.job.summarizer
/**
 * @author Yuva
 */

import org.ekstep.analytics.framework.JobDriver
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.model.MonitorSummaryModel
import org.ekstep.analytics.framework.FrameworkContext

object MonitorSummarizer extends optional.Application with IJob {
    implicit val className = "org.ekstep.analytics.job.MonitorSummarizer"

    def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
        implicit val sparkContext: SparkContext = sc.orNull
        JobLogger.log("Started executing Job")
        JobDriver.run("batch", config, MonitorSummaryModel);
        JobLogger.log("Job Completed.")
    }
}