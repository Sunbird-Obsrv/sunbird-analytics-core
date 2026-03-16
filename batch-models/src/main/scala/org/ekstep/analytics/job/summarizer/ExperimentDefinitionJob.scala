package org.ekstep.analytics.job.summarizer

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{IJob, JobDriver}
import org.ekstep.analytics.model.ExperimentDefinitionModel
import org.ekstep.analytics.framework.FrameworkContext

object ExperimentDefinitionJob extends optional.Application with IJob {

    implicit val className = "org.ekstep.analytics.job.ExperimentDefinitionJob"

    def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
        implicit val sparkContext: SparkContext = sc.orNull
        JobLogger.log("Started executing Job")
        JobDriver.run("batch", config, ExperimentDefinitionModel);
        JobLogger.log("Job Completed.")

    }
}