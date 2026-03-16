package org.ekstep.analytics.job

import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.job.summarizer._
import org.ekstep.analytics.job.updater._
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.framework.exception.JobNotFoundException
import org.ekstep.analytics.util.EmbeddedPostgresql
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterAll

class TestJobFactory extends FlatSpec with Matchers with BeforeAndAfterAll {

    "JobFactory" should "return a Model class for a model code" in {
        val jobIds = List("monitor-job-summ", "wfs", "telemetry-replay", "summary-replay", "content-rating-updater", "experiment", "audit-metrics-report", "druid_reports")

        val jobs = jobIds.map { f => JobFactory.getJob(f) }

        jobs(1) should be(WorkFlowSummarizer)
        jobs(1).isInstanceOf[IJob] should be(true)

        jobs(5) should be(ExperimentDefinitionJob)
        jobs(5).isInstanceOf[IJob] should be(true)

    }

    it should "return JobNotFoundException" in {

        the[JobNotFoundException] thrownBy {
            JobFactory.getJob("test-model")
        } should have message "Unknown job type found"
    }

}
