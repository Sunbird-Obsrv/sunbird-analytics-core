package org.ekstep.analytics.job

import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.job.summarizer._
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.framework.exception.JobNotFoundException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

class TestJobFactory extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

    "JobFactory" should "return a Model class for a model code" in {
        val job = JobFactory.getJob("wfs")
        job should be(WorkFlowSummarizer)
        job.isInstanceOf[IJob] should be(true)
    }

    it should "return JobNotFoundException" in {

        the[JobNotFoundException] thrownBy {
            JobFactory.getJob("test-model")
        } should have message "Unknown job type found"
    }

}
