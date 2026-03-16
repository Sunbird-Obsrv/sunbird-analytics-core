package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils

class TestWorkFlowSummarizer extends SparkSpec(null) {
  
    "WorkFlowSummarizer" should "execute WorkFlowSummarizer job and won't throw any Exception" in {

        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/workflow-summary/test-data1.log"))))), null, null, "org.ekstep.analytics.model.WorkFlowSummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestWorkFlowSummarizer"), Option(true))
        WorkFlowSummarizer.main(JSONUtils.serialize(config))(Option(sc))
    }
}