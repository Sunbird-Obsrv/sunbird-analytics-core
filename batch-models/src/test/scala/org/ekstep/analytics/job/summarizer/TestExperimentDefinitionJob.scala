package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.framework.{Dispatcher, Fetcher, JobConfig, Query}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.SparkSpec

class TestExperimentDefinitionJob extends SparkSpec(null) {

    "ExperimentDefinitionJob" should "execute ExperimentDefinitionJob job and won't throw any Exception" in {

        val config = JobConfig(Fetcher("none", None, None), null, null, "org.ekstep.analytics.model.ExperimentDefinitionModel", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestExperimentDefinitionJob"))
        ExperimentDefinitionJob.main(JSONUtils.serialize(config))(Option(sc))
    }
}