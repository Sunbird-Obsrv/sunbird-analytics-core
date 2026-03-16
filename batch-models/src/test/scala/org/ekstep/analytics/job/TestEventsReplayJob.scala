package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.CommonUtil
import org.scalatest.{FlatSpec, Matchers}

class TestEventsReplayJob extends FlatSpec with Matchers {

  val config = """{"search":{"type":"local","queries":[{"file":"src/test/resources/telemetry-replay/data.json"}]},"model":"org.ekstep.analytics.job.EventsReplayJob","modelParams":{},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"deviceMapping":true}"""

  "EventsReplayJob" should "Read and dispatch data properly" in {
    EventsReplayJob.main(config)(None)
  }

  it should "read and dispatch data" in {

    implicit val sc = CommonUtil.getSparkContext(10, "TestReplay")
    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    val input = EventsReplayJob.getInputData(jobConfig)
    input.count() should be(37)

    val output = EventsReplayJob.dispatchData(jobConfig, input)
    output should be(0)

    EventsReplayJob.main(config)(Option(sc))

  }

}
