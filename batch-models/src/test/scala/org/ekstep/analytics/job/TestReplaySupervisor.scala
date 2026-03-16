package org.ekstep.analytics.job

import org.ekstep.analytics.framework.exception.JobNotFoundException
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework._
import org.ekstep.analytics.model.BaseSpec

class TestReplaySupervisor extends BaseSpec {

  it should " Run WFS from local data " in {
    val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/replay-supervisor/test_data___endDate__*"))))), null, null, "org.ekstep.analytics.model.WorkflowSummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestReplaySupervisor"), Option(true))
    ReplaySupervisor.main("wfs", "2019-11-12", "2019-11-12", JSONUtils.serialize(config));

    the[JobNotFoundException] thrownBy {
      val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/replay-supervisor/__endDate__*"))))), None, None, "org.ekstep.analytics.model.LearnerActivitySummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), None, Option(false))
      ReplaySupervisor.main("abc", "2016-02-21", "2016-02-22", JSONUtils.serialize(config))
    } should have message "Unknown job type found"
  }

  it should "throw DataFetcherException" in {
    val config = JobConfig(Fetcher("s", None, Option(Array(Query(Option("prod-data-store"), Option("ss/"), None, Option("__endDate__"), Option(0))))), Option(Array(Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")))), None, "org.ekstep.analytics.model.ProficiencyUpdater", Option(Map("alpha" -> 1.0d.asInstanceOf[AnyRef], "beta" -> 1.0d.asInstanceOf[AnyRef])), Option(Array(Dispatcher("console", Map("printEvent" -> Option(false))), Dispatcher("kafka", Map("brokerList" -> "localhost:9092", "topic" -> "replay")))), Option(10), Option("TestReplaySupervisor"), Option(false))
    ReplaySupervisor.main("wfs", "2015-09-02", "2015-09-02", JSONUtils.serialize(config));

    implicit val sc = getSparkContext();
    implicit val fc = new FrameworkContext()
    ReplaySupervisor.execute("Test", "2016-02-21", "2016-02-22", null);
  }

  ignore should "throw Exception" in {
    val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/replay-supervisor/__endDate__*"))))), None, None, "org.ekstep.analytics.model.LearnerActivitySummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), None, Option(false))
    ReplaySupervisor.main("wfs", "2015-09-02", "2015-09-02", JSONUtils.serialize(config))
  }
}