package org.ekstep.analytics.job.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils

class TestContentRatingUpdater extends SparkSpec(null) {

  "ContentRatingUpdater" should "execute the job and shouldn't throw any exception" in {
      val fromDate = "2019-05-11"
      val toDate = "2019-05-12"

      val config = JobConfig(Fetcher("none", None, None), null, null, "org.ekstep.analytics.job.ContentRatingUpdater", Option(Map("fromDate" ->  fromDate, "toDate" -> toDate)), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestContentRatingUpdater"), Option(false))
      ContentRatingUpdater.main(JSONUtils.serialize(config))(Option(sc))
  }

}