package org.ekstep.analytics.framework

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.JSONUtils

/**
 * @author Santhosh
 */
class TestModel extends IBatchModel[Event, String] with Serializable {

    def execute(events: RDD[Event], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext, fc: FrameworkContext): RDD[String] = {
        events.map { x => JSONUtils.serialize(x) };
    }

}