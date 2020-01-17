package org.ekstep.analytics.framework

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.JSONUtils

object SampleModel extends IBatchModel[Event,String] {
  
  def execute(data: RDD[Event], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext, fc: FrameworkContext): RDD[String] = {
        val events = DataFilter.filter(data, Filter("eventId","IN",Option(List("OE_START","OE_END"))));
        
        events.map{f => JSONUtils.serialize(f)}
    }
}