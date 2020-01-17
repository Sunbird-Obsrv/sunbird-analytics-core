package org.ekstep.analytics.framework

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.JSONUtils
import org.apache.spark.HashPartitioner
import scala.collection.mutable.Buffer

case class SampleAlgoOut(id: String, events: Buffer[Event]) extends AlgoOutput with Output

object SampleModelTemplate extends IBatchModelTemplate[Event, Event, SampleAlgoOut, SampleAlgoOut] {

    override def preProcess(data: RDD[Event], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Event] = {
        DataFilter.filter(data, Filter("eid", "EQ", Option("GE_GENIE_START")));
    }

    override def algorithm(data: RDD[Event], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[SampleAlgoOut] = {
        val newGroupedEvents = data.map(event => (event.did, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b);
        newGroupedEvents.map { x => SampleAlgoOut(x._1, x._2) }
    }

    override def postProcess(data: RDD[SampleAlgoOut], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[SampleAlgoOut] = {
        data
    }
    
    override def name() : String = "SampleModelTemplate"

}