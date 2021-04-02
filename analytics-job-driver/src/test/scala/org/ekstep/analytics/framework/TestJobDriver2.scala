package org.ekstep.analytics.framework

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterAll
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

case class Dummy(event:String) extends AlgoInput with AlgoOutput with Output
object TestModel4 extends IBatchModelTemplate[Event, Dummy, Dummy, Dummy] with Serializable {

    override def preProcess(events: RDD[Event], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Dummy] = {
      events.map { x => Dummy(JSONUtils.serialize(x)) };
    }
    
    override def algorithm(events: RDD[Dummy], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Dummy] = {
      events
    }
    
    override def postProcess(events: RDD[Dummy], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Dummy] = {
      events
    }

    override def name: String = "TestModel4";
}

class TestJobDriver2 extends FlatSpec with Matchers with BeforeAndAfterAll {
  
  it should "run the batch job driver on model implementing BatchModelTemplate" in {

        val jobConfig = JobConfig(
            Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry.log"))))),
            Option(Array[Filter](Filter("eventId", "EQ", Option("OE_START")))),
            None,
            "org.ekstep.analytics.framework.TestModel2",
            Option(Map()),
            Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))),
            Option(8),
            None,
            None)

        implicit val sc = Option(CommonUtil.getSparkContext(1, "Test"));
        implicit val fc:Option[FrameworkContext] = None;
        JobDriver.run("batch", JSONUtils.serialize(jobConfig), TestModel4);
        CommonUtil.closeSparkContext()(sc.get);
    }
}