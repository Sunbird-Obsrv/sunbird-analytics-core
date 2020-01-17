package org.ekstep.analytics.framework

import org.scalatest._
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.conf.AppConf
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.apache.spark.rdd.RDD

/**
 * @author Santhosh
 */

object TestModel2 extends IBatchModel[MeasuredEvent, String] with Serializable {

    def execute(events: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext, fc: FrameworkContext): RDD[String] = {
        events.map { x => JSONUtils.serialize(x) };
    }

    override def name: String = "TestModel2";
}

object TestModel3 extends IBatchModel[MeasuredEvent, String] with Serializable {

    def execute(events: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext, fc: FrameworkContext): RDD[String] = {
        val contents = events.map { x => x.content_id.getOrElse("") }
        contents;
    }

    override def name: String = "TestModel3";
}

class TestJobDriver extends FlatSpec with Matchers with BeforeAndAfterAll {

    implicit val sc: Option[SparkContext] = None;
    implicit var fc: Option[FrameworkContext] = _;
    
    override def beforeAll() {
      fc = Option(new FrameworkContext());
    }
    
    override def afterAll() {
      fc.get.closeContext();
    }
            
    "TestJobDriver" should "successfully test the job driver" in {

        val jobConfig = JobConfig(
            Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry.log"))))),
            Option(Array[Filter](Filter("eventId", "IN", Option(Array("OE_ASSESS", "OE_START", "OE_END", "OE_LEVEL_SET"))))),
            None,
            "org.ekstep.analytics.framework.TestModel",
            Option(Map()),
            Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))),
            Option(8),
            None,
            Option(true))

        noException should be thrownBy {
            val baos = new ByteArrayOutputStream
            val ps = new PrintStream(baos)
            Console.setOut(ps);
            JobDriver.run[Event, String]("batch", JSONUtils.serialize(jobConfig), new TestModel);
            baos.toString should include("(Total Events Size,1699)");
            baos.close()
        }
    }

    it should "run batch job driver fetching empty rdd" in {
        val jobConfig = JobConfig(
            Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/test_1.log"))))),
            Option(Array(Filter("eventId", "EQ", Option("GE_TRANSFER")))),
            None,
            "org.ekstep.analytics.framework.TestModel",
            Option(Map()),
            Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))),
            Option(8),
            None,
            Option(true))

        JobDriver.run("batch", JSONUtils.serialize(jobConfig), new TestModel);
    }
    
    it should "check batch job driver handling exception" in {
        val jobConfig = JobConfig(
            Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/test_1.log"))))),
            Option(Array(Filter("eventId", "EQ", Option("GE_TRANSFER")))),
            None,
            "org.ekstep.analytics.framework.TestModel",
            Option(Map()),
            Option(Array(Dispatcher("cons", Map("printEvent" -> false.asInstanceOf[AnyRef])))),
            Option(8),
            None,
            Option(true))

        JobDriver.run("batch", JSONUtils.serialize(jobConfig), new TestModel);
    }

    it should "invoke stream job driver" in {
        val jobConfig = JobConfig(Fetcher("stream", None, None), None, None, "", None, None, None, None)
        JobDriver.run("streaming", JSONUtils.serialize(jobConfig), new TestModel);
    }

    it should "thrown an exception if unknown job type is found" in {
        val jobConfig = JobConfig(Fetcher("stream", None, None), None, None, "", None, None, None, None)
        a[Exception] should be thrownBy {
            JobDriver.run("xyz", JSONUtils.serialize(jobConfig), new TestModel);
        }
    }

    it should "thrown an exception if unable to parse the config file" in {
        a[Exception] should be thrownBy {
            JobDriver.run("streaming", JSONUtils.serialize(""), new TestModel);
        }
    }

    it should "fetch the app name from job config model if the app name is not specified" in {

        val jobConfig = JobConfig(
            Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry.log"))))),
            Option(Array[Filter](Filter("eventId", "EQ", Option("OE_START")))),
            None,
            "org.ekstep.analytics.framework.TestModel",
            Option(Map()),
            Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))),
            Option(8),
            None,
            None)

        noException should be thrownBy {
            implicit val sc: Option[SparkContext] = Option(CommonUtil.getSparkContext(1, "Test"));
            JobDriver.run[Event, String]("batch", JSONUtils.serialize(jobConfig), new TestModel);
            CommonUtil.closeSparkContext()(sc.get);
        }
    }

    it should "run the job driver on measured event" in {

        val jobConfig = JobConfig(
            Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry.log"))))),
            Option(Array[Filter](Filter("eventId", "EQ", Option("OE_START")))),
            None,
            "org.ekstep.analytics.framework.TestModel",
            Option(Map()),
            Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))),
            Option(8),
            None,
            Option(true))

        noException should be thrownBy {
            implicit val sc = Option(CommonUtil.getSparkContext(1, "Test"));
            JobDriver.run[MeasuredEvent, String]("batch", JSONUtils.serialize(jobConfig), TestModel2);
            CommonUtil.closeSparkContext()(sc.get);
        }
    }

    // test cases for merging multiple jobs into single job
    it should "run the batch job driver on multiple models" in {

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

        noException should be thrownBy {
            implicit val sc = Option(CommonUtil.getSparkContext(1, "Test"));
            val models = List(TestModel2, TestModel3)
            JobDriver.run[MeasuredEvent, String]("batch", JSONUtils.serialize(jobConfig), models, "TestMergeJobs");
            CommonUtil.closeSparkContext()(sc.get);
        }
    }

    it should "run the stream job driver on multiple models" in {
        val jobConfig = JobConfig(Fetcher("local", None, None), None, None, "", None, None, None, None)
        val models = List(TestModel2, TestModel3)
        JobDriver.run("streaming", JSONUtils.serialize(jobConfig), models, "TestMergeJobs");
    }

    it should "thrown an exception if unknown job type is found when running multile jobs" in {
        val jobConfig = JobConfig(Fetcher("local", None, None), None, None, "", None, None, None, None)
        a[Exception] should be thrownBy {
            val models = List(TestModel2, TestModel3)
            JobDriver.run("xyz", JSONUtils.serialize(jobConfig), models, "TestMergeJobs");
        }
    }

    it should "thrown an exception if unable to parse the config file when running multiple jobs" in {
        a[Exception] should be thrownBy {
            val models = List(TestModel2, TestModel3)
            JobDriver.run("batch", JSONUtils.serialize(""), models, "TestMergeJobs");
        }
    }
}