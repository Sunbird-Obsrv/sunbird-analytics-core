package org.ekstep.analytics.model

import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils

import scala.collection.mutable.Buffer
import org.scalatest.Matchers

case class WorkflowDataRead(did: Option[String], sid: String, uid: String, pdata: PData, channel: String, content_id: Option[String], session_type: String, syncts: Long, dt_range: DtRange, mode: Option[String], item_responses: Option[Buffer[ItemResponse]],
                            start_time: Long, end_time: Long, time_spent: Double, time_diff: Double, interact_events_count: Long, interact_events_per_min: Double, telemetry_version: String,
                            env_summary: Option[Iterable[EnvSummary]], events_summary: Option[Iterable[EventSummary]],
                            page_summary: Option[Iterable[PageSummary]], etags: Option[ETags])

class TestWorkFlowSummaryModel extends SparkFlatSpec with Matchers {

    implicit val fc = new FrameworkContext();

    it should "generate 6 workflow summary with 1 default app summary" in {

        fc.outputEventsCount = sc.longAccumulator("OutputEventsCount");
        fc.inputEventsCount = sc.longAccumulator("InputEventsCount");
        val data = loadFile[String]("src/test/resources/workflow-summary/test-data2.log")
        val out = WorkFlowSummaryModel.execute(data, None)
        out.count() should be(8)
        fc.outputEventsCount.value should be(8)

        val me = out.collect();
        val appSummaryEvent1 = me.filter { x => x.dimensions.`type`.get.equals("app") }
        val sessionSummaryEvent1 = me.filter { x => x.dimensions.`type`.get.equals("session") }
        val playerSummaryEvent1 = me.filter { x => x.dimensions.`type`.get.equals("player") }
        val editorSummaryEvent1 = me.filter { x => x.dimensions.`type`.get.equals("editor") }

        appSummaryEvent1.size should be(3)
        sessionSummaryEvent1.size should be(1)
        playerSummaryEvent1.size should be(1)
        editorSummaryEvent1.size should be(3)

        val event1 = playerSummaryEvent1.last
        event1.`object`.get should not be null
        event1.`object`.get.id should be("do_1122852550749306881159")
        event1.`object`.get.ver.get should be("1.0")
        event1.`object`.get.`type` should be("Content")
        event1.`object`.get.rollup.get.l1 should be("do_3124020553502310402803")
        fc.inputEventsCount.reset()
        fc.outputEventsCount.reset();
    }

    it should "generate 3 workflow summary" in {
      
        val data = loadFile[String]("src/test/resources/workflow-summary/test-data3.log")
        val out = WorkFlowSummaryModel.execute(data, None)
        out.count() should be(3)

        val me = out.collect();
        val appSummaryEvent1 = me.filter { x => x.dimensions.`type`.get.equals("app") }
        val sessionSummaryEvent1 = me.filter { x => x.dimensions.`type`.get.equals("session") }
        val playerSummaryEvent1 = me.filter { x => x.dimensions.`type`.get.equals("player") }
        val editorSummaryEvent1 = me.filter { x => x.dimensions.`type`.get.equals("editor") }

        appSummaryEvent1.size should be(2)
        sessionSummaryEvent1.size should be(1)
        playerSummaryEvent1.size should be(0)
        editorSummaryEvent1.size should be(0)

        val event1 = appSummaryEvent1.filter(f => f.mid.equals("47377F637676DDE217BAFA1DF95E6FF1")).last

        // Validate for event envelope
        event1.eid should be("ME_WORKFLOW_SUMMARY");
        event1.context.pdata.model.get should be("WorkflowSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("SESSION");
        event1.context.date_range should not be null;
        event1.dimensions.`type`.get should be("app");
        event1.dimensions.did.get should be("11573c50cae2078e847f12c91a2d1965eaa73714");
        event1.dimensions.sid.get should be("830811c2-3c02-4c45-8755-3f15064a88a2");
        event1.dimensions.mode.getOrElse("") should be("")

        val summary1 = JSONUtils.deserialize[WorkflowDataRead](JSONUtils.serialize(event1.edata.eks));
        summary1.interact_events_per_min should be(1.0);
        summary1.start_time should be(1515402354310L);
        summary1.interact_events_count should be(1);
        summary1.end_time should be(1515402366478L);
        summary1.time_diff should be(12.17);
        summary1.time_spent should be(12.17);
        summary1.item_responses.get.size should be(0);
        summary1.page_summary.get.size should be(0);
        summary1.env_summary.get.size should be(0);
        summary1.events_summary.get.size should be(2);
        summary1.telemetry_version should be("3.0");

        val esList1 = summary1.events_summary.get
        esList1.size should be(2);
        val esMap1 = esList1.map { x => (x.id, x.count) }.toMap
        esMap1.get("INTERACT").get should be(1);
        esMap1.get("START").get should be(2);

        val event2 = sessionSummaryEvent1.head

        // Validate for event envelope
        event2.eid should be("ME_WORKFLOW_SUMMARY");
        event2.context.pdata.model.get should be("WorkflowSummarizer");
        event2.context.pdata.ver should be("1.0");
        event2.context.granularity should be("SESSION");
        event2.context.date_range should not be null;
        event2.dimensions.`type`.get should be("session");
        event2.dimensions.did.get should be("11573c50cae2078e847f12c91a2d1965eaa73714");
        event2.dimensions.sid.get should be("830811c2-3c02-4c45-8755-3f15064a88a2");
        event1.dimensions.mode.getOrElse("") should be("")

        val summary2 = JSONUtils.deserialize[WorkflowDataRead](JSONUtils.serialize(event2.edata.eks));
        summary2.interact_events_per_min should be(1.0);
        summary2.start_time should be(1515402354782L);
        summary2.interact_events_count should be(1);
        summary2.end_time should be(1515402366478L);
        summary2.time_diff should be(11.7);
        summary2.time_spent should be(11.7);
        summary2.item_responses.get.size should be(0);
        summary2.page_summary.get.size should be(0);
        summary2.env_summary.get.size should be(0);
        summary2.events_summary.get.size should be(2);
        summary2.telemetry_version should be("3.0");

        val esList2 = summary2.events_summary.get
        esList2.size should be(2);
        val esMap2 = esList2.map { x => (x.id, x.count) }.toMap
        esMap2.get("INTERACT").get should be(1);
        esMap2.get("START").get should be(1);
    }

    it should "generate workflow summary with breaking session logic" in {
        val data = loadFile[String]("src/test/resources/workflow-summary/test-data4.log")
        val out = WorkFlowSummaryModel.execute(data, None)
        out.count() should be(7)

        val me = out.collect();
        val appSummaryEvent1 = me.filter { x => x.dimensions.`type`.get.equals("app") }
        val sessionSummaryEvent1 = me.filter { x => x.dimensions.`type`.get.equals("session") }
        val playerSummaryEvent1 = me.filter { x => x.dimensions.`type`.get.equals("player") }
        val editorSummaryEvent1 = me.filter { x => x.dimensions.`type`.get.equals("editor") }

        appSummaryEvent1.size should be(3)
        sessionSummaryEvent1.size should be(2)
        playerSummaryEvent1.size should be(2)
        editorSummaryEvent1.size should be(0)

        val event1 = playerSummaryEvent1.filter(f => f.mid.equals("B30D043CA457042F71E73C24404737BE")).last

        event1.eid should be("ME_WORKFLOW_SUMMARY");
        event1.context.pdata.model.get should be("WorkflowSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("SESSION");
        event1.context.date_range should not be null;
        event1.dimensions.`type`.get should be("player");
        event1.dimensions.did.get should be("b027147870670bc57de790535311fbe5");
        event1.`object`.get.id should be("do_1122852550749306881159")
        event1.dimensions.sid.get should be("7op5o46hpi2abkmp8ckihjeq72");
        event1.dimensions.mode.getOrElse("") should be("preview")

        val summary1 = JSONUtils.deserialize[WorkflowDataRead](JSONUtils.serialize(event1.edata.eks));
        summary1.interact_events_per_min should be(0.3);
        summary1.start_time should be(1515496370223L);
        summary1.interact_events_count should be(1);
        summary1.end_time should be(1515497570223L);
        summary1.time_diff should be(1200.0);
        summary1.time_spent should be(200.0);
        summary1.item_responses.get.size should be(1);
        summary1.page_summary.get.size should be(0);
        summary1.env_summary.get.size should be(0);
        summary1.events_summary.get.size should be(3);
        summary1.telemetry_version should be("3.0");

        val esList1 = summary1.events_summary.get
        esList1.size should be(3);
        val esMap1 = esList1.map { x => (x.id, x.count) }.toMap
        esMap1.get("INTERACT").get should be(2);
        esMap1.get("ASSESS").get should be(1);
        esMap1.get("END").get should be(1);
    }

    it should "generate workflow summary with proper root summary setting logic" in {
        val data = loadFile[String]("src/test/resources/workflow-summary/test-data5.log")
        val out = WorkFlowSummaryModel.execute(data, None)
        out.count() should be(19)

        val me = out.collect();
        val appSummaryEvents = me.filter { x => x.dimensions.`type`.get.equals("app") }
        val sessionSummaryEvents = me.filter { x => x.dimensions.`type`.get.equals("session") }
        val playerSummaryEvents = me.filter { x => x.dimensions.`type`.get.equals("content") }
        val editorSummaryEvents = me.filter { x => x.dimensions.`type`.get.equals("editor") }

        appSummaryEvents.size should be(4)
        sessionSummaryEvents.size should be(0)
        playerSummaryEvents.size should be(5)
        editorSummaryEvents.size should be(0)

        val event1 = appSummaryEvents.filter(f => f.mid.equals("5AFA0438BAE767D64A535FF814108D17")).last

        event1.eid should be("ME_WORKFLOW_SUMMARY");
        event1.context.pdata.model.get should be("WorkflowSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("SESSION");
        event1.context.date_range should not be null;
        event1.dimensions.`type`.get should be("app");
        event1.dimensions.did.get should be("1b21a2906e7de0dd66235e7cf9373adb4aaaa104");
        event1.dimensions.sid.get should be("b8bc1f1d-22a6-4aa6-aa5e-de3654e80f96");
        event1.dimensions.channel.get should be("01235953109336064029450")

        val summary1 = JSONUtils.deserialize[WorkflowDataRead](JSONUtils.serialize(event1.edata.eks));
        summary1.interact_events_per_min should be(1.77);
        summary1.start_time should be(1536645647965L);
        summary1.interact_events_count should be(11);
        summary1.end_time should be(1536646021231L);
        summary1.time_diff should be(373.27);
        summary1.time_spent should be(373.3);
        summary1.item_responses.get.size should be(0);
        summary1.page_summary.get.size should be(4);
        summary1.env_summary.get.size should be(1);
        summary1.events_summary.get.size should be(5);
        summary1.telemetry_version should be("3.0");
    }

    it should "generate workflow summary with default app summary for events starting with other start events" in {
        val data = loadFile[String]("src/test/resources/workflow-summary/test-data1.log")
        val out = WorkFlowSummaryModel.execute(data, None)

        out.count() should be(9)

        val me = out.collect();
        val appSummaryEvents = me.filter { x => x.dimensions.`type`.get.equals("app") }
        appSummaryEvents.size should be(3)

        val event1 = appSummaryEvents.filter(f => f.mid.equals("B151E935C1F3E9D7D30A4A6EB96E2AE2")).last

        event1.eid should be("ME_WORKFLOW_SUMMARY");
        event1.context.pdata.model.get should be("WorkflowSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("SESSION");
        event1.context.date_range should not be null;
        event1.dimensions.`type`.get should be("app");
        event1.dimensions.did.get should be("a49c706d0402d6db3bb7cb3105cc9e7cf9b2ed7e");
        event1.dimensions.sid.get should be("8f32dbc4-c0d0-4630-9ff6-8c3bce3d15bb");
        event1.dimensions.channel.get should be("01235953109336064029450")

        val summary1 = JSONUtils.deserialize[WorkflowDataRead](JSONUtils.serialize(event1.edata.eks));
        summary1.interact_events_per_min should be(0);
        summary1.start_time should be(1534595611976L);
        summary1.interact_events_count should be(0);
        summary1.end_time should be(1534595617324L);
        summary1.time_diff should be(5.35);
        summary1.time_spent should be(5.35);
        summary1.item_responses.get.size should be(0);
        summary1.page_summary.get.size should be(1);
        summary1.env_summary.get.size should be(1);
        summary1.events_summary.get.size should be(3);
        summary1.telemetry_version should be("3.0");
    }

    it should "generate workflow summary with proper root summary closing logic" in {
        val data = loadFile[String]("src/test/resources/workflow-summary/test-data6.log")
        val out = WorkFlowSummaryModel.execute(data, None)
        out.count() should be(15)

        val me = out.collect();
        val appSummaryEvents = me.filter { x => x.dimensions.`type`.get.equals("app") }
        appSummaryEvents.size should be(2)

        val event1 = appSummaryEvents.filter(f => f.mid.equals("D0E7F723AB0CBE295ED757AB608D1EBF")).last

        event1.eid should be("ME_WORKFLOW_SUMMARY");
        event1.context.pdata.model.get should be("WorkflowSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("SESSION");
        event1.context.date_range should not be null;
        event1.dimensions.`type`.get should be("app");
        event1.dimensions.did.get should be("e758d6c277dafbda491a5f3824622b5a612304dc");
        event1.dimensions.sid.get should be("430d7850-39ff-4f3d-b672-dfb4ae875160");
        event1.dimensions.channel.get should be("01235953109336064029450")

        val summary1 = JSONUtils.deserialize[WorkflowDataRead](JSONUtils.serialize(event1.edata.eks));
        summary1.interact_events_per_min should be(2.14);
        summary1.start_time should be(1534700219419L);
        summary1.interact_events_count should be(13);
        summary1.end_time should be(1534700584532L);
        summary1.time_diff should be(365.11);
        summary1.time_spent should be(365.13);
        summary1.item_responses.get.size should be(0);
        summary1.page_summary.get.size should be(4);
        summary1.env_summary.get.size should be(1);
        summary1.events_summary.get.size should be(5);
        summary1.telemetry_version should be("3.0");
    }

    it should "generate workflow summary to test for END case scenarios" in {
        val data = loadFile[String]("src/test/resources/workflow-summary/test-data7.log")
        val out = WorkFlowSummaryModel.execute(data, None)

        out.count() should be(6)

        val event1 = out.collect().filter(f => f.mid.equals("2D2E6508F9F24B6B15DCC9F43B82E9D8")).last

        event1.eid should be("ME_WORKFLOW_SUMMARY");
        event1.context.pdata.model.get should be("WorkflowSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("SESSION");
        event1.context.date_range should not be null;
        event1.dimensions.`type`.get should be("content");
        event1.dimensions.mode.get should be("play");
        event1.dimensions.did.get should be("af53a718911fc1703461bbfe5880cb1585cf54ae");
        event1.dimensions.sid.get should be("d05c1bc5-2f03-4524-9c1f-981e7e701e10");
        event1.dimensions.channel.get should be("505c7c48ac6dc1edc9b08f21db5a571d")

        val summary1 = JSONUtils.deserialize[WorkflowDataRead](JSONUtils.serialize(event1.edata.eks));
        summary1.interact_events_per_min should be(0);
        summary1.start_time should be(1583031769407L);
        summary1.interact_events_count should be(0);
        summary1.end_time should be(1583031850824L);
        summary1.time_diff should be(81.42);
        summary1.time_spent should be(81.41);
        summary1.item_responses.get.size should be(0);
        summary1.page_summary.get.size should be(0);
        summary1.env_summary.get.size should be(0);
        summary1.events_summary.get.size should be(2);
        summary1.telemetry_version should be("3.0");
    }

    it should "generate workflow summary to test else case for session breaking logic" in {
        val data = loadFile[String]("src/test/resources/workflow-summary/test-data8.log")
        val out = WorkFlowSummaryModel.execute(data, None)
        out.count() should be(1)
    }

    
}