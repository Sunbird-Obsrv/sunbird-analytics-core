package org.ekstep.analytics.framework

/**
 * @author Santhosh
 */
class TestModels extends BaseSpec {

    "Models" should "create model objects for raw event" in {

        val gdata = new GData("org.ekstep.aser", "1.0");
        
        val edata = new EData(null, null);
        val event = new Event("OE_TEST", "2015-09-23T09:32:11+05:30", 0L, "2015-09-23T09:32:11+05:30", "1.0", gdata, "569b2478-c8da-4e21-8887-a0dfc2b47d7a", "569b2478-c8da-4e21-8887-a0dfc2b47d7a", "569b2478-c8da-4e21-8887-a0dfc2b47d7a", None, None, edata, None)

        event.eid should be("OE_TEST");
        
        JobStatus.values.size should be(5)
        OnboardStage.values.size should be(7)
        OtherStage.values.size should be(5)
        TestDataExStage.values.size should be(9)
        TestJobStageStatus.values.size should be(2)
        ExperimentStatus.values.size should be(4)
    }

}

object TestDataExStage extends DataExStage

object TestJobStageStatus extends JobStageStatus