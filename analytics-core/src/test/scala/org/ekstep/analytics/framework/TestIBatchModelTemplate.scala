package org.ekstep.analytics.framework

class TestIBatchModelTemplate extends SparkSpec {

    "IBatchModelTemplate" should "group data by did" in {

        implicit val fc = new FrameworkContext();
        val rdd = SampleModelTemplate.execute(events, None);
        rdd.count should be(7);
        SampleModelTemplate.name() should be ("SampleModelTemplate")

    }
}