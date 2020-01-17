package org.ekstep.analytics.framework

class TestJobContext extends SparkSpec {

    "JobContext" should "clean up RDDs and unpersist the RDD from cache" in {
        val eventsRDD = events.cache()
        JobContext.recordRDD(eventsRDD)
        JobContext.cleanUpRDDs()
    }

    "it" should "try to unpersist the RDD which is not in cache" in {
        val eventsRDD = events
        JobContext.recordRDD(eventsRDD)
        JobContext.cleanUpRDDs()
    }
}