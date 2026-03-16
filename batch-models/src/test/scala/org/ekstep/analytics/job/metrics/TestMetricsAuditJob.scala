package org.ekstep.analytics.job.metrics

import org.ekstep.analytics.model.SparkSpec

class TestMetricsAuditJob  extends SparkSpec(null)  {

    "TestMetricsAuditJob" should "execute MetricsAudit job and won't throw any Exception" in {
        val configString ="{\"search\":{\"type\":\"none\"},\"model\":\"org.ekstep.analytics.model.MetricsAuditJob\",\"modelParams\":{\"auditConfig\":[{\"name\":\"denorm\",\"search\":{\"type\":\"local\",\"queries\":[{\"file\":\"src/test/resources/audit-metrics-report/denorm/2019-12-03-1575312964601.json\"}]},\"filters\":[{\"name\":\"flags.user_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.content_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.device_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.dialcode_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.collection_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.derived_location_retrieved\",\"operator\":\"EQ\",\"value\":true}]},{\"name\":\"raw\",\"search\":{\"type\":\"local\",\"queries\":[{\"file\":\"src/test/resources/audit-metrics-report/raw/raw.json\"}]}}]},\"output\":[{\"to\":\"file\",\"params\":{\"file\":\"src/test/resources/audit-metrics-result\"}}],\"parallelization\":8,\"appName\":\"Metrics Audit\"}"
        MetricsAuditJob.main(configString)(Option(sc))
    }

}
