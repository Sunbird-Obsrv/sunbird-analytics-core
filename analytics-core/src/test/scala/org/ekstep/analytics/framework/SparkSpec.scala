package org.ekstep.analytics.framework

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.scalatest.BeforeAndAfterAll

/**
 * @author Santhosh
 */
class SparkSpec(val file: String = "src/test/resources/sample_telemetry.log") extends BaseSpec with BeforeAndAfterAll {

    var events: RDD[Event] = null;
    implicit var sc: SparkContext = null;

    override def beforeAll() {
        super.beforeAll();
        sc = CommonUtil.getSparkContext(1, "TestAnalyticsCore");
        events = loadFile[Event](file)
    }

    override def afterAll() {
        super.afterAll();
        CommonUtil.closeSparkContext();
    }

    def loadFile[T](file: String)(implicit mf: Manifest[T]): RDD[T] = {
        if (file == null) {
            return null;
        }
        sc.textFile(file, 1).map { line => JSONUtils.deserialize[T](line) }.filter { x => x != null }.cache();
    }

}