package org.ekstep.analytics.model


import java.time.{ZoneOffset, ZonedDateTime}

import cats.syntax.either._
import com.ing.wbaa.druid._
import com.ing.wbaa.druid.client.DruidClient
import io.circe.Json
import io.circe.parser.parse
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils
import org.scalamock.scalatest.MockFactory

import scala.concurrent.Future

  class TestMetricsAuditModel extends SparkSpec(null) with MockFactory{

    implicit val fc = mock[FrameworkContext];
    
    override def beforeAll() {
    	    super.beforeAll();
        fc.inputEventsCount = sc.longAccumulator("TestCount");
    }

    "TestMetricsAuditJob" should "get the metrics for monitoring the data pipeline" in {
      val auditConfig = "{\"search\":{\"type\":\"none\"},\"model\":\"org.ekstep.analytics.model.MetricsAuditJob\",\"modelParams\":{\"auditConfig\":[{\"name\":\"denorm\",\"search\":{\"type\":\"local\",\"queries\":[{\"file\":\"src/test/resources/audit-metrics-report/denorm/2019-12-03-1575312964601.json\"}]},\"filters\":[{\"name\":\"flags.user_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.content_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.device_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.dialcode_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.collection_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.derived_location_retrieved\",\"operator\":\"EQ\",\"value\":true}]},{\"name\":\"raw\",\"search\":{\"type\":\"local\",\"queries\":[{\"file\":\"src/test/resources/audit-metrics-report/raw/raw.json\"}]}},{\"name\":\"failed\",\"search\":{\"type\":\"local\",\"queries\":[{\"file\":\"src/test/resources/audit-metrics-report/failed/2019-12-04-1575420457646.json\"}]}},{\"name\":\"channel-raw\",\"search\":{\"type\":\"local\",\"queries\":[{\"file\":\"src/test/resources/audit-metrics-report/channel-raw/2019-12-04-1575399627832.json\"}]}},{\"name\":\"channel-summary\",\"search\":{\"type\":\"local\",\"queries\":[{\"file\":\"src/test/resources/audit-metrics-report/channel-summary/2019-12-04-1575510009570.json\"}]}}]},\"output\":[{\"to\":\"file\",\"params\":{\"path\":\"src/test/resources/\"}}],\"parallelization\":8,\"appName\":\"Metrics Audit\"}"
      val config = JSONUtils.deserialize[JobConfig](auditConfig)
      MetricsAuditModel.execute(sc.emptyRDD, config.modelParams)
    }

    it should "load the local file and give the denorm count" in {
      val auditConfig = "{\"search\":{\"type\":\"none\"},\"model\":\"org.ekstep.analytics.model.MetricsAuditJob\",\"modelParams\":{\"auditConfig\":[{\"name\":\"denorm\",\"search\":{\"type\":\"local\",\"queries\":[{\"file\":\"src/test/resources/audit-metrics-report/denorm/2019-12-03-1575312964601.json\"}]},\"filters\":[{\"name\":\"flags.user_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.content_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.device_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.dialcode_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.collection_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.derived_location_retrieved\",\"operator\":\"EQ\",\"value\":true}]}]},\"output\":[{\"to\":\"file\",\"params\":{\"file\":\"src/test/resources/audit-metrics-result\"}}],\"parallelization\":8,\"appName\":\"Metrics Audit\"}"
      val config = JSONUtils.deserialize[JobConfig](auditConfig)
      val metrics = MetricsAuditModel.execute(sc.emptyRDD, config.modelParams)
      metrics.collect().map{f =>
       val metricsEdata = f.edata.asInstanceOf[Map[String, AnyRef]].get("metrics").get.asInstanceOf[List[V3MetricEdata]]
        metricsEdata.map{edata =>
          if("count".equals(edata.metric)) edata.value should be (Some(312))
          if("user_data_retrieved".equals(edata.metric)) edata.value should be (Some(0))
          if("percentage_events_with_user_data_retrieved".equals(edata.metric)) edata.value should be (Some(0))
          if("derived_location_retrieved".equals(edata.metric)) edata.value should be (Some(1))
        }
      }
    }

    it should "load the local file and give the count for other files than failed/denorm" in {
      val auditConfig = "{\"search\":{\"type\":\"none\"},\"model\":\"org.ekstep.analytics.model.MetricsAuditJob\",\"modelParams\":{\"auditConfig\":[{\"name\":\"raw\",\"search\":{\"type\":\"local\",\"queries\":[{\"file\":\"src/test/resources/audit-metrics-report/raw/raw.json\"}]}}]},\"output\":[{\"to\":\"file\",\"params\":{\"file\":\"src/test/resources/audit-metrics-result\"}}],\"parallelization\":8,\"appName\":\"Metrics Audit\"}"
      val config = Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/audit-metrics-report/raw/raw.json")))))
      val testRDD = DataFetcher.fetchBatchData[String](config)
      testRDD.count() should be (57792)

      val metrics = MetricsAuditModel.getTotalSecorCountAudit(testRDD)
      metrics.filter(f => "inputEvents".equals(f.metric)).map(f => f.value should be (Some(57792)))
    }

    it should "load failed files from local and give the count for failed backup" in {
      val config = Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/audit-metrics-report/failed/2019-12-04-1575420457646.json")))))
      val testRDD = DataFetcher.fetchBatchData[V3ContextEvent](config)
      testRDD.count() should be (94)

      val metrics = MetricsAuditModel.getFailedSecorAudit(testRDD)
      metrics.map{f => if("AnalyticsAPI".equals(f.metric)) f.value should be (Some(94))}
    }

    it should "get the metrics for druid count for monitoring" in {
      implicit val sqlContext = new SQLContext(sc)
      import scala.concurrent.ExecutionContext.Implicits.global
      implicit val mockDruidConfig = DruidConfig.DefaultConfig

      val json: String = """
          {
              "total_count" : 9007
          }
        """
      val doc: Json = parse(json).getOrElse(Json.Null);
      val results = List(DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc));
      val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.Timeseries)

      val mockDruidClient = mock[DruidClient]
      (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes();
      (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();


      val auditConfig = "{\"search\":{\"type\":\"none\"},\"model\":\"org.ekstep.analytics.model.MetricsAuditJob\",\"modelParams\":{\"auditConfig\":[{\"name\":\"telemetry-count\",\"search\":{\"type\":\"druid\",\"druidQuery\":{\"queryType\":\"timeSeries\",\"dataSource\":\"telemetry-events\",\"intervals\":\"LastDay\",\"aggregations\":[{\"name\":\"total_count\",\"type\":\"count\",\"fieldName\":\"count\"}],\"descending\":\"false\"}}},{\"name\":\"summary-count\",\"search\":{\"type\":\"druid\",\"druidQuery\":{\"queryType\":\"timeSeries\",\"dataSource\":\"summary-events\",\"intervals\":\"LastDay\",\"aggregations\":[{\"name\":\"total_count\",\"type\":\"count\",\"fieldName\":\"count\"}],\"descending\":\"false\"}}}]},\"output\":[{\"to\":\"file\",\"params\":{\"file\":\"src/test/resources/audit-metrics-result\"}}],\"parallelization\":8,\"appName\":\"Metrics Audit\"}"
      val config = JSONUtils.deserialize[JobConfig](auditConfig)
      val metrics = MetricsAuditModel.execute(sc.emptyRDD, config.modelParams)
      metrics.count() should be (2)
    }

    it should "execute MetricsAudit job for denorm count < 0" in {
      val configString ="{\"search\":{\"type\":\"none\"},\"model\":\"org.ekstep.analytics.model.MetricsAuditJob\",\"modelParams\":{\"auditConfig\":[{\"name\":\"denorm\",\"search\":{\"type\":\"local\",\"queries\":[{\"file\":\"src/test/resources/audit-metrics-report/denorm/empty.json\"}]},\"filters\":[{\"name\":\"flags.user_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.content_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.device_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.dialcode_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.collection_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.derived_location_retrieved\",\"operator\":\"EQ\",\"value\":true}]},{\"name\":\"raw\",\"search\":{\"type\":\"local\",\"queries\":[{\"file\":\"src/test/resources/audit-metrics-report/raw/raw.json\"}]}}]},\"output\":[{\"to\":\"file\",\"params\":{\"file\":\"src/test/resources/audit-metrics-result\"}}],\"parallelization\":8,\"appName\":\"Metrics Audit\"}"
      val config = JSONUtils.deserialize[JobConfig](configString)
      val metrics = MetricsAuditModel.execute(sc.emptyRDD, config.modelParams)
    }
}
