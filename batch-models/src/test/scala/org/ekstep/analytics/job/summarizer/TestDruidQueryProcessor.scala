package org.ekstep.analytics.job.summarizer
import cats.syntax.either._
import com.ing.wbaa.druid.client.DruidClient
import com.ing.wbaa.druid._
import io.circe.Json
import io.circe.parser._
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.model.{SparkSpec, _}
import org.ekstep.analytics.util.EmbeddedPostgresql
import org.joda.time
import org.scalamock.scalatest.MockFactory

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.Date
import scala.collection.mutable.LinkedHashMap
import scala.concurrent.Future

class TestDruidQueryProcessor extends SparkSpec(null) with MockFactory {

    override def beforeAll(){
        super.beforeAll()
        EmbeddedPostgresql.start()
        EmbeddedPostgresql.createReportConfigTable()
        EmbeddedPostgresql.execute(
            s"""insert into local_report_config (report_id, updated_on, report_description, requested_by,
      report_schedule, config, created_on, submitted_on, status, status_msg,"batch_number") values
      ('district_daily', '${new Date()}', 'District Weekly Description',
        'User1','Daily' , '{"reportConfig":{"id":"district_daily","queryType":"groupBy","dateRange":{"staticInterval":"LastMonth","granularity":"all"},"metrics":[{"metric":"totalUniqueDevices","label":"Total Unique Devices","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastMonth","aggregations":[{"name":"total_scans","type":"cardinality","fieldName":"context_did"}],"dimensions":[{"fieldName":"derived_loc_state","aliasName":"state"}],"filters":[{"type":"in","dimension":"context_pdata_id","values":["__producerEnv__.diksha.portal","__producerEnv__.diksha.app"]},{"type":"isnotnull","dimension":"derived_loc_state"},{"type":"isnotnull","dimension":"derived_loc_district"}],"descending":"false"}}],"labels":{"state":"State"},"output":[{"type":"csv","metrics":["total_scans"],"dims":["state"],"fileParameters":["id","dims"]}]},"store":"__store__","container":"__container__","key":"druid-reports1/"}',
        '${new Date()}', '${new Date()}' ,'ACTIVE', 'Report Updated','1')""")
      EmbeddedPostgresql.execute(
        s"""insert into local_report_config (report_id, updated_on, report_description, requested_by,
      report_schedule, config, created_on, submitted_on, status, status_msg,"batch_number") values
      ('district_weekly', '${new Date()}', 'District Weekly Description',
        'User1','Weekly' , '{"reportConfig":{"id":"district_weekly","queryType":"groupBy","dateRange":{"staticInterval":"LastMonth","granularity":"all"},"metrics":[{"metric":"totalUniqueDevices","label":"Total Unique Devices","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastMonth","aggregations":[{"name":"total_scans","type":"cardinality","fieldName":"context_did"}],"dimensions":[{"fieldName":"derived_loc_state","aliasName":"state"}],"filters":[{"type":"in","dimension":"context_pdata_id","values":["__producerEnv__.diksha.portal","__producerEnv__.diksha.app"]},{"type":"isnotnull","dimension":"derived_loc_state"},{"type":"isnotnull","dimension":"derived_loc_district"}],"descending":"false"}}],"labels":{"state":"State"},"output":[{"type":"csv","metrics":["total_scans"],"dims":["state"],"fileParameters":["id","dims"]}]},"store":"__store__","container":"__container__","key":"druid-reports/"}',
        '${new Date()}', '${new Date()}' ,'ACTIVE', 'Report Updated','1')""")
      EmbeddedPostgresql.execute(
        s"""insert into local_report_config (report_id, updated_on, report_description, requested_by,
      report_schedule, config, created_on, submitted_on, status, status_msg,"batch_number") values
      ('district_monthly', '${new Date()}', 'District Weekly Description',
        'User1','Monthly' , '{"reportConfig":{"id":"district_monthly","queryType":"groupBy","dateRange":{"staticInterval":"LastMonth","granularity":"all"},"metrics":[{"metric":"totalUniqueDevices","label":"Total Unique Devices","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastMonth","aggregations":[{"name":"total_scans","type":"cardinality","fieldName":"context_did"}],"dimensions":[{"fieldName":"derived_loc_state","aliasName":"state"}],"filters":[{"type":"in","dimension":"context_pdata_id","values":["__producerEnv__.diksha.portal","__producerEnv__.diksha.app"]},{"type":"isnotnull","dimension":"derived_loc_state"},{"type":"isnotnull","dimension":"derived_loc_district"}],"descending":"false"}}],"labels":{"state":"State"},"output":[{"type":"csv","metrics":["total_scans"],"dims":["state"],"fileParameters":["id","dims"]}]},"store":"__store__","container":"__container__","key":"druid-reports/"}',
        '${new Date()}', '${new Date()}' ,'ACTIVE', 'Report Updated','1')""")
      EmbeddedPostgresql.execute(
        s"""insert into local_report_config (report_id, updated_on, report_description, requested_by,
      report_schedule, config, created_on, submitted_on, status, status_msg,"batch_number") values
      ('report_once', '${new Date()}', 'District Weekly Description',
        'User1','Once' , '{"reportConfig":{"id":"invalid_report","queryType":"groupBy","dateRange":{"staticInterval":"LastMonth","granularity":"all"},"metrics":[{"metric":"totalUniqueDevices","label":"Total Unique Devices","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastMonth","aggregations":[{"name":"total_scans","type":"cardinality","fieldName":"context_did"}],"dimensions":[{"fieldName":"derived_loc_state","aliasName":"state"}],"filters":[{"type":"in","dimension":"context_pdata_id","values":["__producerEnv__.diksha.portal","__producerEnv__.diksha.app"]},{"type":"isnotnull","dimension":"derived_loc_state"},{"type":"isnotnull","dimension":"derived_loc_district"}],"descending":"false"}}],"labels":{"state":"State"},"output":[{"type":"csv","metrics":["total_scans"],"dims":["state"],"fileParameters":["id","dims"]}]},"store":"__store__","container":"__container__","key":"druid-reports/"}',
        '${new Date()}', '${new Date()}' ,'ACTIVE', 'Report Updated','1')""")
      EmbeddedPostgresql.execute(
        s"""insert into local_report_config (report_id, updated_on, report_description, requested_by,
      report_schedule, config, created_on, submitted_on, status, status_msg,"batch_number") values
      ('invalid_report', '${new Date()}', 'District Weekly Description',
        'User1','Test' , '{"reportConfig":{"id":"invalid_report","queryType":"groupBy","dateRange":{"staticInterval":"LastMonth","granularity":"all"},"metrics":[{"metric":"totalUniqueDevices","label":"Total Unique Devices","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastMonth","aggregations":[{"name":"total_scans","type":"cardinality","fieldName":"context_did"}],"dimensions":[{"fieldName":"derived_loc_state","aliasName":"state"}],"filters":[{"type":"in","dimension":"context_pdata_id","values":["__producerEnv__.diksha.portal","__producerEnv__.diksha.app"]},{"type":"isnotnull","dimension":"derived_loc_state"},{"type":"isnotnull","dimension":"derived_loc_district"}],"descending":"false"}}],"labels":{"state":"State"},"output":[{"type":"csv","metrics":["total_scans"],"dims":["state"],"fileParameters":["id","dims"]}]},"store":"__store__","container":"__container__","key":"druid-reports/"}',
        '${new Date()}', '${new Date()}' ,'ACTIVE', 'Report Updated','1')""")
    }

    override def afterAll(): Unit ={
        super.afterAll()
        // EmbeddedPostgresql.close
    }

   "DruidQueryProcessor" should "execute multiple queries and generate csv reports on multiple dimensions" in {
        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", Option("day"), Option(List(Aggregation(Option("total_scans"), "count", "count"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation(Option("total_sessions"), "count", "count"),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("usage_metrics", "groupBy", QueryDateRange(None, Option("LastDay"), Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), collection.mutable.LinkedHashMap("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", None, List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"))))
        val modelParams = Map("mode"-> "standalone" ,"modelName" -> "UsageMetrics", "reportConfig" -> reportConfig, "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
        val config = JobConfig(Fetcher("none", None, None), null, null, "org.ekstep.analytics.model.DruidQueryProcessingModel", Option(modelParams), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestDruidQueryProcessor"), Option(true))
        DruidQueryProcessor.main(JSONUtils.serialize(config))(Option(sc))
    }

    ignore should "test batch implementation" in {
        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", Option("day"), Option(List(Aggregation(Option("total_scans"), "count", "count"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation(Option("total_sessions"), "count", "count"),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("usage_metrics", "groupBy", QueryDateRange(None, Option("LastDay"), Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), collection.mutable.LinkedHashMap("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", None, List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"))))
        val modelParams = Map("mode"-> "batch","modelName" -> "UsageMetrics", "reportConfig" -> reportConfig, "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
        val config = JobConfig(Fetcher("none", None, None), null, null, "org.ekstep.analytics.model.DruidQueryProcessingModel", Option(modelParams), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestDruidQueryProcessor"), Option(true))
        DruidQueryProcessor.main(JSONUtils.serialize(config))(Option(sc))
    }

   ignore should "test batch implementation with batch" in {
        implicit val fc = mock[FrameworkContext]
        val hadoopFileUtil = new HadoopFileUtil()
        import scala.concurrent.ExecutionContext.Implicits.global

        val json: String = """
            {
                "total_scans" : 9007,
                "producer_id" : "dev.sunbird.learning.platform",
                "state" : "ka"
            }
          """
        val doc: Json = parse(json).getOrElse(null)
        val results = List(DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc));
        val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)

        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, *).returns(Future(druidResponse)).anyNumberOfTimes();
        (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();
        (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("consumption_usage_metrics", "groupBy", QueryDateRange(Option(QueryInterval("2020-01-01", "2020-01-07")), None, Option("day")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), LinkedHashMap("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", None, List("total_scans"), List("date"), List("id", "dims"))), Option(ReportMergeConfig(Some("local"),"DAY", "", 1, Option("ACADEMIC_YEAR"), Option("Date"), Option(1), "daily_metrics.csv")))
        val strConfig = JSONUtils.serialize(reportConfig)
        val modelParams = Map("batchNumber"-> Some(1), "mode"-> "batch","reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "store" -> "local", "container" -> "test-container", "filePath" -> "src/test/resources/druid-reports/", "key" -> "test-reports_main/")
        val config = JobConfig(Fetcher("none", None, None), null, null, "org.ekstep.analytics.model.DruidQueryProcessingModel", Option(modelParams), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestDruidQueryProcessor"), Option(true))
            DruidQueryProcessor.main(JSONUtils.serialize(config))(Option(sc),Option(fc))

    }
  ignore should "test the report conditions" in {
    val date =new time.DateTime("2021-03-20")
    implicit val sqlContext = new SQLContext(sc)
    val df = DruidQueryProcessor.getReportConfigs(None,date.withDayOfWeek(1))(sqlContext)
    df.count() should be (3)
    val df1 = DruidQueryProcessor.getReportConfigs(None,date.withDayOfMonth(1))(sqlContext)
    df1.count() should be (4)

  }
}
