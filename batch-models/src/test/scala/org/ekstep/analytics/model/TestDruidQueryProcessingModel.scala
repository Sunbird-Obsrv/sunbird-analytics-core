package org.ekstep.analytics.model

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.scaladsl.Source
import akka.util.ByteString
import cats.syntax.either._
import com.ing.wbaa.druid._
import com.ing.wbaa.druid.client.DruidClient
import io.circe.Json
import io.circe.parser._
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.exception.DruidConfigException
import org.ekstep.analytics.framework.fetcher.{AkkaHttpClient, DruidDataFetcher}
import org.ekstep.analytics.framework.util.{HTTPClient, HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.util.{LocationResponse, StateLookup}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, Matchers}
import org.sunbird.cloud.storage.IStorageService

import java.time.{ZoneOffset, ZonedDateTime}
import scala.collection.mutable.LinkedHashMap
import scala.concurrent.Future

class TestDruidQueryProcessingModel extends SparkSpec(null) with Matchers with BeforeAndAfterAll with MockFactory {
    implicit val fc = mock[FrameworkContext]
    val hadoopFileUtil = new HadoopFileUtil()

    it should "execute multiple queries and generate csv reports on multiple dimensions with dynamic interval" in {
        //        implicit val sc = CommonUtil.getSparkContext(2, "TestDruidQueryProcessingModel", None, None);
        implicit val sqlContext = new SQLContext(sc)
        import scala.concurrent.ExecutionContext.Implicits.global

        val json: String = """
          {
              "total_scans" : 9007,
              "producer_id" : "dev.sunbird.learning.platform",
              "state" : null
          }
        """
        val doc: Json = parse(json).getOrElse(Json.Null);
        val results = List(DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc));
        val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)

        implicit val mockDruidConfig = DruidConfig.DefaultConfig
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes();
        (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();
        (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("consumption_usage_metrics", "groupBy",
          QueryDateRange(Option(QueryInterval("2020-01-01", "2020-01-07")), None, Option("day")),
          List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery),
            Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)),
          LinkedHashMap("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", None, List("total_scans"), List("state", "producer_id"), List("id", "dims", "date"))), Option(ReportMergeConfig(Some("local"),"DAY", "", 1, Option("ACADEMIC_YEAR"), Option("Date"), Option(1), "daily_metrics.csv")))
        val strConfig = JSONUtils.serialize(reportConfig)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "store" -> "local", "container" -> "test-container", "key" -> "src/test/resources/druid-reports/")
       DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));

    }

      it should "execute multiple queries and generate csv reports on single dimension" in {
          implicit val sqlContext = new SQLContext(sc)
          import scala.concurrent.ExecutionContext.Implicits.global
          implicit val mockDruidConfig = DruidConfig.DefaultConfig
          val mockDruidClient = mock[DruidClient]
          (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).
            returns(Future.apply[DruidResponse](DruidResponseTimeseriesImpl.apply(List(), QueryType.GroupBy))).anyNumberOfTimes();
          (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();

          val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", Option("day"), Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
          val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
          val reportConfig = ReportConfig("consumption_metrics", "groupBy", QueryDateRange(None, Option("LastDay"), Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), LinkedHashMap("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", None, List("total_scans", "total_sessions", "total_ts"), List("state"))))
          val strConfig = JSONUtils.serialize(reportConfig)
          val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
          DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
      }

      it should "execute multiple queries and generate single json report" in {
          implicit val sqlContext = new SQLContext(sc)
          import scala.concurrent.ExecutionContext.Implicits.global
          implicit val mockDruidConfig = DruidConfig.DefaultConfig
          val mockDruidClient = mock[DruidClient]
          (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).
            returns(Future.apply[DruidResponse](DruidResponseTimeseriesImpl.apply(List(), QueryType.GroupBy))).anyNumberOfTimes();
          (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();

          val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", Option("day"), Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
          val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
          val reportConfig = ReportConfig("usage_metrics", "groupBy", QueryDateRange(None, Option("LastDay"), Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), LinkedHashMap("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("json", None, List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"))))
          val strConfig = JSONUtils.serialize(reportConfig)
          val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "bucket" -> "test-container", "key" -> "druid-reports/usage_metrics.json")
          DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
      }

      it should "throw exception if query has different dimensions" in {
          implicit val sqlContext = new SQLContext(sc)
          import scala.concurrent.ExecutionContext.Implicits.global
          implicit val mockDruidConfig = DruidConfig.DefaultConfig
          val mockDruidClient = mock[DruidClient]
          (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).
            returns(Future.apply[DruidResponse](DruidResponseTimeseriesImpl.apply(List(), QueryType.GroupBy))).anyNumberOfTimes();
          (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();

          val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", Option("day"), Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(DruidDimension("device_loc_city", None), DruidDimension("context_pdata_id", None))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
          val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), None, Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
          val reportConfig = ReportConfig("usage_metrics", "groupBy", QueryDateRange(None, Option("LastDay"), Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), LinkedHashMap("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("json", None, List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"))))
          val strConfig = JSONUtils.serialize(reportConfig)
          val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "bucket" -> "test-container", "key" -> "druid-reports/usage_metrics.json", "filePath" -> "src/test/resources/")

          the[DruidConfigException] thrownBy {
              DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
          }
      }

      it should "throw exception if query does not have intervals" in {

          val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", Option("day"), Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
          val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
          val reportConfig = ReportConfig("usage_metrics", "groupBy", QueryDateRange(None, None, Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), LinkedHashMap("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("json", None, List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"))))
          val strConfig = JSONUtils.serialize(reportConfig)
          val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "bucket" -> "test-container", "key" -> "druid-reports/usage_metrics.json", "filePath" -> "src/test/resources/")

          the[DruidConfigException] thrownBy {
              DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
          }
      }

      it should "execute report and generate multiple csv reports" in {
          implicit val sqlContext = new SQLContext(sc)
          import scala.concurrent.ExecutionContext.Implicits.global
          implicit val mockDruidConfig = DruidConfig.DefaultConfig

          val json: String = """
            {
                "total_scans" : 9007,
                "total_sessions" : 100,
                "producer_id" : "dev.sunbird.learning.platform",
                "device_loc_state" : "Karnataka"
            }
          """
          val doc: Json = parse(json).getOrElse(Json.Null);
          val results = List(DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc));
          val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)


          val mockDruidClient = mock[DruidClient]
          (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes();
          (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();
          (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

          val scansQuery1 = DruidQueryModel("groupBy", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(DruidDimension("device_loc_state", None), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
          val contentPlaysQuery1 = DruidQueryModel("groupBy", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", None), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
          val reportConfig1 = ReportConfig("data_metrics", "groupBy", QueryDateRange(None, Option("LastDay"), Option("day")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery1), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery1)), LinkedHashMap("device_loc_state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", Option("scans"), List("total_scans"), List("device_loc_state", "producer_id"), List("id", "dims", "date")), OutputConfig("csv", Option("sessions"), List("total_sessions"), List("device_loc_state", "producer_id"), List("id", "dims", "date"))))
          val strConfig1 = JSONUtils.serialize(reportConfig1)

          val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig1), "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
          DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
      }

      it should "execute weekly report and generate csv reports" in {
          implicit val sqlContext = new SQLContext(sc)
          import scala.concurrent.ExecutionContext.Implicits.global
          implicit val mockDruidConfig = DruidConfig.DefaultConfig
          val mockDruidClient = mock[DruidClient]
          (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).
            returns(Future.apply[DruidResponse](DruidResponseTimeseriesImpl.apply(List(), QueryType.GroupBy))).anyNumberOfTimes();
          (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();

          val scansQuery2 = DruidQueryModel("groupBy", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
          val contentPlaysQuery2 = DruidQueryModel("groupBy", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
          val reportConfig2 = ReportConfig("usage_metrics", "groupBy", QueryDateRange(None, Option("LastWeek"), None), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery2), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery2)), LinkedHashMap("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", None, List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"))))
          val strConfig2 = JSONUtils.serialize(reportConfig2)
          val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig2), "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
          DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
      }

      it should "execute weekly report for successful QR Scans and generate csv reports" in {
          implicit val sqlContext = new SQLContext(sc)
          import scala.concurrent.ExecutionContext.Implicits.global
          implicit val mockDruidConfig = DruidConfig.DefaultConfig
          val mockDruidClient = mock[DruidClient]
          (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).
            returns(Future.apply[DruidResponse](DruidResponseTimeseriesImpl.apply(List(), QueryType.GroupBy))).anyNumberOfTimes();
          (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();

          //        val totalQRscansQuery = DruidQueryModel("timeSeries", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_scans"), "count", ""))), None, Option(List(DruidFilter("isnotnull", "edata_filters_dialcodes", None),DruidFilter("equals", "eid", Option("SEARCH")))))
          val totalSuccessfulQRScansQuery = DruidQueryModel("timeSeries", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_successful_scans"), "count", ""))), None, Option(List(DruidFilter("isnotnull", "edata_filters_dialcodes", None),DruidFilter("equals", "eid", Option("SEARCH")), DruidFilter("greaterthan", "edata_size", Option(0.asInstanceOf[AnyRef])))))
          val totalFailedQRScansQuery = DruidQueryModel("timeSeries", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_failed_scans"), "count", ""))), None, Option(List(DruidFilter("isnotnull", "edata_filters_dialcodes", None),DruidFilter("equals", "eid", Option("SEARCH")), DruidFilter("equals", "edata_size", Option("0".asInstanceOf[AnyRef])))))

          val totalPercentFailedQRScansQuery = DruidQueryModel("timeSeries", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_failed_scans"),"javascript","edata_size",Option("function(current, edata_size) { return current + (edata_size == 0 ? 1 : 0); }"),Option("function(partialA, partialB) { return partialA + partialB; }"),Option("function () { return 0; }")), Aggregation(Option("total_scans"), "count", ""))), None, Option(List(DruidFilter("isnotnull", "edata_filters_dialcodes", None),DruidFilter("equals", "eid", Option("SEARCH")))), None, Option(List(PostAggregation("javascript", "total_percent_failed_scans", PostAggregationFields("total_failed_scans", "total_scans"), "function(total_failed_scans, total_scans) { return (total_failed_scans/total_scans) * 100}"))))

          val totalcontentDownloadQuery = DruidQueryModel("timeSeries", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_content_download"), "count", ""))), None, Option(List(DruidFilter("equals", "edata_subtype", Option("ContentDownload-Success")),DruidFilter("equals", "eid", Option("INTERACT")), DruidFilter("equals", "context_pdata_id", Option("prod.sunbird.app")))))
          val contentPlayedQuery = DruidQueryModel("timeseries", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_content_plays"), "count", ""))), None, Option(List(DruidFilter("equals", "eid", Option("ME_WORKFLOW_SUMMARY")), DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("equals", "dimensions_type", Option("content")), DruidFilter("equals", "dimensions_mode", Option("play")))))
          val uniqueDevicesQuery = DruidQueryModel("timeseries", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_unique_devices"), "cardinality", "dimensions_did"))), None, Option(List(DruidFilter("equals", "dimensions_pdata_id", Option("prod.sunbird.app")),DruidFilter("equals", "dimensions_type", Option("app")))))
          val contentPlayedInHoursQuery = DruidQueryModel("timeseries", "summary-events", "LastDay", None, Option(List(Aggregation(Option("sum__edata_time_spent"), "doubleSum", "edata_time_spent"))), None, Option(List(DruidFilter("equals", "eid", Option("ME_WORKFLOW_SUMMARY")), DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("equals", "dimensions_type", Option("content")), DruidFilter("equals", "dimensions_mode", Option("play")))), None, Option(List(PostAggregation("arithmetic", "total_time_spent_in_hours", PostAggregationFields("sum__edata_time_spent", 3600.asInstanceOf[AnyRef], "constant"), "/" ))))
          val reportConfig2 = ReportConfig("data_metrics", "timeseries",
              QueryDateRange(None, Option("LastDay"), Option("day")),
              List(
                  //                Metrics("totalQRScans", "Total Scans", totalQRscansQuery),
                  Metrics("totalSuccessfulScans", "Total Successful QR Scans", totalSuccessfulQRScansQuery),
                  Metrics("totalFailedScans", "Total Failed QR Scans", totalFailedQRScansQuery),
                  //                Metrics("totalPercentFailedScans", "Total Percent Failed QR Scans", totalPercentFailedQRScansQuery),
                  Metrics("totalContentDownload", "Total Content Download", totalcontentDownloadQuery),
                  Metrics("totalContentPlayed","Total Content Played", contentPlayedQuery),
                  Metrics("totalUniqueDevices","Total Unique Devices", uniqueDevicesQuery),
                  Metrics("totalContentPlayedInHour","Total Content Played In Hour", contentPlayedInHoursQuery)),
              LinkedHashMap(
                  "total_scans" -> "Number of QR Scans",
                  "total_successful_scans" -> "Number of successful QR Scans",
                  "total_failed_scans" -> "Number of failed QR Scans", "total_content_download" -> "Total Content Downloads",
                  "total_percent_failed_scans" -> "Total Percent Failed Scans",
                  "total_content_plays" -> "Total Content Played",
                  "total_unique_devices" -> "Total Unique Devices",
                  "total_time_spent_in_hours" -> "Total time spent in hours"),
              List(OutputConfig("csv", Option("scans"),
                  List("total_scans", "total_successful_scans", "total_failed_scans",
                      "total_percent_failed_scans",
                      "total_content_download", "total_content_plays",
                      "total_unique_devices",
                      "total_time_spent_in_hours"
                  ))))
          val strConfig2 = JSONUtils.serialize(reportConfig2)
          //        val reportConfig = """{"id":"data_metrics","queryType":"groupBy","dateRange":{"staticInterval":"LastDay","granularity":"day"},"metrics":[{"metric":"totalQrScans","label":"Total QR Scans","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastDay","aggregations":[{"name":"total_scans","type":"count"}],"dimensions":[["device_loc_state","state"],["context_pdata_id","producer_id"]],"filters":[{"type":"isnotnull","dimension":"edata_filters_dialcodes"},{"type":"equals","dimension":"eid","value":"SEARCH"}],"descending":"false"}},{"metric":"totalSuccessfulScans","label":"Total Successful QR Scans","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastDay","aggregations":[{"name":"total_successful_scans","type":"count"}],"dimensions":[["device_loc_state","state"],["context_pdata_id","producer_id"]],"filters":[{"type":"isnotnull","dimension":"edata_filters_dialcodes"},{"type":"greaterThan","dimension":"edata_size","value":0},{"type":"equals","dimension":"eid","value":"SEARCH"}],"descending":"false"}},{"metric":"totalfailedQRScans","label":"Total Failed QR Scans","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastDay","aggregations":[{"name":"total_failed_scans","type":"count"}],"dimensions":[["device_loc_state","state"],["context_pdata_id","producer_id"]],"filters":[{"type":"isnotnull","dimension":"edata_filters_dialcodes"},{"type":"equals","dimension":"edata_size","value":0},{"type":"equals","dimension":"eid","value":"SEARCH"}],"descending":"false"}}],"labels":{"state":"State","total_sessions":"Number of Content Plays","producer_id":"Producer","total_scans":"Total Number of QR Scans","total_successful_scans":"Total Number Of Successful QR Scans","total_failed_scans":"Total Number Of Failed QR Scans"},"output":[{"type":"csv","label":"QR Scans","metrics":["total_scans","total_successful_scans","total_failed_scans"],"dims":[],"fileParameters":["id","date"]}]}"""
          val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig2), "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
          DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
      }

      it should "execute desktop metrics" in {
          implicit val sqlContext = new SQLContext(sc)
          import scala.concurrent.ExecutionContext.Implicits.global
          implicit val mockDruidConfig = DruidConfig.DefaultConfig
          val mockDruidClient = mock[DruidClient]
          (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).
            returns(Future.apply[DruidResponse](DruidResponseTimeseriesImpl.apply(List(), QueryType.GroupBy))).anyNumberOfTimes();
          (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();

          val totalContentDownloadDesktopQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay",Option("all"),
              Option(List(Aggregation(Option("total_content_download_on_desktop"), "count", "mid"))),
              Option(List(DruidDimension("content_board", Option("state")))),
              Option(List(
                  DruidFilter("equals", "context_env", Option("downloadManager")),
                  DruidFilter("equals", "edata_state", Option("COMPLETED")),
                  DruidFilter("equals", "context_pdata_id", Option("prod.sunbird.desktop")),
                  DruidFilter("equals", "eid", Option("AUDIT"))
              ))
          )

          val totalContentPlayedDesktopQuery = DruidQueryModel("groupBy", "summary-events", "LastDay",Option("all"),
              Option(List(Aggregation(Option("total_content_plays_on_desktop"), "count", "mid"))),
              Option(List(DruidDimension("collection_board", Option("state")))),
              Option(List(
                  DruidFilter("equals", "eid", Option("ME_WORKFLOW_SUMMARY")),
                  DruidFilter("equals", "dimensions_mode", Option("play")),
                  DruidFilter("equals", "dimensions_type", Option("content")),
                  DruidFilter("equals", "dimensions_pdata_id", Option("prod.sunbird.desktop"))
              ))
          )

          val totalContentPlayedInHourOnDesktopQuery = DruidQueryModel("groupBy", "summary-events", "LastDay",Option("all"),
              Option(List(Aggregation(Option("sum__edata_time_spent"), "doubleSum", "edata_time_spent"))),
              Option(List(DruidDimension("collection_board", Option("state")))),
              Option(List(
                  DruidFilter("equals", "eid", Option("ME_WORKFLOW_SUMMARY")),
                  DruidFilter("equals", "dimensions_mode", Option("play")),
                  DruidFilter("equals", "dimensions_type", Option("content")),
                  DruidFilter("equals", "dimensions_pdata_id", Option("prod.sunbird.desktop"))
              )), None,
              Option(List(
                  PostAggregation("arithmetic", "total_time_spent_in_hours_on_desktop",
                      PostAggregationFields("sum__edata_time_spent", 3600.asInstanceOf[AnyRef], "constant"), "/"
                  )
              ))
          )

          val totalUniqueDevicesPlayedContentOnDesktopQuery = DruidQueryModel("groupBy", "summary-events", "LastDay",
              Option("all"),
              Option(List(Aggregation(Option("total_unique_devices_on_desktop_played_content"), "cardinality", "dimensions_did"))),
              Option(List(DruidDimension("collection_board", Option("state")))),
              Option(List(
                  DruidFilter("equals", "eid", Option("ME_WORKFLOW_SUMMARY")),
                  DruidFilter("equals", "dimensions_mode", Option("play")),
                  DruidFilter("equals", "dimensions_type", Option("content")),
                  DruidFilter("equals", "dimensions_pdata_id", Option("prod.sunbird.desktop"))
              ))
          )

          val reportConfig1 = ReportConfig("Desktop-Consumption-Daily-Reports", "groupBy",
              QueryDateRange(None, Option("LastDay"), Option("day")),
              List(
                  Metrics("totalContentDownloadDesktop", "Total Content Download", totalContentDownloadDesktopQuery),
                  Metrics("totalContentPlayedDesktop", "Total time spent in hours", totalContentPlayedDesktopQuery),
                  Metrics("totalContentPlayedInHourOnDesktop", "Total Content Download", totalContentPlayedInHourOnDesktopQuery),
                  Metrics("totalUniqueDevicesPlayedContentOnDesktop", "Total Unique Devices On Desktop that played content", totalUniqueDevicesPlayedContentOnDesktopQuery)
              ),
              LinkedHashMap(
                  "state" -> "State",
                  "total_content_download_on_desktop" -> "Total Content Downloads",
                  "total_content_plays_on_desktop" -> "Total Content Played",
                  "total_time_spent_in_hours_on_desktop" -> "Total time spent in hours",
                  "total_unique_devices_on_desktop_played_content" -> "Total Unique Devices On Desktop that played content"
              ),
              List(
                  OutputConfig("csv", Option("desktop"),
                      List(
                          "total_content_download_on_desktop",
                          "total_time_spent_in_hours_on_desktop",
                          "total_content_plays_on_desktop",
                          "total_unique_devices_on_desktop_played_content"
                      ),
                      List("state"),
                      List("dims")
                  )
              )
          )
          val strConfig1 = JSONUtils.serialize(reportConfig1)

          val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig1), "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
          DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
      }

      it should "execute weekly report without dimension and generate csv reports" in {
          implicit val sqlContext = new SQLContext(sc)
          import scala.concurrent.ExecutionContext.Implicits.global
          implicit val mockDruidConfig = DruidConfig.DefaultConfig

          val json: String = """
            {
                "total_scans" : 9007,
                "total_sessions" : 100,
                "total_ts" : 120.0
            }
          """
          val doc: Json = parse(json).getOrElse(Json.Null);
          val results = List(DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc));
          val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.Timeseries)

          val mockDruidClient = mock[DruidClient]
          (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes();
          (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();
          (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

          val scansQuery2 = DruidQueryModel("timeSeries", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_scans"), "count", ""))), None, Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
          val contentPlaysQuery2 = DruidQueryModel("timeSeries", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), None, Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
          val reportConfig2 = ReportConfig("", "timeSeries", QueryDateRange(None, Option("LastWeek"), None), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery2), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery2)), LinkedHashMap("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", None, List("total_scans", "total_sessions", "total_ts"))))
          val strConfig2 = JSONUtils.serialize(reportConfig2)
          val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig2), "bucket" -> "test-container", "key" -> "druid-reports/")
          DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
      }

      it should "test for dateRange" in {
          val interval = QueryInterval("2020-05-02", "2020-05-06")
          val result = DruidQueryProcessingModel.getDateRange(interval,0,"telemetry-events")
          result should be ("2020-05-02T05:30:00/2020-05-06T05:30:00")

          val slidingInterval = QueryInterval("2020-05-02", "2020-05-06")
          val slidingIntervalResult = DruidQueryProcessingModel.getDateRange(slidingInterval, 2,"telemetry-events")
          slidingIntervalResult should be ("2020-04-30T05:30:00/2020-05-04T05:30:00")

        val slidingIntervalRollup = QueryInterval("2020-05-02", "2020-05-06")
        val slidingIntervalResultRollup = DruidQueryProcessingModel.getDateRange(slidingIntervalRollup, 2,"summary-rollup-events")
        slidingIntervalResultRollup should be ("2020-04-30T00:00:00/2020-05-04T00:00:00")
          }

      it should "test for setStorageConf method" in {
          DruidQueryProcessingModel.setStorageConf("s3", None, None)
          DruidQueryProcessingModel.setStorageConf("azure", None, None)
      }

      it should "execute multiple queries and generate csv reports on only date as dimensions with dynamic interval" in {
          //        implicit val sc = CommonUtil.getSparkContext(2, "TestDruidQueryProcessingModel", None, None);

          implicit val sqlContext = new SQLContext(sc)
          import scala.concurrent.ExecutionContext.Implicits.global

          val json: String = """
            {
                "total_scans" : 9007,
                "producer_id" : "dev.sunbird.learning.platform",
                "state" : "ka"
            }
          """
          val doc: Json = parse(json).getOrElse(Json.Null);
          val results = List(DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc));
          val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)

          implicit val mockDruidConfig = DruidConfig.DefaultConfig
          val mockDruidClient = mock[DruidClient]
          (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes();
          (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();
          (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

          val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
          val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
          val reportConfig = ReportConfig("consumption_usage_metrics", "groupBy", QueryDateRange(Option(QueryInterval("2020-01-01", "2020-01-07")), None, Option("day")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), LinkedHashMap("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", None, List("total_scans"), List("date"), List("id", "dims"))), Option(ReportMergeConfig(Some("local"),"DAY", "", 1, Option("ACADEMIC_YEAR"), Option("Date"), Option(1), "daily_metrics.csv")))
          val strConfig = JSONUtils.serialize(reportConfig)
          val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "store" -> "local", "container" -> "test-container", "filePath" -> "src/test/resources/druid-reports/", "key" -> "test-reports/")
              DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
      }

      it should "execute multiple queries and generate csv reports on only date as dimensions with out key prefix in config" in {
          //        implicit val sc = CommonUtil.getSparkContext(2, "TestDruidQueryProcessingModel", None, None);
          implicit val sqlContext = new SQLContext(sc)
          import scala.concurrent.ExecutionContext.Implicits.global

          val json: String = """
            {
                "total_scans" : 9007,
                "producer_id" : "dev.sunbird.learning.platform",
                "state" : "ka"
            }
          """
          val doc: Json = parse(json).getOrElse(Json.Null);
          val results = List(DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc));
          val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)

          implicit val mockDruidConfig = DruidConfig.DefaultConfig
          val mockDruidClient = mock[DruidClient]
          (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes();
          (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();
          (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

          val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
          val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
          val reportConfig = ReportConfig("consumption_usage_metrics", "groupBy", QueryDateRange(Option(QueryInterval("2020-01-01", "2020-01-07")), None, Option("day")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), LinkedHashMap("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", None, List("total_scans"), List("date"), List("id", "dims"))), Option(ReportMergeConfig(Some("local"),"DAY", "", 1, Option("ACADEMIC_YEAR"), Option("Date"), Option(1), "daily_metrics.csv")))
          val strConfig = JSONUtils.serialize(reportConfig)
          val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "store" -> "local", "container" -> "test-container", "filePath" -> "src/test/resources/druid-reports/")
          DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));

      }


      it should "execute remove invalid locations" in {

          implicit val sqlContext = new SQLContext(sc)
          import scala.concurrent.ExecutionContext.Implicits.global

        val json: String = """
            {
                "total_sessions" : 2000,
                "total_ts" : 5,
                "district" : "Nellore",
                "state" : "Andhra Pradesh"
            }
          """
          val doc: Json = parse(json).getOrElse(Json.Null);
          val results = List(DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc));
          val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)

          implicit val mockDruidConfig = DruidConfig.DefaultConfig
          val mockDruidClient = mock[DruidClient]
          (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes();
          (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();
          (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();
          val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("derived_loc_state", Option("state")), DruidDimension("derived_loc_district", Option("district"),Option("Extraction"), Option("STRING"),
              Option(List(ExtractFn("registeredLookup","districtLookup"),ExtractFn("javascript", "function(str){return str == null ? null: str.toLowerCase().trim().split(' ').map(function(t){return t.substring(0,1).toUpperCase()+t.substring(1,t.length)}).join(' ')}")))))),
              Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))


          val reportConfig = ReportConfig("test_usage_metrics", "groupBy", QueryDateRange(Option(QueryInterval("2020-05-24", "2020-05-24")), None, Option("day")), List(Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), LinkedHashMap("state" -> "State", "district" -> "District", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", None, List("total_sessions", "total_ts"), List("date"), List("id", "dims"),Some(false))), Option(ReportMergeConfig(Some("local"),"DAY", "", 1, Option("ACADEMIC_YEAR"), Option("Date"), Option(1), "daily_metrics.csv")))
          val strConfig = JSONUtils.serialize(reportConfig)
          val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "store" -> "local", "container" -> "test-container", "filePath" -> "src/test/resources/druid-reports/", "key" -> "test-reports3/")
           DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams))

      }

      it should "execute the stream query" in {

          implicit val sqlContext = new SQLContext(sc)
          val json: String =
              """
            {
                "total_sessions" : 2000,
                "total_ts" : 5,
                "district" : "Nellore",
                "state" : "Andhra Pradesh"
            }
          """
          val doc: Json = parse(json).getOrElse(Json.Null);
          val results = List(DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc));

          implicit val mockDruidConfig = DruidConfig.DefaultConfig
          val mockDruidClient = mock[DruidClient]
          (mockDruidClient.doQueryAsStream(_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Source(results)).anyNumberOfTimes();
          (mockDruidClient.actorSystem _).expects().returning(ActorSystem("TestQuery")).anyNumberOfTimes()
          (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();
          (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();
          val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_sessions"), "count", ""), Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("derived_loc_state", Option("state")), DruidDimension("derived_loc_district", Option("district"), Option("Extraction"), Option("STRING"),
              Option(List(ExtractFn("registeredLookup", "districtLookup"), ExtractFn("javascript", "function(str){return str == null ? null: str.toLowerCase().trim().split(' ').map(function(t){return t.substring(0,1).toUpperCase()+t.substring(1,t.length)}).join(' ')}")))))),
              Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))), DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
          val reportConfig = ReportConfig("test_usage_metrics", "groupBy", QueryDateRange(Option(QueryInterval("2020-05-24", "2020-05-24")), None, Option("day")), List(Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), LinkedHashMap("state" -> "State", "district" -> "District", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", None, List("total_sessions", "total_ts"), List("date"), List("id", "dims"), Some(false))), Option(ReportMergeConfig(Some("local"),"DAY", "", 1, Option("ACADEMIC_YEAR"), Option("Date"), Option(1), "daily_metrics.csv")))
          val strConfig = JSONUtils.serialize(reportConfig)
          val modelParams = Map("quoteColumns" -> List("Content Play Time"),"streamQuery" -> true.asInstanceOf[AnyRef], "reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "store" -> "local", "container" -> "test-container", "filePath" -> "src/test/resources/druid-reports/", "key" -> "test-reports1/")
         DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams)).collect()
      }

      it should "verify DruidOutput operations"  in {
          val json: String =
              """
            {
                "total_sessions" : 2000,
                "total_ts" : 5,
                "district" : "Nellore",
                "state" : "Andhra Pradesh"
            }
          """

          val output = new DruidOutput(JSONUtils.deserialize[Map[String,AnyRef]](json))
          output.size should be(4)
          val output2 =output + ("count" -> 1)
          output2.size should be(5)
          val output3 = output - ("count")
          output3.size should be(4)
          output3.get("total_ts").get should be(5)
      }


      it should "test exhaust query with static interval " in {
          implicit val sqlContext = new SQLContext(sc)

          val sqlQuery = DruidQueryModel("scan", "summary-rollup-syncts", "LastDay", Option("all"),
              None, None, None, None, None, None, Option(List(DruidSQLDimension("state",Option("LOOKUP(derived_loc_state, 'stateSlugLookup')")),
                  DruidSQLDimension("dimensions_pdata_id",None))),None)

          val mockAKkaUtil = mock[AkkaHttpClient]
          val url = String.format("%s://%s:%s%s%s", "http",AppConf.getConfig("druid.rollup.host"),
              AppConf.getConfig("druid.rollup.port"),AppConf.getConfig("druid.url"),"sql")
          val request = HttpRequest(method = HttpMethods.POST,
              uri = url,
              entity = HttpEntity(ContentTypes.`application/json`, JSONUtils.serialize(DruidDataFetcher.getSQLDruidQuery(sqlQuery))))
          val stripString =
              """
                {"dimensions_pdata_id":"dev.portal", "state":"10","date":"2020-09-12","total_count":11}
                {"dimensions_pdata_id":"dev.app", "state":"5","date":"2020-09-12","total_count" :10}
              """.stripMargin
          val mockDruidClient = mock[DruidClient]
          (mockDruidClient.actorSystem _).expects().returning(ActorSystem("TestQuery")).anyNumberOfTimes()
          (fc.getDruidRollUpClient _).expects().returns(mockDruidClient).anyNumberOfTimes();
          (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();
          (mockAKkaUtil.sendRequest(_: HttpRequest)(_: ActorSystem))
            .expects(request,mockDruidClient.actorSystem)
            .returns(Future.successful(HttpResponse(entity = HttpEntity(ByteString(stripString))))).anyNumberOfTimes();

          (fc.getAkkaHttpUtil _).expects().returns(mockAKkaUtil).once()

            val reportConfig = ReportConfig("test_usage_metrics", "sql", QueryDateRange(None, Option("LastDay"), Option("day")), List(Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", sqlQuery)), LinkedHashMap(), List(OutputConfig("csv", None, List("total_count"), List("date"), List("id", "dims"), Some(false))), Option(ReportMergeConfig(Some("local"),"DAY", "", 1, Option("ACADEMIC_YEAR"), Option("Date"), Option(1), "daily_metrics.csv")))
          val strConfig = JSONUtils.serialize(reportConfig)
          val modelParams = Map("exhaustQuery" -> true.asInstanceOf[AnyRef], "reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "store" -> "local", "container" -> "test-container", "filePath" -> "src/test/resources/druid-reports/", "key" -> "test-reports2/")
           DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams)).collect()
      }
  it should "test exhaust query with interval range " in {
    implicit val sqlContext = new SQLContext(sc)

    val sqlQuery = DruidQueryModel("scan", "summary-rollup-syncts", "2020-08-23T00:00:00+00:00/2020-08-24T00:00:00+00:00", Option("all"),
      None, None, None, None, None, None, Option(List(DruidSQLDimension("state",Option("LOOKUP(derived_loc_state, 'stateSlugLookup')")),
        DruidSQLDimension("dimensions_pdata_id",None))),None)

    val mockAKkaUtil = mock[AkkaHttpClient]
    val url = String.format("%s://%s:%s%s%s", "http",AppConf.getConfig("druid.rollup.host"),
      AppConf.getConfig("druid.rollup.port"),AppConf.getConfig("druid.url"),"sql")
    val request = HttpRequest(method = HttpMethods.POST,
      uri = url,
      entity = HttpEntity(ContentTypes.`application/json`, JSONUtils.serialize(DruidDataFetcher.getSQLDruidQuery(sqlQuery))))
    val stripString =
      """
                {"dimensions_pdata_id":"dev.portal", "state":"10","date":"2020-09-12","total_count":11}
                {"dimensions_pdata_id":"dev.app", "state":"5","date":"2020-09-12","total_count" :10}
              """.stripMargin
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("TestQuery")).anyNumberOfTimes()
    (fc.getDruidRollUpClient _).expects().returns(mockDruidClient).anyNumberOfTimes();
    (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();
    (mockAKkaUtil.sendRequest(_: HttpRequest)(_: ActorSystem))
      .expects(request,mockDruidClient.actorSystem)
      .returns(Future.successful(HttpResponse(entity = HttpEntity(ByteString(stripString))))).anyNumberOfTimes();

    (fc.getAkkaHttpUtil _).expects().returns(mockAKkaUtil).once()

    val reportConfig = ReportConfig("test_usage_metrics", "sql", QueryDateRange(Option(QueryInterval("2020-08-23", "2020-08-24")), None, Option("day")), List(Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", sqlQuery)), LinkedHashMap(), List(OutputConfig("csv", None, List("total_count"), List("date"), List("id", "dims"), Some(false))), Option(ReportMergeConfig(Some("local"),"DAY", "", 1, Option("ACADEMIC_YEAR"), Option("Date"), Option(1), "daily_metrics.csv")))
    val strConfig = JSONUtils.serialize(reportConfig)
    val modelParams = Map("exhaustQuery" -> true.asInstanceOf[AnyRef], "reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "store" -> "local", "container" -> "test-container", "filePath" -> "src/test/resources/druid-reports/", "key" -> "test-reports2/")
    DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams)).collect()
  }
  it should "test exhaust query zip  " in {
    implicit val sqlContext = new SQLContext(sc)

    val sqlQuery = DruidQueryModel("scan", "summary-rollup-syncts", "2020-08-23T00:00:00+00:00/2020-08-24T00:00:00+00:00", Option("all"),
      None, None, None, None, None, None, Option(List(DruidSQLDimension("state", Option("LOOKUP(derived_loc_state, 'stateSlugLookup')")),
        DruidSQLDimension("dimensions_pdata_id", None))), None)

    val mockAKkaUtil = mock[AkkaHttpClient]
    val url = String.format("%s://%s:%s%s%s", "http", AppConf.getConfig("druid.rollup.host"),
      AppConf.getConfig("druid.rollup.port"), AppConf.getConfig("druid.url"), "sql")
    val request = HttpRequest(method = HttpMethods.POST,
      uri = url,
      entity = HttpEntity(ContentTypes.`application/json`, JSONUtils.serialize(DruidDataFetcher.getSQLDruidQuery(sqlQuery))))
    val stripString =
      """
                {"dimensions_pdata_id":"dev.portal", "state":"10","date":"2020-09-12","total_count":11}
                {"dimensions_pdata_id":"dev.app", "state":"5","date":"2020-09-12","total_count" :10}
                {"dimensions_pdata_id":"dev.check", "state":"5","date":"2020-09-12","total_count" :9}
              """.stripMargin
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("TestQuery")).anyNumberOfTimes()
    (fc.getDruidRollUpClient _).expects().returns(mockDruidClient).anyNumberOfTimes();
    (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();
    (mockAKkaUtil.sendRequest(_: HttpRequest)(_: ActorSystem))
      .expects(request, mockDruidClient.actorSystem)
      .returns(Future.successful(HttpResponse(entity = HttpEntity(ByteString(stripString))))).anyNumberOfTimes();

    (fc.getAkkaHttpUtil _).expects().returns(mockAKkaUtil).anyNumberOfTimes()
    (fc.getStorageService(_:String,_:String,_:String)).expects("azure","azure_storage_key","azure_storage_secret")
      .returns(mock[IStorageService]).anyNumberOfTimes()


    val reportConfig = ReportConfig("test_usage_metrics", "sql", QueryDateRange(Option(QueryInterval("2020-08-23", "2020-08-24")),
      None, Option("day")), List(Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", sqlQuery)),
      LinkedHashMap(), List(OutputConfig("csv", None, List("total_count"), List("date"), List("id", "dims"), Some(false), Some(true))),
      None)
    val strConfig = JSONUtils.serialize(reportConfig)
    val modelParams = Map("exhaustQuery" -> true.asInstanceOf[AnyRef],
      "reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig),
      "store" -> "local", "container" -> "test-container", "filePath" -> "src/test/resources/druid-reports/", "key" -> "exhaust-reports/")
    DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams)).collect()
    val reportConfig1 = ReportConfig("test_usage_metrics", "sql", QueryDateRange(Option(QueryInterval("2020-08-23", "2020-08-24")),
      None, Option("day")), List(Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", sqlQuery)),
      LinkedHashMap(), List(OutputConfig("csv", None, List("total_count"), List("date"), List("id", "dims"), Some(false), Some(true))),
      None)
    val strConfig1 = JSONUtils.serialize(reportConfig1)
    val modelParams1 = Map("exhaustQuery" -> true.asInstanceOf[AnyRef],
      "reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig1),
      "store" -> "azure", "container" -> "test-container", "filePath" -> "src/test/resources/druid-reports/", "key" -> "exhaust-reports/")
    an[Exception] should be thrownBy {
      DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams1)).collect()
    }
  }

  it should "test report output config " in {
    implicit val sqlContext = new SQLContext(sc)
    val mockRestUtil = mock[HTTPClient]
    val request ="{\"request\": {\"filters\": {\"type\" :[\"state\",\"district\"]},\"limit\" : 10000}}"
    val response =
      s"""{
    "id": "api.location.search",
    "ver": "v1",
    "ts": "2020-05-28 15:48:24:648+0000",
    "params": {
        "resmsgid": null,
        "msgid": null,
        "err": null,
        "status": "success",
        "errmsg": null
    },
    "responseCode": "OK",
    "result": {
        "response": [
            {
                "code": "28",
                "name": "Andhra Pradesh",
                "id": "0393395d-ea39-49e0-8324-313b4df4a550",
                "type": "state"
            },{
                 "code": "2813",
                 "name": "Visakhapatnam",
                 "id": "884b1221-03c5-4fbb-8b0b-4309c683d682",
                 "type": "district",
                 "parentId": "0393395d-ea39-49e0-8324-313b4df4a550"
                 },
                 {
                 "code": "2813",
                 "name": "East Godavari",
                 "id": "f460be3e-340b-4fde-84db-cab1985efd6c",
                 "type": "district",
                 "parentId": "0393395d-ea39-49e0-8324-313b4df4a550"
                 },{
                               "code": "2",
                               "name": "West Bengal",
                               "id": "0393395d-ea39-49e0-8324-313b4df4a551",
                               "type": "state"
                           },
                {
                                "code": "287",
                                "name": "Koltata",
                                "id": "f460be3e-340b-4fde-84db-cab1985efdfe",
                                "type": "district",
                                "parentId": "0393395d-ea39-49e0-8324-313b4df4a551"
                                }],
             "count": 5
            }
          }"""
    val stateResponse =
      s"""
               {
                 "version": "v1",
                 "lookupExtractorFactory": {
                   "type": "map",
                   "map": {
                     "Andhra Pradesh": "apekx",
                     "Assam": "as",
                     "Bihar": "br"
                   }
                 }
               }
             """.stripMargin
    (mockRestUtil.get[StateLookup](_: String, _: Option[Map[String, String]])(_: Manifest[StateLookup]))
      .expects(AppConf.getConfig("druid.state.lookup.url"), None,manifest[StateLookup])
      .returns(JSONUtils.deserialize[StateLookup](stateResponse))
    (mockRestUtil.post[LocationResponse](_: String, _: String, _: Option[Map[String, String]])(_: Manifest[LocationResponse]))
      .expects(AppConf.getConfig("location.search.url"), request, Some(Map("Authorization" -> AppConf.getConfig("location.search.token"))), manifest[LocationResponse])
      .returns(JSONUtils.deserialize[LocationResponse](response))
    val accumulator = sc.longAccumulator("DruidReportCount")
    val rdd = sc.parallelize(List(DruidOutput.apply(JSONUtils.deserialize[Map[String,AnyRef]]("""{"dimensions_pdata_id":"dev.portal","district":"Visakhapatnam", "state":"apekx","date":"2020-09-12","total_count":11}"""))))
    val config = OutputConfig("csv", None, List("total_count"), List("date"), List("id", "dims"), Some(true))
      DruidQueryProcessingModel.getReportDF(mockRestUtil,config,rdd,accumulator).collect().length should be (1)
  }

}
