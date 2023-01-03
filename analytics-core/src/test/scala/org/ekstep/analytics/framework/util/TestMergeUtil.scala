package org.ekstep.analytics.framework.util

import java.util.Date
import org.apache.hadoop.fs.azure.AzureException
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.framework._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import org.sunbird.cloud.storage.BaseStorageService

class TestMergeUtil extends SparkSpec with Matchers with MockFactory {

  "MergeUtil" should "test the merge function" in {

    implicit val fc = new FrameworkContext
    val mergeUtil = new MergeUtil()

    val config =
      """{"type":"local","id":"consumption_usage_metrics","frequency":"DAY","basePath":"","rollup":1,"rollupAge":"ACADEMIC_YEAR",
        |"rollupCol":"Date","rollupFormat": "dd-MM-yyyy","rollupRange":1,"merge":{"files":[{"reportPath":"src/test/resources/report.csv",
        |"deltaPath":"src/test/resources/delta.csv"}],
        |"dims":["Date"]},"container":"test-container","postContainer":null,"deltaFileAccess":true,"reportFileAccess":true}""".stripMargin


    mergeUtil.mergeFile(JSONUtils.deserialize[MergeConfig](config))

  }

  "MergeUtil" should "test the merge without date function" in {

    implicit val fc = new FrameworkContext
    val mergeUtil = new MergeUtil()

    val config =
      """{"type":"local","id":"consumption_usage_metrics","frequency":"DAY","basePath":"","rollup":0,"rollupAge":"ACADEMIC_YEAR",
        |"rollupCol":"Date","rollupFormat": "dd-MM-yyyy","rollupRange":1,"merge":{"files":[{"reportPath":"src/test/resources/report_withoutdate.csv",
        |"deltaPath":"src/test/resources/delta_withoutdate.csv"}],
        |"dims":["Date"]},"container":"test-container","postContainer":null,"deltaFileAccess":true,"reportFileAccess":true}""".stripMargin


    mergeUtil.mergeFile(JSONUtils.deserialize[MergeConfig](config))

  }

  "MergeUtil" should "test the column order in the merge function" in {

    implicit val fc = new FrameworkContext
    val mergeUtil = new MergeUtil()

    val config =
      """{"type":"local","id":"consumption_usage_metrics","frequency":"DAY","basePath":"","rollup":0,"rollupAge":"ACADEMIC_YEAR",
        |"rollupCol":"Date","rollupFormat": "yyyy-MM-dd","rollupRange":1,"merge":{"files":[{"reportPath":"src/test/resources/report_order.csv",
        |"deltaPath":"src/test/resources/delta_order.csv"}],
        |"dims":["Date"]},"container":"test-container","postContainer":null,"deltaFileAccess":true,"reportFileAccess":true,
        |"columnOrder":["Date","Producer","State","Number of Successful QR Scans"]}""".stripMargin

    mergeUtil.mergeFile(JSONUtils.deserialize[MergeConfig](config))

    val config1 =
      """{"type":"local","id":"consumption_usage_metrics","frequency":"DAY","basePath":"","rollup":0,"rollupAge":"ACADEMIC_YEAR",
        |"rollupCol":"Date","rollupFormat": "yyyy-MM-dd","rollupRange":1,"merge":{"files":[{"reportPath":"src/test/resources/report_date.csv",
        |"deltaPath":"src/test/resources/delta_order.csv"}],
        |"dims":["Date"]},"container":"test-container","postContainer":null,"deltaFileAccess":true,"reportFileAccess":true,
        |"columnOrder":["Date","Producer","State","Number of Successful QR Scans"],"dateRequired":false,"metricLabels":["Number of Successful QR Scans"]}""".stripMargin

    mergeUtil.mergeFile(JSONUtils.deserialize[MergeConfig](config1))

    val config2 =
      """{"type":"local","id":"consumption_usage_metrics","frequency":"DAY","basePath":"","rollup":1,"rollupAge":"ACADEMIC_YEAR",
        |"rollupCol":"Data as of Last Sunday(test)","rollupFormat": "dd-MM-yyyy","rollupRange":1,"merge":{"files":[{"reportPath":"src/test/resources/report_weekly.csv",
        |"deltaPath":"src/test/resources/delta.csv"}],
        |"dims":["Date"]},"container":"test-container","postContainer":null,"deltaFileAccess":true,"reportFileAccess":true,
        |"columnOrder":["Date","Producer","State","Number of Successful QR Scans"],"dateRequired":false,"metricLabels":["Number of Successful QR Scans"]}""".stripMargin

    mergeUtil.mergeFile(JSONUtils.deserialize[MergeConfig](config2))
  }

  "MergeUtil" should "test the azure merge function" in {

    implicit val mockFc = mock[FrameworkContext]
    val mockStorageService = mock[BaseStorageService]
    val mergeUtil = new MergeUtil()
    val config =
      """{"id":"daily_metrics.csv","frequency":"DAY","basePath":"/mount/data/analytics/tmp","rollup":1,"rollupAge":"ACADEMIC_YEAR",
        |"rollupCol":"Date","rollupFormat": "dd-MM-yyyy","rollupRange":2,"merge":{"files":[{"reportPath":"apekx/daily_metrics.csv",
        |"deltaPath":"druid-reports/ETB-Consumption-Daily-Reports/apekx/2020-11-03.csv"},{"reportPath":"apekx/daily_metrics1.csv",
        |"deltaPath":"druid-reports/ETB-Consumption-Daily-Reports/apekx/2020-11-03.csv"}],"dims":["Date"]},"container":"reports",
        |"postContainer":"test-container","deltaFileAccess":true,"reportFileAccess":false}""".stripMargin
    val jsonConfig = JSONUtils.deserialize[MergeConfig](config)
      (mockFc.getStorageService(_:String, _:String, _:String):BaseStorageService).expects("azure", "azure_storage_key", "azure_storage_secret").returns(mockStorageService).anyNumberOfTimes()
    (mockStorageService.searchObjects _).expects(jsonConfig.container,"druid-reports/ETB-Consumption-Daily-Reports/apekx/2020-11-03.csv",None,None,None,"yyyy-MM-dd").returns(null).anyNumberOfTimes()
    (mockStorageService.getPaths _).expects(jsonConfig.container, null).returns(List("src/test/resources/delta.csv")).anyNumberOfTimes()
    (mockFc.getStorageService(_:String, _:String, _:String):BaseStorageService).expects("azure", "druid_storage_account_key", "druid_storage_account_secret").returns(mockStorageService).anyNumberOfTimes()
    (mockStorageService.searchObjects _).expects(jsonConfig.postContainer.get,"apekx/daily_metrics.csv",None,None,None,"yyyy-MM-dd").returns(null)
    (mockStorageService.getPaths _).expects(jsonConfig.postContainer.get, null).returns(List("src/test/resources/report.csv"))
    (mockStorageService.searchObjects _).expects(jsonConfig.postContainer.get,"apekx/daily_metrics1.csv",None,None,None,"yyyy-MM-dd").returns(null)
    (mockStorageService.getPaths _).expects(jsonConfig.postContainer.get, null).returns(List())
     mergeUtil.mergeFile(JSONUtils.deserialize[MergeConfig](config))
  }

  "MergeUtil" should "test the azure merge if report is not available" in {

    implicit val mockFc = mock[FrameworkContext]
    val mockStorageService = mock[BaseStorageService]
    val mergeUtil = new MergeUtil()
    val config =
      """{"type":"azure","id":"daily_metrics.csv","frequency":"DAY","basePath":"/mount/data/analytics/tmp","rollup":1,"rollupAge":"ACADEMIC_YEAR",
        |"rollupCol":"Date","rollupFormat": "dd-MM-yyyy","rollupRange":2,"merge":{"files":[{"reportPath":"apekx/daily_metrics.csv",
        |"deltaPath":"druid-reports/ETB-Consumption-Daily-Reports/apekx/2020-11-03.csv"}],"dims":["Date"]},"container":"reports",
        |"deltaFileAccess":true,"reportFileAccess":true}""".stripMargin
    val jsonConfig = JSONUtils.deserialize[MergeConfig](config)
    (mockFc.getStorageService(_:String, _:String, _:String):BaseStorageService).expects("azure", "azure_storage_key", "azure_storage_secret").returns(mockStorageService)
    (mockStorageService.searchObjects _).expects(jsonConfig.container,"druid-reports/ETB-Consumption-Daily-Reports/apekx/2020-11-03.csv",None,None,None,"yyyy-MM-dd").returns(null)
    (mockStorageService.getPaths _).expects(jsonConfig.container, null).returns(List("src/test/resources/delta.csv"))
    (mockFc.getStorageService(_:String, _:String, _:String):BaseStorageService).expects("azure", "azure_storage_key", "azure_storage_secret").returns(mockStorageService)
    (mockStorageService.searchObjects _).expects("report-verification","apekx/daily_metrics.csv",None,None,None,"yyyy-MM-dd").returns(null)
    (mockStorageService.getPaths _).expects("report-verification", null).returns(List())
    mergeUtil.mergeFile(JSONUtils.deserialize[MergeConfig](config))
  }

  "MergeUtil" should "test the exception case" in {

    implicit val mockFc = mock[FrameworkContext]
    val mergeUtil = new MergeUtil()

    val config =
      """{"type":"blob","id":"daily_metrics.csv","frequency":"DAY","basePath":"/mount/data/analytics/tmp","rollup":1,"rollupAge":"ACADEMIC_YEAR",
        |"rollupCol":"Date","rollupFormat": "dd-MM-yyyy","rollupRange":1,"merge":{"files":[{"reportPath":"apekx/daily_metrics.csv",
        |"deltaPath":"druid-reports/ETB-Consumption-Daily-Reports/apekx/2020-11-03.csv"}],"dims":["Date"]},"container":"reports",
        |"postContainer":"test-container","deltaFileAccess":true,"reportFileAccess":true}""".stripMargin

    a[Exception] should be thrownBy {
      mergeUtil.mergeFile(JSONUtils.deserialize[MergeConfig](config))
    }
  }

  "MergeUtil" should "test all rollup conditions" in {

    implicit val mockFc = mock[FrameworkContext]
    val mergeUtil = new MergeUtil()

    val config =
      """{"type":"local","id":"consumption_usage_metrics","frequency":"DAY","basePath":"","rollup":1,"rollupAge":"ACADEMIC_YEAR",
        |"rollupCol":"Date","rollupFormat": "dd-MM-yyyy","rollupRange":2,"merge":{"files":[{"reportPath":"src/test/resources/report.csv",
        |"deltaPath":"src/test/resources/delta.csv"}],
        |"dims":["Date"]},"container":"test-container","postContainer":null,"deltaFileAccess":true,"reportFileAccess":true}""".stripMargin
    implicit val sqlContext = new SQLContext(sc)
    val deltaDF = sqlContext.read.options(Map("header" -> "true")).csv("src/test/resources/delta_rollup.csv")
    val reportDF = sqlContext.read.options(Map("header" -> "true")).csv("src/test/resources/report_rollup.csv")
    val configMap  =JSONUtils.deserialize[MergeConfig](config)
    val rollupCol= configMap.rollupCol.getOrElse("Date")
    val rollupFormat= configMap.rollupFormat.getOrElse("dd-MM-yyyy")
    mergeUtil.mergeReport(rollupCol,rollupFormat,deltaDF,reportDF,JSONUtils.deserialize[MergeConfig](config), List("Date")).count should be(10)
    val config1 =
      """{"type":"local","id":"consumption_usage_metrics","frequency":"DAY","basePath":"","rollup":1,"rollupAge":"GEN_YEAR",
        |"rollupCol":"Date","rollupFormat": "dd-MM-yyyy","rollupRange":1,"merge":{"files":[{"reportPath":"src/test/resources/report.csv",
        |"deltaPath":"src/test/resources/delta.csv"}],
        |"dims":["Date"]},"container":"test-container","postContainer":null,"deltaFileAccess":true,"reportFileAccess":true}""".stripMargin
    mergeUtil.mergeReport(rollupCol,rollupFormat,deltaDF,reportDF,JSONUtils.deserialize[MergeConfig](config1),List("Date")).count should be(9)
    val config2 =
      """{"type":"local","id":"consumption_usage_metrics","frequency":"DAY","basePath":"","rollup":1,"rollupAge":"MONTH",
        |"rollupCol":"Date","rollupFormat": "dd-MM-yyyy","rollupRange":1,"merge":{"files":[{"reportPath":"src/test/resources/report.csv",
        |"deltaPath":"src/test/resources/delta.csv"}],
        |"dims":["Date"]},"container":"test-container","postContainer":null,"deltaFileAccess":true,"reportFileAccess":true}""".stripMargin
    mergeUtil.mergeReport(rollupCol,rollupFormat,deltaDF,reportDF,JSONUtils.deserialize[MergeConfig](config2),List("Date")).count should be(8)
    val config3 =
      """{"type":"local","id":"consumption_usage_metrics","frequency":"DAY","basePath":"","rollup":1,"rollupAge":"WEEK",
        |"rollupCol":"Date","rollupFormat": "dd-MM-yyyy","rollupRange":1,"merge":{"files":[{"reportPath":"src/test/resources/report.csv",
        |"deltaPath":"src/test/resources/delta.csv"}],
        |"dims":["Date"]},"container":"test-container","postContainer":null,"deltaFileAccess":true,"reportFileAccess":true}""".stripMargin
    mergeUtil.mergeReport(rollupCol,rollupFormat,deltaDF,reportDF,JSONUtils.deserialize[MergeConfig](config3),List("Date")).count should be(7)

    val config4 =
      """{"type":"local","id":"consumption_usage_metrics","frequency":"DAY","basePath":"","rollup":1,"rollupAge":"DAY",
        |"rollupCol":"Date","rollupFormat": "dd-MM-yyyy","rollupRange":4,"merge":{"files":[{"reportPath":"src/test/resources/report.csv",
        |"deltaPath":"src/test/resources/delta.csv"}],
        |"dims":["Date"]},"container":"test-container","postContainer":null,"deltaFileAccess":true,"reportFileAccess":true}""".stripMargin
    mergeUtil.mergeReport(rollupCol,rollupFormat,deltaDF,reportDF,JSONUtils.deserialize[MergeConfig](config4),List("Date")).count should be(4)
    val config5 =
      """{"type":"local","id":"consumption_usage_metrics","frequency":"DAY","basePath":"","rollup":1,"rollupAge":"None",
        |"rollupRange":4,"rollupFormat": "dd-MM-yyyy","merge":{"files":[{"reportPath":"src/test/resources/report.csv",
        |"deltaPath":"src/test/resources/delta.csv"}],
        |"dims":["Date"]},"container":"test-container","postContainer":null,"deltaFileAccess":true,"reportFileAccess":true}""".stripMargin
    mergeUtil.mergeReport(rollupCol,rollupFormat,deltaDF,reportDF,JSONUtils.deserialize[MergeConfig](config5),List("Date","State")).count should be(11)
  }

  "MergeUtil" should "test without rollup condition" in {

    implicit val mockFc = mock[FrameworkContext]
    val mergeUtil = new MergeUtil()

    val config =
      """{"type":"local","id":"consumption_usage_metrics","frequency":"DAY","basePath":"","rollup":0,"rollupAge":"ACADEMIC_YEAR",
        |"rollupCol":"Date","rollupFormat": "dd-MM-yyyy","rollupRange":2,"merge":{"files":[{"reportPath":"src/test/resources/report.csv",
        |"deltaPath":"src/test/resources/delta.csv"}],
        |"dims":["Date"]},"container":"test-container","postContainer":null,"deltaFileAccess":true,"reportFileAccess":true}""".stripMargin
    implicit val sqlContext = new SQLContext(sc)
    val deltaDF = sqlContext.read.options(Map("header" -> "true")).csv("src/test/resources/delta_rollup.csv")
    val reportDF = sqlContext.read.options(Map("header" -> "true")).csv("src/test/resources/report_rollup.csv")
    mergeUtil.mergeReport("Date","dd-MM-yyyy",deltaDF,reportDF,JSONUtils.deserialize[MergeConfig](config),List("Date")).count should be(1)

  }

  "MergeUtil" should "test the else conditions function" in {

    implicit val fc = new FrameworkContext
    val mergeUtil = new MergeUtil()
    val fileUtil = new HadoopFileUtil();
    val config =
      """{"type":"local","id":"consumption_usage_metrics","frequency":"DAY","basePath":"","rollup":1,"rollupAge":"ACADEMIC_YEAR",
        |"rollupCol":"Date","rollupRange":1,"merge":{"files":[{"reportPath":"src/test/resources/report_test.csv",
        |"deltaPath":"src/test/resources/delta_test.csv"}],
        |"dims":["Date"]},"container":"test-container","postContainer":null,"deltaFileAccess":true,"reportFileAccess":true}""".stripMargin


    mergeUtil.mergeFile(JSONUtils.deserialize[MergeConfig](config))
    val config1 =
      """{"type":"local","id":"consumption_usage_metrics","frequency":"DAY","basePath":"","rollup":1,"rollupAge":"ACADEMIC_YEAR",
        |"rollupCol":"Date||%Y-%m-%d","rollupRange":1,"merge":{"files":[{"reportPath":"src/test/resources/report_test.csv",
        |"deltaPath":"src/test/resources/delta_test.csv"}],
        |"dims":["Date"]},"container":"test-container","postContainer":null,"deltaFileAccess":true,"reportFileAccess":true}""".stripMargin


    mergeUtil.mergeFile(JSONUtils.deserialize[MergeConfig](config1))
    fileUtil.delete(sc.hadoopConfiguration, "src/test/resources/report_test.csv","src/test/resources/report_test.json")

  }

}