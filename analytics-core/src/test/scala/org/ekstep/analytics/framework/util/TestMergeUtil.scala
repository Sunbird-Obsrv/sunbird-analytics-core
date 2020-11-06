package org.ekstep.analytics.framework.util

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
              |"rollupCol":"Date","rollupRange":1,"merge":{"files":[{"reportPath":"src/test/resources/report.csv",
              |"deltaPath":"src/test/resources/delta.csv"}],
              |"dims":["Date"]},"container":"test-container","postContainer":null,"deltaFileAccess":true,"reportFileAccess":true}""".stripMargin


        mergeUtil.mergeFile(JSONUtils.deserialize[MergeScriptConfig](config))

    }


    "MergeUtil" should "test the azure merge function" in {

        implicit val mockFc = mock[FrameworkContext]
        val mockStorageService = mock[BaseStorageService]
        val mergeUtil = new MergeUtil()

        val config =
            """{"type":"azure","id":"daily_metrics.csv","frequency":"DAY","basePath":"/mount/data/analytics/tmp","rollup":1,"rollupAge":"ACADEMIC_YEAR",
              |"rollupCol":"Date","rollupRange":1,"merge":{"files":[{"reportPath":"apekx/daily_metrics.csv",
              |"deltaPath":"druid-reports/ETB-Consumption-Daily-Reports/apekx/2020-11-03.csv"}],"dims":["Date"]},"container":"reports",
              |"postContainer":"test-container","deltaFileAccess":true,"reportFileAccess":false}""".stripMargin
        val jsonConfig = JSONUtils.deserialize[MergeScriptConfig](config)
        (mockFc.getStorageService(_:String, _:String, _:String):BaseStorageService).expects("azure", "azure_storage_key", "azure_storage_secret").returns(mockStorageService)
        (mockStorageService.searchObjects _).expects(jsonConfig.container,"druid-reports/ETB-Consumption-Daily-Reports/apekx/2020-11-03.csv",None,None,None,"yyyy-MM-dd").returns(null)
        (mockStorageService.getPaths _).expects(jsonConfig.container, null).returns(List("src/test/resources/delta.csv"))
        (mockFc.getStorageService(_:String, _:String, _:String):BaseStorageService).expects("azure", "report_storage_key", "report_storage_secret").returns(mockStorageService)
        (mockStorageService.searchObjects _).expects(jsonConfig.postContainer.get,"apekx/daily_metrics.csv",None,None,None,"yyyy-MM-dd").returns(null)
        (mockStorageService.getPaths _).expects(jsonConfig.postContainer.get, null).returns(List("src/test/resources/report.csv"))
        a[AzureException] should be thrownBy {
            mergeUtil.mergeFile(JSONUtils.deserialize[MergeScriptConfig](config))
        }
    }


    "MergeUtil" should "test the exception case" in {

        implicit val mockFc = mock[FrameworkContext]
        val mergeUtil = new MergeUtil()

        val config =
            """{"type":"blob","id":"daily_metrics.csv","frequency":"DAY","basePath":"/mount/data/analytics/tmp","rollup":1,"rollupAge":"ACADEMIC_YEAR",
              |"rollupCol":"Date","rollupRange":1,"merge":{"files":[{"reportPath":"apekx/daily_metrics.csv",
              |"deltaPath":"druid-reports/ETB-Consumption-Daily-Reports/apekx/2020-11-03.csv"}],"dims":["Date"]},"container":"reports",
              |"postContainer":"test-container","deltaFileAccess":true,"reportFileAccess":true}""".stripMargin

        a[Exception] should be thrownBy {
            mergeUtil.mergeFile(JSONUtils.deserialize[MergeScriptConfig](config))
        }
    }

        "MergeUtil" should "test all rollup conditions" in {

            implicit val mockFc = mock[FrameworkContext]
            val mergeUtil = new MergeUtil()

            val config =
                """{"type":"local","id":"consumption_usage_metrics","frequency":"DAY","basePath":"","rollup":1,"rollupAge":"ACADEMIC_YEAR",
                  |"rollupCol":"Date","rollupRange":2,"merge":{"files":[{"reportPath":"src/test/resources/report.csv",
                  |"deltaPath":"src/test/resources/delta.csv"}],
                  |"dims":["Date"]},"container":"test-container","postContainer":null,"deltaFileAccess":true,"reportFileAccess":true}""".stripMargin
            implicit val sqlContext = new SQLContext(sc)
            val deltaDF = sqlContext.read.options(Map("header" -> "true")).csv("src/test/resources/delta_rollup.csv")
            val reportDF = sqlContext.read.options(Map("header" -> "true")).csv("src/test/resources/report_rollup.csv")
            mergeUtil.mergeReport(deltaDF,reportDF,JSONUtils.deserialize[MergeScriptConfig](config)).count should be(10)
            val config1 =
                """{"type":"local","id":"consumption_usage_metrics","frequency":"DAY","basePath":"","rollup":1,"rollupAge":"GEN_YEAR",
                  |"rollupCol":"Date","rollupRange":1,"merge":{"files":[{"reportPath":"src/test/resources/report.csv",
                  |"deltaPath":"src/test/resources/delta.csv"}],
                  |"dims":["Date"]},"container":"test-container","postContainer":null,"deltaFileAccess":true,"reportFileAccess":true}""".stripMargin
            mergeUtil.mergeReport(deltaDF,reportDF,JSONUtils.deserialize[MergeScriptConfig](config1)).count should be(9)
            val config2 =
                """{"type":"local","id":"consumption_usage_metrics","frequency":"DAY","basePath":"","rollup":1,"rollupAge":"MONTH",
                  |"rollupCol":"Date","rollupRange":1,"merge":{"files":[{"reportPath":"src/test/resources/report.csv",
                  |"deltaPath":"src/test/resources/delta.csv"}],
                  |"dims":["Date"]},"container":"test-container","postContainer":null,"deltaFileAccess":true,"reportFileAccess":true}""".stripMargin
            mergeUtil.mergeReport(deltaDF,reportDF,JSONUtils.deserialize[MergeScriptConfig](config2)).count should be(8)
            val config3 =
                """{"type":"local","id":"consumption_usage_metrics","frequency":"DAY","basePath":"","rollup":1,"rollupAge":"WEEK",
                  |"rollupCol":"Date","rollupRange":1,"merge":{"files":[{"reportPath":"src/test/resources/report.csv",
                  |"deltaPath":"src/test/resources/delta.csv"}],
                  |"dims":["Date"]},"container":"test-container","postContainer":null,"deltaFileAccess":true,"reportFileAccess":true}""".stripMargin
            mergeUtil.mergeReport(deltaDF,reportDF,JSONUtils.deserialize[MergeScriptConfig](config3)).count should be(7)

            val config4 =
                """{"type":"local","id":"consumption_usage_metrics","frequency":"DAY","basePath":"","rollup":1,"rollupAge":"DAY",
                  |"rollupCol":"Date","rollupRange":4,"merge":{"files":[{"reportPath":"src/test/resources/report.csv",
                  |"deltaPath":"src/test/resources/delta.csv"}],
                  |"dims":["Date"]},"container":"test-container","postContainer":null,"deltaFileAccess":true,"reportFileAccess":true}""".stripMargin
            mergeUtil.mergeReport(deltaDF,reportDF,JSONUtils.deserialize[MergeScriptConfig](config4)).count should be(4)
            val config5 =
                """{"type":"local","id":"consumption_usage_metrics","frequency":"DAY","basePath":"","rollup":1,"rollupAge":"None",
                  |"rollupCol":"Date","rollupRange":4,"merge":{"files":[{"reportPath":"src/test/resources/report.csv",
                  |"deltaPath":"src/test/resources/delta.csv"}],
                  |"dims":["Date"]},"container":"test-container","postContainer":null,"deltaFileAccess":true,"reportFileAccess":true}""".stripMargin
            mergeUtil.mergeReport(deltaDF,reportDF,JSONUtils.deserialize[MergeScriptConfig](config5)).count should be(11)
    }

    "MergeUtil" should "test without rollup condition" in {

        implicit val mockFc = mock[FrameworkContext]
        val mergeUtil = new MergeUtil()

        val config =
            """{"type":"local","id":"consumption_usage_metrics","frequency":"DAY","basePath":"","rollup":0,"rollupAge":"ACADEMIC_YEAR",
              |"rollupCol":"Date","rollupRange":2,"merge":{"files":[{"reportPath":"src/test/resources/report.csv",
              |"deltaPath":"src/test/resources/delta.csv"}],
              |"dims":["Date"]},"container":"test-container","postContainer":null,"deltaFileAccess":true,"reportFileAccess":true}""".stripMargin
        implicit val sqlContext = new SQLContext(sc)
        val deltaDF = sqlContext.read.options(Map("header" -> "true")).csv("src/test/resources/delta_rollup.csv")
        val reportDF = sqlContext.read.options(Map("header" -> "true")).csv("src/test/resources/report_rollup.csv")
        mergeUtil.mergeReport(deltaDF,reportDF,JSONUtils.deserialize[MergeScriptConfig](config)).count should be(1)

    }

}