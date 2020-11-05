package org.ekstep.analytics.framework.util

import org.apache.hadoop.fs.azure.AzureException
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
              |"postContainer":"test-container","deltaFileAccess":true,"reportFileAccess":true}""".stripMargin
        val jsonConfig = JSONUtils.deserialize[MergeScriptConfig](config)
        (mockFc.getStorageService(_:String, _:String, _:String):BaseStorageService).expects("azure", "azure_storage_key", "azure_storage_secret").returns(mockStorageService)
        (mockStorageService.searchObjects _).expects(jsonConfig.container,"druid-reports/ETB-Consumption-Daily-Reports/apekx/2020-11-03.csv",None,None,None,"yyyy-MM-dd").returns(null)
        (mockStorageService.getPaths _).expects(jsonConfig.container, null).returns(List("src/test/resources/delta.csv"))
        (mockFc.getStorageService(_:String, _:String, _:String):BaseStorageService).expects("azure", "azure_storage_key", "azure_storage_secret").returns(mockStorageService)
        (mockStorageService.searchObjects _).expects(jsonConfig.postContainer.get,"apekx/daily_metrics.csv",None,None,None,"yyyy-MM-dd").returns(null)
        (mockStorageService.getPaths _).expects(jsonConfig.postContainer.get, null).returns(List("src/test/resources/report.csv"))
        a[AzureException] should be thrownBy {
            mergeUtil.mergeFile(JSONUtils.deserialize[MergeScriptConfig](config))
        }
    }
}