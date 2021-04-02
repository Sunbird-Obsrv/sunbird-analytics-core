package org.ekstep.analytics.framework

import org.ekstep.analytics.framework.exception.DataFetcherException
import org.ekstep.analytics.framework.util.JSONUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import org.sunbird.cloud.storage.BaseStorageService
import org.ekstep.analytics.framework.fetcher.S3DataFetcher
import org.ekstep.analytics.framework.fetcher.AzureDataFetcher

/**
 * @author Santhosh
 */


case class GroupByPid(time: String, producer_id: String, producer_pid: String, total_duration: Double, count: Int)
case class TimeSeriesData(time: String, count: Int)

class TestDataFetcher extends SparkSpec with Matchers with MockFactory {

    "DataFetcher" should "fetch the streaming events matching query" in {
        
        val rdd = DataFetcher.fetchStreamData(null, null);
        rdd should be (null);
        
    }
    
    it should "fetch the events from local file" in {
        
        implicit val fc = new FrameworkContext();
        fc.inputEventsCount = sc.longAccumulator("Count");
        val search = Fetcher("local", None, Option(Array(
            Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry.log"))
        )));
        val rdd = DataFetcher.fetchBatchData[Event](search);
        rdd.count should be (7437)
        
        val search0 = Fetcher("local", None, Option(Array(
            Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry_2.log"))
        )));
        val rddString = DataFetcher.fetchBatchData[String](search0);
        rddString.count should be (19)
        
        val search1 = Fetcher("local", None, Option(Array(
            Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry.log"))
        )));
        val rdd1 = DataFetcher.fetchBatchData[TestDataFetcher](search1);
        rdd1.count should be (0)
        
        val search2 = Fetcher("local", None, Option(Array(
            Query(None, None, None, None, None, None, None, None, None, None)
        )));
        val rdd2 = DataFetcher.fetchBatchData[TestDataFetcher](search2);
        rdd1.count should be (0)
    }
    
    it should "fetch no file from S3 and return an empty RDD" in {
        
        implicit val mockFc = mock[FrameworkContext];
        val mockStorageService = mock[BaseStorageService]
        mockFc.inputEventsCount = sc.longAccumulator("Count");
        (mockFc.getStorageService(_:String):BaseStorageService).expects("aws").returns(mockStorageService);
        (mockStorageService.searchObjects _).expects("dev-data-store", "abc/", Option("2012-01-01"), Option("2012-02-01"), None, "yyyy-MM-dd").returns(null);
        (mockStorageService.getPaths _).expects("dev-data-store", null).returns(List("src/test/resources/sample_telemetry_2.log"))
        val queries = Option(Array(
            Query(Option("dev-data-store"), Option("abc/"), Option("2012-01-01"), Option("2012-02-01"))
        ));
        val rdd = DataFetcher.fetchBatchData[Event](Fetcher("S3", None, queries));
        rdd.count should be (19)
    }
    
    it should "throw DataFetcherException" in {
        
        implicit val fc = new FrameworkContext();
        // Throw unknown fetcher type found
        the[DataFetcherException] thrownBy {
            DataFetcher.fetchBatchData[Event](Fetcher("s3", None, None));    
        }
        
        the[DataFetcherException] thrownBy {
            val fileFetcher = Fetcher("file", None, Option(Array(
                Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry.log"))
            )));
            DataFetcher.fetchBatchData[Event](fileFetcher);
        } should have message "Unknown fetcher type found"
    }

    it should "fetch the batch events from azure" in {

        implicit val mockFc = mock[FrameworkContext];
        mockFc.inputEventsCount = sc.longAccumulator("Count");
        val mockStorageService = mock[BaseStorageService]
        (mockFc.getStorageService(_:String, _:String, _:String):BaseStorageService).expects("azure", "azure_storage_key", "azure_storage_secret").returns(mockStorageService);
        (mockStorageService.searchObjects _).expects("dev-data-store", "raw/", Option("2017-08-31"), Option("2017-08-31"), None, "yyyy-MM-dd").returns(null);
        (mockStorageService.getPaths _).expects("dev-data-store", null).returns(List("src/test/resources/sample_telemetry_2.log"))
        val queries = Option(Array(
            Query(Option("dev-data-store"), Option("raw/"), Option("2017-08-31"), Option("2017-08-31"))
        ));
        val rdd = DataFetcher.fetchBatchData[Event](Fetcher("azure", None, queries));
        rdd.count should be (19)
    }

    it should "invoke the druid data fetcher" in {

        implicit val fc = new FrameworkContext();
        val unknownQuery = DruidQueryModel("time", "telemetry-events", "LastWeek", Option("day"), None, None, Option(List(DruidFilter("in", "eid", None, Option(List("START", "END"))))))
        the[DataFetcherException] thrownBy {
            DataFetcher.fetchBatchData[TimeSeriesData](Fetcher("druid", None, None, Option(unknownQuery)));
        } should have message "Unknown druid query type found"
    }

    it should "fetch no data for none fetcher type" in {
        implicit val fc = new FrameworkContext();
        fc.getStorageService("azure") should not be (null)
        val rdd = DataFetcher.fetchBatchData[Event](Fetcher("none", None, None));
        rdd.isEmpty() should be (true)
    }
    
    it should "cover the missing branches in S3DataFetcher, AzureDataFetcher and DruidDataFetcher" in {
      implicit val fc = new FrameworkContext();
      var query = JSONUtils.deserialize[Query]("""{"bucket":"test-container","prefix":"test/","folder":"true","endDate":"2020-01-10"}""")
      S3DataFetcher.getObjectKeys(Array(query)).head should be ("s3n://test-container/test/2020-01-10")
      AzureDataFetcher.getObjectKeys(Array(query)).head should be ("wasb://test-container@azure-test-key.blob.core.windows.net/test/2020-01-10")
      
      query = JSONUtils.deserialize[Query]("""{"bucket":"test-container","prefix":"test/","folder":"true","endDate":"2020-01-10","excludePrefix":"test"}""")
      S3DataFetcher.getObjectKeys(Array(query)).size should be (0)
      AzureDataFetcher.getObjectKeys(Array(query)).size should be (0)
      
    }


    it should "check for getFilteredKeys from azure via partitions" in {

        // with single partition
        val query1 = Query(Option("dev-data-store"), Option("raw/"), Option("2020-06-10"), Option("2020-06-11"), None, None, None, None, None, None, None, None, None, None, Option(List(0)))
        val keys1 = DataFetcher.getFilteredKeys(query1, Array("https://sunbirddevprivate.blob.core.windows.net/dev-data-store/raw/2020-06-10-0-1591845501666.json.gz", "https://sunbirddevprivate.blob.core.windows.net/dev-data-store/raw/2020-06-10-1-1591845501666.json.gz", "https://sunbirddevprivate.blob.core.windows.net/dev-data-store/raw/2020-06-11-0-1591845501666.json.gz", "https://sunbirddevprivate.blob.core.windows.net/dev-data-store/raw/2020-06-11-1-1591845501666.json.gz"), Option(List(0)))
        keys1.length should be (2)
        keys1.head should be ("https://sunbirddevprivate.blob.core.windows.net/dev-data-store/raw/2020-06-10-0-1591845501666.json.gz")

        // with mutilple partition
        val query2 = Query(Option("dev-data-store"), Option("raw/"), Option("2020-06-11"), Option("2020-06-11"), None, None, None, None, None, None, None, None, None, None, Option(List(0,1)))
        val keys2 = DataFetcher.getFilteredKeys(query2, Array("https://sunbirddevprivate.blob.core.windows.net/dev-data-store/raw/2020-06-11-0-1591845501666.json.gz", "https://sunbirddevprivate.blob.core.windows.net/dev-data-store/raw/2020-06-11-1-1591845501666.json.gz"), Option(List(0,1)))
        keys2.length should be (2)
        keys2.head should be ("https://sunbirddevprivate.blob.core.windows.net/dev-data-store/raw/2020-06-11-0-1591845501666.json.gz")

        // without partition
        val query3 = Query(Option("dev-data-store"), Option("raw/"), Option("2020-06-11"), Option("2020-06-11"), None, None, None, None, None, None, None, None, None, None, None)
        val keys3 = DataFetcher.getFilteredKeys(query3, Array("https://sunbirddevprivate.blob.core.windows.net/dev-data-store/raw/2020-06-11-0-1591845501666.json.gz", "https://sunbirddevprivate.blob.core.windows.net/dev-data-store/raw/2020-06-11-1-1591845501666.json.gz"), None)
        keys3.length should be (2)
        keys3.head should be ("https://sunbirddevprivate.blob.core.windows.net/dev-data-store/raw/2020-06-11-0-1591845501666.json.gz")

        // without only end date
        val query4 = Query(Option("dev-data-store"), Option("raw/"), None, Option("2020-06-11"), None, None, None, None, None, None, None, None, None, None, Option(List(0,1)))
        val keys4 = DataFetcher.getFilteredKeys(query4, Array("https://sunbirddevprivate.blob.core.windows.net/dev-data-store/raw/2020-06-11-0-1591845501666.json.gz", "https://sunbirddevprivate.blob.core.windows.net/dev-data-store/raw/2020-06-11-1-1591845501666.json.gz"), Option(List(0,1)))
        keys4.length should be (2)
        keys4.head should be ("https://sunbirddevprivate.blob.core.windows.net/dev-data-store/raw/2020-06-11-0-1591845501666.json.gz")

        // without only end date and delta
        val query5 = Query(Option("dev-data-store"), Option("raw/"), None, Option("2020-06-11"), Option(1), None, None, None, None, None, None, None, None, None, Option(List(0)))
        val keys5 = DataFetcher.getFilteredKeys(query5, Array("https://sunbirddevprivate.blob.core.windows.net/dev-data-store/raw/2020-06-10-0-1591845501666.json.gz", "https://sunbirddevprivate.blob.core.windows.net/dev-data-store/raw/2020-06-10-1-1591845501666.json.gz", "https://sunbirddevprivate.blob.core.windows.net/dev-data-store/raw/2020-06-11-0-1591845501666.json.gz", "https://sunbirddevprivate.blob.core.windows.net/dev-data-store/raw/2020-06-11-1-1591845501666.json.gz"), Option(List(0)))
        keys5.length should be (2)
        keys5.head should be ("https://sunbirddevprivate.blob.core.windows.net/dev-data-store/raw/2020-06-10-0-1591845501666.json.gz")
    }

}