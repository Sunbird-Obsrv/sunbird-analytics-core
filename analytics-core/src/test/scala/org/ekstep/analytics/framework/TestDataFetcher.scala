package org.ekstep.analytics.framework

import org.ekstep.analytics.framework.exception.DataFetcherException
import org.ekstep.analytics.framework.util.JSONUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import org.sunbird.cloud.storage.BaseStorageService

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
    }
    
    it should "fetch no file from S3 and return an empty RDD" in {
        
        implicit val mockFc = mock[FrameworkContext];
        val mockStorageService = mock[BaseStorageService]
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
        val mockStorageService = mock[BaseStorageService]
        (mockFc.getStorageService(_:String):BaseStorageService).expects("azure").returns(mockStorageService);
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
        val unknownQuery = DruidQueryModel("scan", "telemetry-events", "LastWeek", Option("day"), None, None, Option(List(DruidFilter("in", "eid", None, Option(List("START", "END"))))))
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
}