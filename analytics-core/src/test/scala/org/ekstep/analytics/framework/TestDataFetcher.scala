package org.ekstep.analytics.framework

import org.ekstep.analytics.framework.exception.DataFetcherException
import org.ekstep.analytics.framework.util.JSONUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should.Matchers
import org.sunbird.cloud.storage.IStorageService
import org.ekstep.analytics.framework.fetcher.S3DataFetcher

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
        val mockStorageService = mock[IStorageService]
        mockFc.inputEventsCount = sc.longAccumulator("Count");
        (mockFc.getStorageService(_:String):IStorageService).expects("aws").returns(mockStorageService);
        (mockStorageService.searchObjects(_: String, _: String, _: String, _: String, _: Integer, _: String)).expects("dev-data-store", "abc/", "2012-01-01", "2012-02-01", *, "yyyy-MM-dd").returns(null);
        (mockStorageService.getPaths _).expects("dev-data-store", *).returns(java.util.Arrays.asList("src/test/resources/sample_telemetry_2.log"))
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

    it should "fetch no data for none fetcher type" in {
        implicit val fc = new FrameworkContext();
        val rdd = DataFetcher.fetchBatchData[Event](Fetcher("none", None, None));
        rdd.isEmpty() should be (true)
    }

    it should "cover the missing branches in S3DataFetcher" in {
      implicit val fc = new FrameworkContext();
      var query = JSONUtils.deserialize[Query]("""{"bucket":"test-container","prefix":"test/","folder":"true","endDate":"2020-01-10"}""")
      S3DataFetcher.getObjectKeys(Array(query)).head should be ("s3a://test-container/test/2020-01-10")

      query = JSONUtils.deserialize[Query]("""{"bucket":"test-container","prefix":"test/","folder":"true","endDate":"2020-01-10","excludePrefix":"test"}""")
      S3DataFetcher.getObjectKeys(Array(query)).size should be (0)

    }


    it should "check for getFilteredKeys via partitions" in {

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
