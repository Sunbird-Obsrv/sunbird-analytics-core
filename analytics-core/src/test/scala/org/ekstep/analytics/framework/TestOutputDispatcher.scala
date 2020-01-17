package org.ekstep.analytics.framework

import java.io.{File, IOException}

import org.ekstep.analytics.framework.dispatcher.AzureDispatcher
import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils}
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import org.sunbird.cloud.storage.BaseStorageService

/**
  * @author Santhosh
  */
class TestOutputDispatcher extends SparkSpec("src/test/resources/sample_telemetry_2.log") with Matchers with MockFactory {

  "OutputDispatcher" should "dispatch output to console" in {

    implicit val fc = new FrameworkContext();
    val outputs = Option(Array(
      Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef]))
    ))
    val eventsInString = events.map { x => JSONUtils.serialize(x) }
    noException should be thrownBy {
      OutputDispatcher.dispatch(outputs, eventsInString);
    }

    noException should be thrownBy {
      OutputDispatcher.dispatch(Dispatcher("console", Map()), sc.parallelize(events.take(1)));
    }

    val eventsInArray = events.map { x => JSONUtils.serialize(x) }.collect
    noException should be thrownBy {
      OutputDispatcher.dispatch(Dispatcher("console", Map()), eventsInArray);
    }
  }

  it should "dispatch output to s3" in {

    implicit val mockFc = mock[FrameworkContext];
    val mockStorageService = mock[BaseStorageService]
    (mockFc.getStorageService(_: String): BaseStorageService).expects("aws").returns(mockStorageService).anyNumberOfTimes();
    (mockStorageService.upload _).expects("dev-data-store", *, *, Option(false), None, None, None).returns(null).anyNumberOfTimes();
    (mockStorageService.closeContext _).expects().returns().anyNumberOfTimes()
    val output1 = Dispatcher("s3file", Map[String, AnyRef]("bucket" -> "dev-data-store", "key" -> "output/test-log1.json", "filePath" -> "src/test/resources/sample_telemetry.log", "zip" -> true.asInstanceOf[AnyRef]));
    val output2 = Dispatcher("s3file", Map[String, AnyRef]("bucket" -> "dev-data-store", "key" -> "output/test-log1.json", "filePath" -> "src/test/resources/sample_telemetry.log.gz"));
    val output3 = Dispatcher("s3file", Map[String, AnyRef]("bucket" -> "dev-data-store", "key" -> "output/test-log2.json"));
    noException should be thrownBy {
      OutputDispatcher.dispatch(output1, events);
      OutputDispatcher.dispatch(output2, events);
      OutputDispatcher.dispatch(output3, events);
    }

    val output4 = Dispatcher("s3", Map[String, AnyRef]("bucket" -> "dev-data-store", "key" -> "output/test-log1.json", "filePath" -> "src/test/resources/sample_telemetry.log", "zip" -> true.asInstanceOf[AnyRef]));
    val output5 = Dispatcher("s3", Map[String, AnyRef]("bucket" -> "dev-data-store", "key" -> "output/test-log1.json", "filePath" -> "src/test/resources/sample_telemetry.log.gz"));
    val output6 = Dispatcher("s3", Map[String, AnyRef]("bucket" -> "dev-data-store", "key" -> "output/test-log2.json"));
    val eventRDDString = events.map(f => JSONUtils.serialize(f)).collect();
    //noException should be thrownBy {
    OutputDispatcher.dispatch(output4, eventRDDString);
    OutputDispatcher.dispatch(output5, eventRDDString);
    OutputDispatcher.dispatch(output6, eventRDDString);
    //}

  }

  it should "throw dispatcher exceptions" in {

    implicit val fc = new FrameworkContext();
    // Unknown Dispatcher
    a[DispatcherException] should be thrownBy {
      OutputDispatcher.dispatch(Dispatcher("xyz", Map[String, AnyRef]()), events);
    }

    // Invoke kafka dispatcher with no parameters
    a[DispatcherException] should be thrownBy {
      OutputDispatcher.dispatch(Dispatcher("kafka", Map[String, AnyRef]()), events);
    }

    // Invoke kafka dispatcher with missing topic
    a[DispatcherException] should be thrownBy {
      OutputDispatcher.dispatch(Dispatcher("kafka", Map("brokerList" -> "localhost:9092")), events);
    }

    // Invoke script dispatcher without required fields ('script')
    a[DispatcherException] should be thrownBy {
      OutputDispatcher.dispatch(Dispatcher("script", Map[String, AnyRef]()), events);
    }

    // Invoke File dispatcher without required fields ('file')
    a[DispatcherException] should be thrownBy {
      OutputDispatcher.dispatch(Dispatcher("file", Map[String, AnyRef]()), events);
    }

    // Invoke script dispatcher with invalid script
    a[IOException] should be thrownBy {
      OutputDispatcher.dispatch(Dispatcher("script", Map("script" -> "src/test/resources/simpleScript3.sh")), events);
    }

    a[DispatcherException] should be thrownBy {
      OutputDispatcher.dispatch(Dispatcher("script", Map("script" -> "src/test/resources/simpleScript2.sh")), events);
    }

    // Invoke S3 dispatcher without required fields ('bucket','key')
    a[DispatcherException] should be thrownBy {
      OutputDispatcher.dispatch(Dispatcher("s3", Map[String, AnyRef]("zip" -> true.asInstanceOf[AnyRef])), events);
      OutputDispatcher.dispatch(Dispatcher("s3", Map[String, AnyRef]("bucket" -> Option("test"))), events);
      OutputDispatcher.dispatch(Dispatcher("s3File", Map[String, AnyRef]("zip" -> true.asInstanceOf[AnyRef])), events);
      OutputDispatcher.dispatch(Dispatcher("s3File", Map[String, AnyRef]("bucket" -> Option("test"))), events);
    }

    // Invoke dispatch with null dispatcher
    a[DispatcherException] should be thrownBy {
      OutputDispatcher.dispatch(null.asInstanceOf[Dispatcher], events);
    }

    val eventsInArray = events.map { x => JSONUtils.serialize(x) }.collect
    a[DispatcherException] should be thrownBy {
      OutputDispatcher.dispatch(null.asInstanceOf[Dispatcher], eventsInArray);
    }

    // Invoke dispatch with None dispatchers
    a[DispatcherException] should be thrownBy {
      OutputDispatcher.dispatch(None, events);
    }

    val noEvents = sc.parallelize(Array[Event]());

    // Invoke dispatch with Empty events
    noException should be thrownBy {
      OutputDispatcher.dispatch(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])), noEvents);
    }

    // Invoke dispatch with Empty events
    noException should be thrownBy {
      OutputDispatcher.dispatch(Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), noEvents);
    }

    OutputDispatcher.dispatch(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])), Array[String]());
  }

  it should "execute test cases related to script dispatcher" in {

    implicit val fc = new FrameworkContext();
    val result = OutputDispatcher.dispatch(Dispatcher("script", Map("script" -> "src/test/resources/simpleScript.sh")), events);
    //result(0) should endWith ("analytics-core");
    //result(1) should include ("7436");
  }


  it should "dispatch output to a file" in {

    implicit val fc = new FrameworkContext();
    OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "src/test/resources/test_output.log")), events);
    val f = new File("src/test/resources/test_output.log");
    f.exists() should be(true)
    CommonUtil.deleteFile("src/test/resources/test_output.log");
  }

  it should "dispatch output to azure" in {

    implicit val mockFc = mock[FrameworkContext];
    val mockStorageService = mock[BaseStorageService]
    (mockFc.getStorageService(_: String): BaseStorageService).expects("azure").returns(mockStorageService).anyNumberOfTimes();
    (mockStorageService.upload _).expects("dev-data-store", *, *, Option(false), None, None, None).returns(null).anyNumberOfTimes();
    (mockStorageService.closeContext _).expects().returns().anyNumberOfTimes()
    val date = System.currentTimeMillis()
    val output1 = Dispatcher("azure", Map[String, AnyRef]("bucket" -> "dev-data-store", "key" -> s"output/test-dispatcher1-$date.json", "zip" -> true.asInstanceOf[AnyRef]));
    val output2 = Dispatcher("azure", Map[String, AnyRef]("bucket" -> "dev-data-store", "key" -> s"output/test-dispatcher2-$date.json", "filePath" -> "src/test/resources/sample_telemetry.log"));
    val strData = events.map(f => JSONUtils.serialize(f))

    noException should be thrownBy {
      OutputDispatcher.dispatch(output2, strData.collect());
    }

  }

  it should "dispatch directory to azure" in {

    implicit val mockFc = mock[FrameworkContext];
    val mockStorageService = mock[BaseStorageService]
    (mockFc.getStorageService(_: String): BaseStorageService).expects("azure").returns(mockStorageService).anyNumberOfTimes();
    (mockStorageService.upload _).expects("dev-data-store", *, *, Option(true), *, Option(3), *).returns("").anyNumberOfTimes();
    (mockStorageService.closeContext _).expects().returns().anyNumberOfTimes()
    //noException should be thrownBy {
    AzureDispatcher.dispatchDirectory(Map[String, AnyRef]("bucket" -> "dev-data-store", "key" -> s"output/test-directory/", "dirPath" -> "src/test/resources/1234/OE_INTERACT/"));
    //}
  }

  it should "give DispatcherException if azure config is missing " in {

    implicit val fc = new FrameworkContext();
    the[DispatcherException] thrownBy {
      AzureDispatcher.dispatchDirectory(Map[String, AnyRef]("key" -> s"output/test-directory/", "dirPath" -> "src/test/resources/1234/OE_INTERACT/"));
    } should have message "'local file path', 'bucket' & 'key' parameters are required to upload directory to azure"

    the[DispatcherException] thrownBy {
      AzureDispatcher.dispatch(Map[String, AnyRef]("key" -> s"output/test-directory/", "dirPath" -> "src/test/resources/1234/OE_INTERACT/"), events.map(f => JSONUtils.serialize(f)));
    } should have message "'bucket' & 'key' parameters are required to send output to azure"

    the[DispatcherException] thrownBy {
      AzureDispatcher.dispatch(events.map(f => JSONUtils.serialize(f)).collect(), Map[String, AnyRef]("key" -> s"output/test-directory/", "dirPath" -> "src/test/resources/1234/OE_INTERACT/"));
    } should have message "'bucket' & 'key' parameters are required to send output to azure"
  }

  it should "dispatch output to elastic-search" in {

    implicit val fc = new FrameworkContext();
    val eventsInString = events.map { x => JSONUtils.serialize(x) }
    val output1 = Dispatcher("elasticsearch", Map[String, AnyRef]("index" -> "test_index"));
    a[EsHadoopIllegalArgumentException] should be thrownBy {
      OutputDispatcher.dispatch(output1, eventsInString);
    }
  }

  it should "throw exception while dispatching output to elastic-search" in {

    implicit val fc = new FrameworkContext();
    val eventsInString = events.map { x => JSONUtils.serialize(x) }
    val output1 = Dispatcher("elasticsearch", Map[String, AnyRef]());
    a[DispatcherException] should be thrownBy {
      OutputDispatcher.dispatch(output1, eventsInString);
    }
  }

}