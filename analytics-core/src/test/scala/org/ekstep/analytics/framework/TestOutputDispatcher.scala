package org.ekstep.analytics.framework

import java.io.{File, IOException}

import org.ekstep.analytics.framework.dispatcher.AzureDispatcher
import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils}
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import org.sunbird.cloud.storage.BaseStorageService
import org.ekstep.analytics.framework.dispatcher.S3Dispatcher
import org.apache.hadoop.fs.azure.AzureException
import org.ekstep.analytics.framework.dispatcher.ConsoleDispatcher

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
      OutputDispatcher.dispatch(Dispatcher("s3", Map[String, AnyRef]("key" -> "testKey")), events);
    }
    
    a[DispatcherException] should be thrownBy {
      OutputDispatcher.dispatch(Dispatcher("s3", Map[String, AnyRef]("bucket" -> "testBucket")), events);
    }
    
    a[DispatcherException] should be thrownBy {
      OutputDispatcher.dispatch(StorageConfig("s3", null, null), events);
    }
    
    a[DispatcherException] should be thrownBy {
      OutputDispatcher.dispatch(StorageConfig("file", "test", null), events);
    }
    
    a[DispatcherException] should be thrownBy {
      OutputDispatcher.dispatch(null.asInstanceOf[StorageConfig], events);
    }
    
    a[DispatcherException] should be thrownBy {
      ConsoleDispatcher.dispatch(events.map(f => JSONUtils.serialize(f)), StorageConfig("file", "test", null));
    }

    // Invoke dispatch with null dispatcher
    a[DispatcherException] should be thrownBy {
      OutputDispatcher.dispatch(null.asInstanceOf[Dispatcher], events);
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
    
    OutputDispatcher.dispatch(StorageConfig("local", null, "src/test/resources/test_output.log"), events);
    val f2 = new File("src/test/resources/test_output.log");
    f2.exists() should be(true)
    CommonUtil.deleteFile("src/test/resources/test_output.log");
  }
  
  it should "give DispatcherException if azure config is missing " in {

    implicit val fc = new FrameworkContext();
    val eventArr = events.map(f => JSONUtils.serialize(f)).cache();
    
    the[DispatcherException] thrownBy {
      AzureDispatcher.dispatch(Map[String, AnyRef]("key" -> "output/test-directory/", "dirPath" -> "src/test/resources/1234/OE_INTERACT/"), eventArr);
    } should have message "'bucket' & 'key' parameters are required to send output to azure"
    
    the[DispatcherException] thrownBy {
      AzureDispatcher.dispatch(Map[String, AnyRef]("bucket" -> "test-bucket", "dirPath" -> "src/test/resources/1234/OE_INTERACT/"), eventArr);
    } should have message "'bucket' & 'key' parameters are required to send output to azure"
    
    the[DispatcherException] thrownBy {
      OutputDispatcher.dispatch(StorageConfig("azure", "test-bucket", null), eventArr);
    } should have message "'bucket' & 'key' parameters are required to send output to azure"
    
    the[DispatcherException] thrownBy {
      OutputDispatcher.dispatch(StorageConfig("azure", null, "output/test-directory/"), eventArr);
    } should have message "'bucket' & 'key' parameters are required to send output to azure"

  }


  it should "dispatch output to S3/Azure" in {

    implicit val fc = new FrameworkContext();

    a[AzureException] should be thrownBy {
      AzureDispatcher.dispatch(Map[String, AnyRef]("key" -> "test_key", "bucket" -> "test_bucket"), events.map(f => JSONUtils.serialize(f)));
    }
    
    a[AzureException] should be thrownBy {
      OutputDispatcher.dispatch(StorageConfig("azure", "test_bucket", "test_key", Option("azure_storage_key")), events.map(f => JSONUtils.serialize(f)));
    }
    
    a[AzureException] should be thrownBy {
      OutputDispatcher.dispatch(StorageConfig("azure", "test_bucket", "test_key"), events.map(f => JSONUtils.serialize(f)));
    }
    
    a[IllegalArgumentException] should be thrownBy {
      S3Dispatcher.dispatch(Map[String, AnyRef]("key" -> "test_key", "bucket" -> "test_bucket"), events.map(f => JSONUtils.serialize(f)));
    }

    a[IOException] should be thrownBy {
      OutputDispatcher.dispatch(StorageConfig("gcloud", "test-obsrv-data-store", "test_key/test_data.json"), events.map(f => JSONUtils.serialize(f)));
    }
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