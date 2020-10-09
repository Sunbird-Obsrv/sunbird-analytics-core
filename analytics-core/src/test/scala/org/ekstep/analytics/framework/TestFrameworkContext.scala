package org.ekstep.analytics.framework

import java.text.SimpleDateFormat

import org.scalatest._
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.CommonUtil
import org.apache.spark.SparkContext
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import com.fasterxml.jackson.core.JsonParseException
import org.ekstep.analytics.framework.exception.DataFilterException
import org.apache.spark.SparkException
import org.ekstep.analytics.framework.util.JSONUtils

import scala.collection.mutable.Buffer
import java.util.Date

import org.joda.time.DateTime


/**
 * @author Santhosh
 */
class TestFrameworkContext extends BaseSpec with BeforeAndAfterAll {
    
    "FrameworkContext" should "test all methods" in {
      
      val fc = new FrameworkContext();
      
      noException should be thrownBy {
        fc.shutdownStorageService();
      }
      
      fc.initialize(Option(Array(("azure", "local", "local"))));
      fc.getStorageService("azure", "local", "local") should not be (null)
      
      fc.storageContainers.clear();
      fc.getStorageService("azure") should not be (null)
      
      fc.setDruidClient(null, null);
      noException should be thrownBy {
        fc.shutdownDruidClient();
      }
      
      fc.getDruidClient() should not be (null);
      fc.setDruidClient(fc.getDruidClient(), fc.getDruidRollUpClient())
      
      fc.getAkkaHttpUtil() should  not be (null)
      fc.closeContext();
    }
        
}