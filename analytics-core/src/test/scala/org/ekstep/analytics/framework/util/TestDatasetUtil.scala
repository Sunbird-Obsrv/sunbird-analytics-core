package org.ekstep.analytics.framework.util

import org.ekstep.analytics.framework._
import org.joda.time.LocalDate
import java.io.File

import org.joda.time.DateTime
import java.util.Date
import java.text.SimpleDateFormat

import scala.collection.mutable.ListBuffer
import org.joda.time.format.DateTimeFormat
import org.ekstep.analytics.framework.Period._
import org.apache.spark.sql.Encoders
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.apache.hadoop.fs.azure.AzureException
import org.apache.hadoop.fs.s3.S3Exception

class TestDatasetUtil extends BaseSpec {

    "DatasetUtil" should "test the dataset extensions" in {
      
      val fileUtil = new HadoopFileUtil();
      val sparkSession = CommonUtil.getSparkSession(1, "TestDatasetUtil", None, None, None);
      val rdd = sparkSession.sparkContext.parallelize(Seq(EnvSummary("env1", 22.1, 3), EnvSummary("env2", 20.1, 3), EnvSummary("env1", 32.1, 4)), 1);
      
      import sparkSession.implicits._
      val df = sparkSession.createDataFrame(rdd);
      df.saveToBlobStore(StorageConfig("local", null, "src/test/resources"), "csv", "test-report", Option(Map("header" -> "true")), Option(Seq("env")));
      
      val rdd2 = sparkSession.sparkContext.textFile("src/test/resources/test-report/env1.csv", 1).collect();
      rdd2.head should be ("time_spent,count")
      rdd2.last should be ("32.1,4")
      
      df.saveToBlobStore(StorageConfig("local", null, "src/test/resources"), "csv", "test-report2", None, None);
      val rdd3 = sparkSession.sparkContext.textFile("src/test/resources/test-report2.csv", 1).collect();
      rdd3.head should be ("env1,22.1,3")
      rdd3.last should be ("env1,32.1,4")
      
      fileUtil.delete(sparkSession.sparkContext.hadoopConfiguration, "src/test/resources/test-report", "src/test/resources/test-report2", "src/test/resources/test-report2.csv");
      sparkSession.stop();
    }
    
    it should "test exception branches" in {
      
      val sparkSession = CommonUtil.getSparkSession(1, "TestDatasetUtil", None, None, None);
      val rdd = sparkSession.sparkContext.parallelize(Seq(EnvSummary("env1", 22.1, 3), EnvSummary("env2", 20.1, 3), EnvSummary("env1", 32.1, 4)), 1);
      
      import sparkSession.implicits._
      val df = sparkSession.createDataFrame(rdd);
      a[AzureException] should be thrownBy {
        df.saveToBlobStore(StorageConfig("azure", "test-container", "src/test/resources"), "csv", "test-report", Option(Map("header" -> "true")), Option(Seq("env")));        
      }
      
      a[S3Exception] should be thrownBy {
        df.saveToBlobStore(StorageConfig("s3", "test-container", "src/test/resources"), "csv", "test-report", Option(Map("header" -> "true")), Option(Seq("env")));
      }
      
      sparkSession.stop();
    }
}