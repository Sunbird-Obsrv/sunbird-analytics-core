package org.ekstep.analytics.framework.util

import org.ekstep.analytics.framework._
import org.joda.time.LocalDate
import java.io.File

import org.joda.time.DateTime
import java.util.Date
import java.text.SimpleDateFormat

import org.apache.hadoop.fs.Path

import scala.collection.mutable.ListBuffer
import org.joda.time.format.DateTimeFormat
import org.ekstep.analytics.framework.Period._
import org.apache.spark.sql.Encoders
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.apache.hadoop.fs.azure.AzureException
import org.apache.hadoop.fs.s3.S3Exception
import org.apache.spark.sql.functions.col

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

  "DatasetUtil" should "test the dataset copy functionality" in {

    val fileUtil = new HadoopFileUtil();
    val sparkSession = CommonUtil.getSparkSession(1, "TestDatasetUtil", None, None, None);
    val rdd = sparkSession.sparkContext.parallelize(Seq(EnvSummary("env1", 22.1, 3), EnvSummary("env2", 20.1, 3), EnvSummary("env1", 32.1, 4)), 1);

    val tempDir = "src/test/resources/test-report/_tmp"

    val partitioningColumns = Option(Seq("env"));
    val dims = partitioningColumns.getOrElse(Seq());
    val options = Option(Map("header" -> "true"))
    val df = sparkSession.createDataFrame(rdd);
    val conf = sparkSession.sparkContext.hadoopConfiguration
    val filePrefix = ""
    val format = "csv"
    val srcFS=new Path("src/test/resources/test-report/_tmp/env=env1")
      val srcDir = srcFS.getFileSystem(conf)
    fileUtil.delete(sparkSession.sparkContext.hadoopConfiguration, "" + tempDir)
    val opts = options.getOrElse(Map());
    df.coalesce(1).write.format(format).options(opts).partitionBy(dims: _*).save(filePrefix + tempDir);
   fileUtil.copyMerge("" + "src/test/resources/test-report/_tmp/env=env1", "src/test/resources/test-report/env2.csv", sparkSession.sparkContext.hadoopConfiguration, false);
    srcDir.delete(new Path("src/test/resources/test-report/_tmp/env=env1"), true)
    fileUtil.delete(sparkSession.sparkContext.hadoopConfiguration, "src/test/resources/test-report", "src/test/resources/test-report2", "src/test/resources/test-report2.csv");
    fileUtil.copyMerge("" + "src/test/resources/test-report/_tmp/env=env1", "src/test/resources/test-report/env2.csv", sparkSession.sparkContext.hadoopConfiguration, false);
    sparkSession.stop();

  }
}