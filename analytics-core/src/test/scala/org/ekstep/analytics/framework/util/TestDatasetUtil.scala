package org.ekstep.analytics.framework.util

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.azure.AzureException
import org.apache.hadoop.fs.s3.S3Exception
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import org.sunbird.cloud.storage.BaseStorageService

import java.io.Serializable

case class EnvTestSummary(env: String, time_spent: String, count: Long) extends Serializable {}
case class DruidSummary(Date:String,row1: String, time_spent: Double, count: Long)
class TestDatasetUtil extends BaseSpec with Matchers with MockFactory {

    "DatasetUtil" should "test the dataset extensions" in {
      
      val fileUtil = new HadoopFileUtil();
      val sparkSession = CommonUtil.getSparkSession(1, "TestDatasetUtil", None, None, None);
      val rdd = sparkSession.sparkContext.parallelize(Seq(EnvSummary("env1", 22.1, 3), EnvSummary("env2", 20.1, 3), EnvSummary("env1", 32.1, 4)), 1);
      val df = sparkSession.createDataFrame(rdd);
      df.saveToBlobStore(StorageConfig("local", null, "src/test/resources"), "csv", "test-report", Option(Map("header" -> "true")), Option(Seq("env")));
      
      val rdd2 = sparkSession.sparkContext.textFile("src/test/resources/test-report/env1.csv", 1).collect();
      rdd2.head should be ("time_spent,count")
      rdd2.last should be ("32.1,4")
      
      df.saveToBlobStore(StorageConfig("local", null, "src/test/resources"), "csv", "test-report2", None, None);
      val rdd3 = sparkSession.sparkContext.textFile("src/test/resources/test-report2.csv", 1).collect();
      rdd3.head should be ("env1,22.1,3")
      rdd3.last should be ("env1,32.1,4")

      // get file size
      val size = fileUtil.size("src/test/resources/test-report2.csv", sparkSession.sparkContext.hadoopConfiguration)
      size should be (36)

      // get multiple files size
      val filesWithSize = fileUtil.size(sparkSession.sparkContext.hadoopConfiguration, "src/test/resources/test-report/env1.csv", "src/test/resources/test-report/env2.csv", "src/test/resources/test-report2.csv")
      filesWithSize.size should be (3)
      filesWithSize.head._1 should be ("src/test/resources/test-report/env1.csv")
      filesWithSize.head._2 should be (31)

      val rdd1 = sparkSession.sparkContext.parallelize(Seq(DruidSummary("2020-01-11","env1", 22.1, 3), DruidSummary("2020-01-11","env2", 20.1, 3)), 1)
      val df1 = sparkSession.createDataFrame(rdd1);

      df1.saveToBlobStore(StorageConfig("local", null, "src/test/resources"), "csv", "test-report3", None,
        Option(Seq("Date")),None,None,Some(List("time_spent","row1","count")))
      val rdd4 = sparkSession.sparkContext.textFile("src/test/resources/test-report3/2020-01-11.csv", 1).collect();
      rdd4.head should be ("22.1,env1,3")

      fileUtil.delete(sparkSession.sparkContext.hadoopConfiguration, "src/test/resources/test-report", "src/test/resources/test-report2","src/test/resources/test-report3"
        , "src/test/resources/test-report2.csv","src/test/resources/test-report3.csv", "src/test/resources/test-report3/2020-01-11.zip");
      sparkSession.stop();
    }
  "DatasetUtil" should "test the zip functionality" in {

    val fileUtil = new HadoopFileUtil();
    val sparkSession = CommonUtil.getSparkSession(1, "TestDatasetUtil", None, None, None);

    val rdd1 = sparkSession.sparkContext.parallelize(Seq(DruidSummary("2020-01-11","env1", 22.1, 3), DruidSummary("2020-01-11","env2", 20.1, 3)), 1);
    val df1 = sparkSession.createDataFrame(rdd1);

    val rdd = sparkSession.sparkContext.parallelize(Seq(DruidSummary("2020-01-12","env1", 22.1, 3), DruidSummary("2020-01-12","env2", 20.1, 3)), 1);
    val df = sparkSession.createDataFrame(rdd);

    df1.saveToBlobStore(StorageConfig("local", null, "src/test/resources"), "csv", "test-report3", Option(Map("header" -> "true")),
      Option(Seq("Date")),None,Option(true))
    val rdd2 = sparkSession.sparkContext.textFile("src/test/resources/test-report3/2020-01-11.zip", 1).collect();


    a[AzureException] should be thrownBy {
      df1.saveToBlobStore(StorageConfig("azure", "test-container", "src/test/resources"), "csv",
        "test-report", Option(Map("header" -> "true")), Option(Seq("env")),None,Option(true))
    }
    val mockBaseStorageService = mock[BaseStorageService]
    (mockBaseStorageService.download _).expects("test-container", "test-report3/2020-01-12.csv","src/test/resources/test-report3/", Some(false)).once()
    (mockBaseStorageService.upload _).expects("test-container", "src/test/resources/test-report3/2020-01-12.zip",
      "test-report3/2020-01-12.zip", Some(false), Some(0), Some(3), None).once()
      df.copyMergeFile(Seq("Date"), "", "src/test/resources/test-report3/_tmp",
        "src/test/resources/test-report3", sparkSession.sparkContext.hadoopConfiguration, "csv",
        Map("header" -> "true"), StorageConfig("azure", "test-container", "src/test/resources"), Option(mockBaseStorageService), Option(true),None,Some("src/test/resources/"))

    fileUtil.delete(sparkSession.sparkContext.hadoopConfiguration, "src/test/resources/test-report", "src/test/resources/test-report2","src/test/resources/test-report3"
      , "src/test/resources/test-report2.csv","src/test/resources/test-report3/2020-01-11.zip");
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

  it should "test the dataset saveToBlobStore with scientific notation format" in {

    val sparkSession = CommonUtil.getSparkSession(1, "TestDatasetUtil", None, None, None);
    val rdd1 = sparkSession.sparkContext.parallelize(Seq(EnvSummary("env1", 1209058.0, 3), EnvSummary("env2", 1.20905875E+08, 3), EnvSummary("env1", 140905875.0, 4)), 1);
    val df1 = sparkSession.createDataFrame(rdd1);
    df1.saveToBlobStore(StorageConfig("local", null, "src/test/resources"), "csv", "test-report-exponential", Option(Map("header" -> "true")), Option(Seq("env")));

    // Check for numeric value with more than 8 digits, writing to file in scientific notation format
    val rdd2 = sparkSession.sparkContext.textFile("src/test/resources/test-report-exponential/env1.csv", 1).collect();
    rdd2.head should be("time_spent,count")
    rdd2.toList(1) should be("1209058.0,3")
    rdd2.last should be("1.40905875E8,4")

    val rdd3 = sparkSession.sparkContext.parallelize(Seq(EnvTestSummary("env1", "1209058.0", 3), EnvTestSummary("env2", "1.20905875E+08", 3), EnvTestSummary("env1", "140905875.0", 4)), 1);
    val df3 = sparkSession.createDataFrame(rdd3);
    df3.saveToBlobStore(StorageConfig("local", null, "src/test/resources"), "csv", "test-report-exponential2", Option(Map("header" -> "true")), Option(Seq("env")));

    // Check for numeric value with more than 8 digits as String data type, writing to file in normal decimal format
    val rdd4 = sparkSession.sparkContext.textFile("src/test/resources/test-report-exponential2/env1.csv", 1).collect();
    rdd4.head should be("time_spent,count")
    rdd4.last should be("140905875.0,4")

    sparkSession.stop();

  }
}