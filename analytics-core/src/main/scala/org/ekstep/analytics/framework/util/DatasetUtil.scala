package org.ekstep.analytics.framework.util

import java.nio.file.Paths

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.col
import org.ekstep.analytics.framework.StorageConfig

class DatasetExt(df: Dataset[Row]) {

  private val fileUtil = new HadoopFileUtil();

  private def getTempDir(filePrefix: String, reportId: String): String = {
    Paths.get(filePrefix, reportId, "/_tmp/").toString()
  }

  private def getFinalDir(filePrefix: String, reportId: String): String = {
    Paths.get(filePrefix, reportId).toString();
  }

  private def filePaths(dims: Seq[String], row: Row, format: String, tempDir: String, finalDir: String): (String, String) = {

    val dimPaths = for(dim <- dims) yield {
      dim + "=" + row.get(row.fieldIndex(dim))
    }

    val paths = for(dim <- dims) yield {
      row.get(row.fieldIndex(dim))
    }

    (Paths.get(tempDir, dimPaths.mkString("/")).toString(), Paths.get(finalDir, paths.mkString("/")) + "." + format)
  }

  def saveToBlobStore(storageConfig: StorageConfig, format: String, reportId: String, options: Option[Map[String, String]], partitioningColumns: Option[Seq[String]]): List[String] = {

    val conf = df.sparkSession.sparkContext.hadoopConfiguration;

    val file = storageConfig.store.toLowerCase() match {
      case "s3" =>
        CommonUtil.getS3FileWithoutPrefix(storageConfig.container, storageConfig.fileName);
      case "azure" =>
        CommonUtil.getAzureFileWithoutPrefix(storageConfig.container, storageConfig.fileName, storageConfig.accountKey.getOrElse("azure_storage_key"))
      case _ =>
        storageConfig.fileName
    }

    val filePrefix = storageConfig.store.toLowerCase() match {
      case "s3" =>
        "s3n://"
      case "azure" =>
        "wasb://"
      case _ =>
        ""
    }

    val tempDir = getTempDir(file, reportId);
    val finalDir = getFinalDir(file, reportId);

    val dims = partitioningColumns.getOrElse(Seq());

    fileUtil.delete(conf, filePrefix + tempDir)
    val opts = options.getOrElse(Map());
    val files = if(dims.nonEmpty) {
      val map = df.select(dims.map(f => col(f)):_*).distinct().collect().map(f => filePaths(dims, f, format, tempDir, finalDir)).toMap
      df.coalesce(1).write.format(format).options(opts).partitionBy(dims: _*).save(filePrefix + tempDir);
      map.foreach(f => {
        fileUtil.delete(conf, filePrefix + f._2)
        fileUtil.copyMerge(filePrefix + f._1, filePrefix + f._2, conf, true);
      })
      map.map(f => filePrefix + f._2).toList
    } else {
      df.coalesce(1).write.format(format).options(opts).save(filePrefix + tempDir);
      fileUtil.delete(conf, filePrefix + finalDir + "." + format)
      fileUtil.copyMerge(filePrefix + tempDir, filePrefix + finalDir + "." + format, conf, true);
      List(filePrefix + finalDir + "." + format)
    }
    fileUtil.delete(conf, filePrefix + tempDir)
    files
  }

}

object DatasetUtil {
  implicit def extensions(df: Dataset[Row]) = new DatasetExt(df);

}