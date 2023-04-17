package org.ekstep.analytics.framework.util

import net.lingala.zip4j.ZipFile
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row}
import org.ekstep.analytics.framework.StorageConfig
import org.sunbird.cloud.storage.BaseStorageService
import org.apache.hadoop.conf.Configuration
import org.ekstep.analytics.framework.conf.AppConf

import java.io.File
import java.nio.file.Paths

class DatasetExt(df: Dataset[Row]) {

  private val fileUtil = new HadoopFileUtil();

  private def getTempDir(filePrefix: String, reportId: String): String = {
    Paths.get(filePrefix, reportId, "/_tmp/").toString()
  }

  private def getFinalDir(filePrefix: String, reportId: String): String = {
    Paths.get(filePrefix, reportId).toString();
  }

  private def filePaths(dims: Seq[String], row: Row, format: String, tempDir: String, finalDir: String): (String, String) = {

    val dimPaths = for (dim <- dims) yield {
      dim + "=" + row.get(row.fieldIndex(dim))
    }

    val paths = for (dim <- dims) yield {
      row.get(row.fieldIndex(dim))
    }

    (Paths.get(tempDir, dimPaths.mkString("/")).toString(), Paths.get(finalDir, paths.mkString("/")) + "." + format)
  }

  def saveToBlobStore(storageConfig: StorageConfig, format: String, reportId: String, options: Option[Map[String, String]],
                      partitioningColumns: Option[Seq[String]], storageService: Option[BaseStorageService] = None,
                      zip: Option[Boolean] = Option(false), columnOrder: Option[List[String]] = Option(List()), fileExt:Option[String] = None): List[String] = {

    val conf = df.sparkSession.sparkContext.hadoopConfiguration;

    val file = storageConfig.store.toLowerCase() match {
      case "s3" =>
        CommonUtil.getS3FileWithoutPrefix(storageConfig.container, storageConfig.fileName);
      case "azure" =>
        CommonUtil.getAzureFileWithoutPrefix(storageConfig.container, storageConfig.fileName, storageConfig.accountKey.getOrElse("azure_storage_key"))
      case "gcloud" =>
        CommonUtil.getGCloudFileWithoutPrefix(storageConfig.container, storageConfig.fileName);
      case "oci" =>
        CommonUtil.getS3FileWithoutPrefix(storageConfig.container, storageConfig.fileName);
      case _ =>
        storageConfig.fileName
    }

    val filePrefix = storageConfig.store.toLowerCase() match {
      case "s3" =>
        "s3n://"
      case "azure" =>
        "wasb://"
      case "gcloud" =>
        "gs://"
      case "oci" =>
        "s3n://"
      case _ =>
        ""
    }

    val tempDir = getTempDir(file, reportId);
    val finalDir = getFinalDir(file, reportId);

    val dims = partitioningColumns.getOrElse(Seq())
    var headersList = columnOrder.getOrElse(List())
    fileUtil.delete(conf, filePrefix + tempDir)
    val opts = options.getOrElse(Map());
    val files = if (dims.nonEmpty) {
      if (zip.getOrElse(false)) {
        copyMergeFile(dims, filePrefix, tempDir, finalDir, conf, format, opts, storageConfig, storageService, zip, None, Some(file))
      }
      else
        copyMergeFile(dims, filePrefix, tempDir, finalDir, conf, format, opts, storageConfig, None, None, Some(headersList), None)
    } else {
      if (headersList.size > 0) {
        headersList = headersList ++ dims
        df.repartition(1).select(headersList.head, headersList.tail: _*).write.format(format).options(opts).save(filePrefix + tempDir)
      }
      else {
        df.repartition(1).write.format(format).options(opts).save(filePrefix + tempDir)
      }
      fileUtil.delete(conf, filePrefix + finalDir + "." + fileExt.getOrElse(format))
      fileUtil.copyMerge(filePrefix + tempDir, filePrefix + finalDir + "." + fileExt.getOrElse(format), conf, true);
      List(filePrefix + finalDir + "." + fileExt.getOrElse(format))
    }
    fileUtil.delete(conf, filePrefix + tempDir)
    files
  }

  def copyMergeFile(dims: Seq[String], filePrefix: String, srcPath: String, desPath: String, conf: Configuration,
                    format: String, opts: Map[String, String], storageConfig: StorageConfig,
                    storageService: Option[BaseStorageService] = None, zip: Option[Boolean] = Some(false),
                    columnOrder: Option[List[String]] = Some(List.empty[String]), filePath: Option[String]) = {
    fileUtil.delete(conf, filePrefix + srcPath)
    val map = df.select(dims.map(f => col(f)): _*).distinct().collect().map(f => filePaths(dims, f, format, srcPath, desPath)).toMap
    var headersList = columnOrder.getOrElse(List())
    if (headersList.size > 0) {
      headersList = headersList ++ dims
      df.repartition(1).select(headersList.head, headersList.tail: _*).write.format(format).options(opts).partitionBy(dims: _*).save(filePrefix + srcPath)
    } else {
      df.repartition(1).write.format(format).options(opts).partitionBy(dims: _*).save(filePrefix + srcPath)
    }
    map.foreach(f => {
      fileUtil.delete(conf, filePrefix + f._2)
      fileUtil.copyMerge(filePrefix + f._1, filePrefix + f._2, conf, true)
      if (zip.getOrElse(false)) {
        storageConfig.store.toLowerCase() match {
          case "local" =>
            new ZipFile((filePrefix + f._2).replace(format, "zip")).addFile(new File(filePrefix + f._2))
            fileUtil.copy(f._2.replace(format, "zip"), f._2, conf)
          case _ =>
            val fileName = Paths.get(filePrefix + f._2).getFileName.toString
            val reportPath = f._2.replace(filePath.get, "")
            val tmpDir = AppConf.getConfig("spark_output_temp_dir") + reportPath.replace(fileName, "")
            val localPath = tmpDir + fileName
            storageService.get.download(storageConfig.container, reportPath, tmpDir, Some(false))
            val zipFile = new ZipFile(localPath.replace(format, "zip"))
            zipFile.addFile(new File(localPath))
            storageService.get.upload(storageConfig.container, localPath.replace(format, "zip"),
              reportPath.replace(format, "zip"), Some(false), Some(0), Some(3), None)
            fileUtil.delete(conf, tmpDir)
        }
        fileUtil.delete(conf, filePrefix + f._2)
      }
    })
    fileUtil.delete(conf, filePrefix + srcPath)
    map.map{f =>
      if(zip.getOrElse(false))
        (filePrefix + f._2).replace(format, "zip")
      else
        (filePrefix + f._2)
    }.toList
  }

}

object DatasetUtil {
  implicit def extensions(df: Dataset[Row]) = new DatasetExt(df);

}