package org.ekstep.analytics.framework.util

import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, unix_timestamp, _}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.{FrameworkContext, MergeConfig, StorageConfig}
import org.joda.time
import org.joda.time.format.DateTimeFormat

case class MergeResult(updatedReportDF: DataFrame, oldReportDF: DataFrame, storageConfig: StorageConfig)


class MergeUtil {
  implicit val className = "org.ekstep.analytics.framework.util.MergeUtil"
  val druidDateFormat =AppConf.getConfig("druid.report.date.format")
  def mergeFile(mergeConfig: MergeConfig)(implicit sc: SparkContext, fc: FrameworkContext): Unit = {
    implicit val sqlContext = new SQLContext(sc)
    var reportSchema: StructType = null
    val rollupColOption = """\|\|""".r.split(mergeConfig.rollupCol.getOrElse("Date"))
    val rollupCol = rollupColOption.apply(0)
    val rollupFormat = mergeConfig.rollupFormat.getOrElse({
      if (rollupColOption.length > 1) rollupColOption.apply(1).replaceAll("%Y", "yyyy").replaceAll("%m", "MM")
        .replaceAll("%d", "dd") else  druidDateFormat
    })
    mergeConfig.merge.files.foreach(filePaths => {
      val isPrivate = mergeConfig.reportFileAccess.getOrElse(true)
      val storageKey= if(isPrivate) "cloud_storage_key" else "druid_storage_account_key"
      val storageSecret= if(isPrivate) "cloud_storage_secret" else "druid_storage_account_secret"
      val metricLabels= mergeConfig.metricLabels.getOrElse(List())
      val path = new Path(filePaths("reportPath"))
      val postContainer= mergeConfig.postContainer.getOrElse(AppConf.getConfig("druid.report.default.container"))
      val storageType = mergeConfig.`type`.getOrElse(AppConf.getConfig("druid.report.default.storage"))
      var columnOrder = mergeConfig.columnOrder.getOrElse(List())
      if(columnOrder.size > 0)
        columnOrder = (List(rollupCol)++ columnOrder).distinct
      if(!mergeConfig.dateRequired.getOrElse(true) || !rollupCol.equals("Date"))
        columnOrder = columnOrder.filter(p=> !p.equals("Date"))
      val mergeResult = storageType.toLowerCase() match {
        case "local" =>
          var deltaDF = sqlContext.read.options(Map("header" -> "true")).csv(filePaths("deltaPath"))
          deltaDF = (if(deltaDF.columns.contains("Date")) deltaDF.withColumn(rollupCol,
            date_format(col("Date"), rollupFormat)) else deltaDF).dropDuplicates()
          val reportDF = if (new java.io.File(filePaths("reportPath")).exists)
            sqlContext.read.options(Map("header" -> "true")).csv(filePaths("reportPath"))
          else deltaDF
          val reportDfColumns = reportDF.columns
          columnOrder = columnOrder.filter(col=> reportDfColumns.contains(col))
          MergeResult(mergeReport(rollupCol,rollupFormat,deltaDF, reportDF, mergeConfig, mergeConfig.merge.dims), reportDF,
            StorageConfig(storageType, null, FilenameUtils.getFullPathNoEndSeparator(filePaths("reportPath"))))
        case "azure" =>
          var deltaDF = fetchBlobFile(filePaths("deltaPath"),
            mergeConfig.deltaFileAccess.getOrElse(true), mergeConfig.container)
            deltaDF = (if(deltaDF.columns.contains("Date")) deltaDF.withColumn(rollupCol,
              date_format(col("Date"), rollupFormat)) else deltaDF).dropDuplicates()
          val reportFile = fetchBlobFile(filePaths("reportPath"), mergeConfig.reportFileAccess.getOrElse(true),
            postContainer)
          val reportDF = if (null == reportFile) {
            sqlContext.createDataFrame(sc.emptyRDD[Row], if (null == reportSchema) {
              deltaDF.schema
            } else reportSchema
            )
          }
          else {
            reportSchema = reportFile.schema
            reportFile
          }
          val reportDfColumns = reportDF.columns
          columnOrder = columnOrder.filter(col=> reportDfColumns.contains(col))
          MergeResult(mergeReport(rollupCol,rollupFormat,deltaDF, reportDF, mergeConfig, mergeConfig.merge.dims), reportDF,
            StorageConfig(storageType, postContainer, path.getParent.getName,Option(storageKey),Option(storageSecret)))
        case "s3" =>
          var deltaDF = fetchOSSFile(filePaths("deltaPath"),
            mergeConfig.deltaFileAccess.getOrElse(true), mergeConfig.container)
            deltaDF = (if(deltaDF.columns.contains("Date")) deltaDF.withColumn(rollupCol,
              date_format(col("Date"), rollupFormat)) else deltaDF).dropDuplicates()
          val reportFile = fetchOSSFile(filePaths("reportPath"), mergeConfig.reportFileAccess.getOrElse(true),
            postContainer)
          val reportDF = if (null == reportFile) {
            sqlContext.createDataFrame(sc.emptyRDD[Row], if (null == reportSchema) {
              deltaDF.schema
            } else reportSchema
            )
          }
          else {
            reportSchema = reportFile.schema
            reportFile
          }
          val reportDfColumns = reportDF.columns
          columnOrder = columnOrder.filter(col=> reportDfColumns.contains(col))
          MergeResult(mergeReport(rollupCol,rollupFormat,deltaDF, reportDF, mergeConfig, mergeConfig.merge.dims), reportDF,
            StorageConfig(storageType, postContainer, path.getParent.getName,Option(storageKey),Option(storageSecret)))
        case _ =>
          throw new Exception("Merge type unknown: "+storageType)
      }
      // Rename old file by appending date and store it
      try {
        val backupFilePrefix =String.format("%s-%s", FilenameUtils.removeExtension(path.getName), new time.DateTime().toString(druidDateFormat))
        saveReport(mergeResult.oldReportDF,mergeResult, backupFilePrefix,"csv",true)
        saveReport(convertReportToJsonFormat(sqlContext,mergeResult.oldReportDF,columnOrder,metricLabels),mergeResult, backupFilePrefix,"json",true)
        // Append new data to report file
        val updatedDF = mergeResult.updatedReportDF
        saveReport(convertReportToJsonFormat(sqlContext,updatedDF,columnOrder,metricLabels), mergeResult,
          FilenameUtils.removeExtension(path.getName),"json",false)
        saveReport(updatedDF,mergeResult, FilenameUtils.removeExtension(path.getName),"csv",
          false,Some(columnOrder))

      }catch {
        case ex : Exception =>
          Console.println(mergeConfig.id + " Merge failed while saving to blob: ", ex.printStackTrace())
          JobLogger.log(mergeConfig.id + " report merge failed with error : " +ex.getMessage, None, INFO)
      }
    })
  }

  def saveReport(df:DataFrame,mergeResult: MergeResult,fileName:String,format: String,backup:Boolean,
                 columnOrder: Option[List[String]] = Some(List())): Unit =
  {
    df.saveToBlobStore(StorageConfig(mergeResult.storageConfig.store,
      mergeResult.storageConfig.container, if(backup) "report-backups/"+mergeResult.storageConfig.fileName
      else mergeResult.storageConfig.fileName,mergeResult.storageConfig.accountKey,mergeResult.storageConfig.secretKey), format,
      fileName, Option(Map("header" -> "true", "mode" -> "overwrite")), None,None,None,columnOrder)
  }

  def mergeReport(rollupCol:String,rollupFormat:String,delta: DataFrame,
                  reportDF: DataFrame, mergeConfig: MergeConfig, dims: List[String]): DataFrame = {

    if (mergeConfig.rollup > 0) {
      val reportDfColumns = reportDF.columns
      val deltaDF = delta.drop(delta.columns.filter(p => !reportDfColumns.contains(p)): _*)
        .select(reportDfColumns.head, reportDfColumns.tail: _*)
      val filteredDf = mergeConfig.rollupCol.map { rollupCol =>
        val rollupColumn = """\|\|""".r.split(rollupCol).apply(0)
        reportDF.as("report").join(deltaDF.as("delta"),
          col("report." + rollupColumn) === col("delta." + rollupColumn), "inner")
          .select("report.*")
      }.getOrElse({
        reportDF.as("report").join(deltaDF.as("delta"), dims, "inner")
          .select("report.*")
      })

      val finalDf = reportDF.except(filteredDf).union(deltaDF)
      rollupReport(finalDf, mergeConfig, rollupCol, rollupFormat).orderBy(unix_timestamp(col(rollupCol), rollupFormat))
    }
    else
      delta
  }

  def rollupReport(reportDF: DataFrame, mergeConfig: MergeConfig, rollupCol: String, rollupFormat: String): DataFrame = {
    val subtract = (x: Int, y: Int) => x - y
    val rollupRange = subtract(mergeConfig.rollupRange.get, 1)
    val maxDate = reportDF.agg(max(unix_timestamp(col(rollupCol)
      , rollupFormat)) as "Max").collect().apply(0).getAs[Long]("Max")
    val convert = (x: Long) => x * 1000L
    val endDate = new time.DateTime(convert(maxDate))
    var endYear = endDate.year().get()
    var endMonth = endDate.monthOfYear().get()
    val startDate = mergeConfig.rollupAge.get match {
      case "ACADEMIC_YEAR" =>
        if (endMonth <= 5)
          endYear = subtract(subtract(endYear, 1), rollupRange)
        else
          endYear = subtract(endYear, rollupRange)
        new time.DateTime(endYear, 6, 1, 0, 0, 0)
      case "GEN_YEAR" =>
        endYear = subtract(endYear, rollupRange)
        new time.DateTime(endYear, 1, 1, 0, 0, 0)
      case "MONTH" =>
        endMonth = subtract(endMonth, rollupRange)
        endYear = if (endMonth < 1) endYear + ((if (endMonth != 0) endMonth else -1) / 12).floor.toInt else endYear
        endMonth = if (endMonth < 1) endMonth + 12 else endMonth
        new time.DateTime(endYear, endMonth, 1, 0, 0, 0)
      case "WEEK" =>
        endDate.withDayOfWeek(1).minusWeeks(rollupRange)
      case "DAY" =>
        endDate.minusDays(rollupRange)
      case _ =>
        new time.DateTime(1970, 1, 1, 0, 0, 0)
    }
    reportDF.filter(row =>{
      DateTimeFormat.forPattern(rollupFormat).parseDateTime(row.getAs[String](rollupCol))
      .getMillis >= startDate.getMillis
    })
  }

  def fetchBlobFile(filePath: String, isPrivate: Boolean, container: String)(implicit sqlContext: SQLContext, fc: FrameworkContext): DataFrame = {

    val storageService = if (isPrivate)
      fc.getStorageService("azure", "cloud_storage_key", "cloud_storage_secret")
    else {
      fc.getStorageService("azure", "druid_storage_account_key", "druid_storage_account_secret")
    }
    val keys = storageService.searchObjects(container, filePath)
    val reportPaths = storageService.getPaths(container, keys).toArray.mkString(",")
    if (reportPaths.nonEmpty)
      sqlContext.read.options(Map("header" -> "true","scientific" ->"false")).csv(reportPaths)
    else null
  }

  def fetchOSSFile(filePath: String, isPrivate: Boolean, container: String)(implicit sqlContext: SQLContext, fc: FrameworkContext): DataFrame = {

    val storageService = if (isPrivate)
      fc.getStorageService("oci", "cloud_storage_key", "cloud_storage_secret")
    else {
      fc.getStorageService("oci", "druid_storage_account_key", "druid_storage_account_secret")
    }
    val keys = storageService.searchObjects(container, filePath)
    val reportPaths = storageService.getPaths(container, keys).toArray.mkString(",")
    if (reportPaths.nonEmpty)
      sqlContext.read.options(Map("header" -> "true","scientific" ->"false")).csv(reportPaths)
    else null
  }

  def convertReportToJsonFormat(sqlContext: SQLContext, df: DataFrame,columnOrder:List[String],metricLabels:List[String]): DataFrame = {
    import sqlContext.implicits._
    val cols = if(columnOrder.size>0) columnOrder.toArray else df.columns
    df.map(row =>{
      val dataMap = cols.map(col => (col,if(metricLabels.contains(col))
        BigDecimal(if(null != row.getAs[String](col))row.getAs[String](col) else "0").toString()
      else row.getAs[String](col))).toMap
      (cols, cols.map(col=> dataMap.get(col)), dataMap)}).groupBy("_1").agg(collect_list("_2").alias("tableData"),
      collect_list("_3").alias("data")).withColumnRenamed("_1", "keys")
  }
}