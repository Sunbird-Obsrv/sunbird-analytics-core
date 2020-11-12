package org.ekstep.analytics.framework.util

import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, unix_timestamp, _}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.{FrameworkContext, MergeConfig, StorageConfig}
import org.joda.time
import org.joda.time.format.DateTimeFormat

case class MergeResult(updatedReportDF : DataFrame, oldReportDF : DataFrame, storageConfig : StorageConfig)
class MergeUtil {

    def mergeFile(mergeConfig: MergeConfig)(implicit sc: SparkContext, fc: FrameworkContext): Unit = {
        implicit val sqlContext = new SQLContext(sc)
        mergeConfig.merge.files.foreach(filePaths => {
            val path = new Path(filePaths("reportPath"))
            val mergeResult = mergeConfig.`type`.toLowerCase() match {
                case "local" =>
                    val deltaDF = sqlContext.read.options(Map("header" -> "true")).csv(filePaths("deltaPath"))
                    val reportDF = sqlContext.read.options(Map("header" -> "true")).csv(filePaths("reportPath"))
                    MergeResult(mergeReport(deltaDF, reportDF, mergeConfig, mergeConfig.merge.dims), reportDF,
                      StorageConfig(mergeConfig.`type`, null, FilenameUtils.getFullPathNoEndSeparator(filePaths("reportPath"))))

                case "azure" =>
                    val deltaDF = fetchBlobFile(filePaths("deltaPath"),
                        mergeConfig.deltaFileAccess.getOrElse(true), mergeConfig.container)

                    val reportDF = fetchBlobFile(filePaths("reportPath"), mergeConfig.reportFileAccess.getOrElse(true),
                        mergeConfig.postContainer.getOrElse("reports"))
                    MergeResult(mergeReport(deltaDF, reportDF, mergeConfig, mergeConfig.merge.dims), reportDF,
                      StorageConfig(mergeConfig.`type`, mergeConfig.postContainer.get, path.getParent.getName))

                case _ =>
                    throw new Exception("Merge type unknown")
            }
            // Rename old file with appending date
            mergeResult.oldReportDF.saveToBlobStore(mergeResult.storageConfig, "csv",
                String.format("%s-%s",FilenameUtils.removeExtension(path.getName), new time.DateTime().toString("yyyy-MM-dd")),
                Option(Map("header" -> "true", "mode" -> "overwrite")), None)
            mergeResult.oldReportDF.saveToBlobStore(mergeResult.storageConfig, "json",
                String.format("%s-%s",FilenameUtils.removeExtension(path.getName), new time.DateTime().toString("yyyy-MM-dd")),
                Option(Map("header" -> "true", "mode" -> "overwrite")), None)

            // Append new data to report file
            mergeResult.updatedReportDF.saveToBlobStore(mergeResult.storageConfig, "csv", FilenameUtils.removeExtension(path.getName),
                Option(Map("header" -> "true", "mode" -> "overwrite")), None)
            mergeResult.updatedReportDF.saveToBlobStore(mergeResult.storageConfig, "json", FilenameUtils.removeExtension(path.getName),
                Option(Map("header" -> "true", "mode" -> "overwrite")), None)

        })
    }


    def mergeReport(delta: DataFrame, reportDF: DataFrame, mergeConfig: MergeConfig, dims: List[String]): DataFrame = {

        if (mergeConfig.rollup > 0) {
            val rollupFormat = mergeConfig.rollupFormat.getOrElse("dd-MM-yyyy")
            val reportDfColumns = reportDF.columns
            val rollupCol = mergeConfig.rollupCol.getOrElse("Date")
            val deltaDF = delta.withColumn(rollupCol, date_format(col(rollupCol), rollupFormat)).dropDuplicates()
              .drop(delta.columns.filter(p => !reportDfColumns.contains(p)): _*)
              .select(reportDfColumns.head, reportDfColumns.tail: _*)

            val filteredDf = mergeConfig.rollupCol.map { rollupCol =>
                reportDF.as("report").join(deltaDF.as("delta"),
                    col("report." + rollupCol) === col("delta." + rollupCol), "inner")
                  .select("report.*")
            }.getOrElse({
                reportDF.as("report").join(deltaDF.as("delta"), dims,"inner")
                  .select("report.*")
            })

            val finalDf = reportDF.except(filteredDf).union(deltaDF)
            rollupReport(finalDf, mergeConfig).orderBy(unix_timestamp(col(rollupCol), rollupFormat))
        }
        else
            delta
    }

    def rollupReport(reportDF: DataFrame, mergeConfig: MergeConfig): DataFrame = {
        val rollupFormat = mergeConfig.rollupFormat.getOrElse("dd-MM-yyyy")
        val subtract = (x: Int, y: Int) => x - y
        val rollupRange = subtract(mergeConfig.rollupRange.get, 1)
        val maxDate = reportDF.agg(max(unix_timestamp(col(mergeConfig.rollupCol.getOrElse("Date"))
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
                endDate.minusDays(rollupRange.toInt)
            case _ =>
                new time.DateTime(1970, 1, 1, 0, 0, 0)
        }
        reportDF.filter(p => DateTimeFormat.forPattern(rollupFormat)
          .parseDateTime(p.getAs[String]("Date"))
          .getMillis >= startDate.asInstanceOf[time.DateTime].getMillis)
    }

    def fetchBlobFile(filePath: String, isPrivate: Boolean, container: String)(implicit sqlContext: SQLContext, fc: FrameworkContext): DataFrame = {

        val storageService =
            if (isPrivate)
                fc.getStorageService("azure", "azure_storage_key", "azure_storage_secret")
            else
                fc.getStorageService("azure", "report_storage_key", "report_storage_secret")
        val keys = storageService.searchObjects(container, filePath)

        sqlContext.read.options(Map("header" -> "true")).csv(storageService.getPaths(container, keys).toArray.mkString(","))
    }
}
