package org.ekstep.analytics.framework.util

import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, unix_timestamp, _}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.ekstep.analytics.framework.{FrameworkContext, MergeScriptConfig, StorageConfig}
import org.joda.time
import org.joda.time.format.DateTimeFormat
import org.ekstep.analytics.framework.util.DatasetUtil.extensions

class MergeUtil {

    def mergeFile(mergeConfig: MergeScriptConfig)(implicit sc: SparkContext, fc: FrameworkContext): Unit = {
        implicit val sqlContext = new SQLContext(sc)
        mergeConfig.merge.files.foreach(filePaths => {
            val path = new Path(filePaths("reportPath"))
            val mergeResult:(DataFrame, StorageConfig) = mergeConfig.`type`.toLowerCase() match {
                case "local" =>
                    val deltaDF = sqlContext.read.options(Map("header" -> "true")).csv(filePaths("deltaPath"))
                    val reportDF = sqlContext.read.options(Map("header" -> "true")).csv(filePaths("reportPath"))
                    (mergeReport(deltaDF, reportDF, mergeConfig),StorageConfig(mergeConfig.`type`, null, FilenameUtils.getFullPathNoEndSeparator(filePaths("reportPath"))))
                case "azure" =>
                    val deltaDF = downloadAzureFile(filePaths("deltaPath"),
                        mergeConfig.deltaFileAccess.getOrElse(true), mergeConfig.container)

                    val reportDF = downloadAzureFile(filePaths("reportPath"), mergeConfig.reportFileAccess.getOrElse(true),
                        mergeConfig.postContainer.getOrElse("reports"))
                    (mergeReport(deltaDF, reportDF, mergeConfig),StorageConfig(mergeConfig.`type`, mergeConfig.postContainer.get, path.getParent.getName))
                case _ =>
                    throw new Exception("Merge type unknown");
            }
            val mergeDF= mergeResult._1
            mergeDF.saveToBlobStore(mergeResult._2, "csv", FilenameUtils.removeExtension(path.getName), Option(Map("header" -> "true", "mode" -> "overwrite")), None)
            mergeDF.saveToBlobStore(mergeResult._2, "json", FilenameUtils.removeExtension(path.getName), Option(Map("header" -> "true", "mode" -> "overwrite")), None)
        })
    }


    def mergeReport(delta: DataFrame, reportDF: DataFrame, mergeConfig: MergeScriptConfig): DataFrame = {

        if (mergeConfig.rollup > 0) {
            val defaultFormat = "dd-MM-yyyy"
            val reportDfColumns = reportDF.columns
            val rollupCol = mergeConfig.rollupCol.getOrElse("Date")
            val deltaDF = delta.withColumn(rollupCol, date_format(col(rollupCol), defaultFormat))
            val filteredDf = reportDF.as("report").join(deltaDF.as("delta"),
                col("report." + rollupCol) === col("delta." + rollupCol), "inner")
              .select("report.*")

            val finalDf = reportDF.except(filteredDf).union(deltaDF.dropDuplicates()
              .drop(deltaDF.columns.filter(p => !reportDfColumns.contains(p)): _*)
              .select(reportDfColumns.head, reportDfColumns.tail: _*))
            rollupReport(finalDf, mergeConfig).orderBy(unix_timestamp(col(rollupCol), defaultFormat))
        }
        else
            delta
    }

    def rollupReport(reportDF: DataFrame, mergeScriptConfig: MergeScriptConfig): DataFrame = {
        val defaultFormat = "dd-MM-yyyy"
        val subtract = (x: Int, y: Int) => x - y
        val rollupRange = subtract(mergeScriptConfig.rollupRange.get,1)
        val maxDate = reportDF.agg(max(unix_timestamp(col("Date"),defaultFormat)) as "Max").collect().apply(0).getAs[Long]("Max")
        val convert = (x: Long) => x * 1000L
        val endDate = new time.DateTime(convert(maxDate))
        var endYear = endDate.year().get()
        var endMonth = endDate.monthOfYear().get()
        val startDate = mergeScriptConfig.rollupAge.get match {
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
        reportDF.filter(p => DateTimeFormat.forPattern(defaultFormat)
          .parseDateTime(p.getAs[String]("Date"))
          .getMillis >= startDate.asInstanceOf[time.DateTime].getMillis)
    }

    def downloadAzureFile(filePath: String, isPrivate: Boolean, container: String)(implicit sqlContext: SQLContext, fc: FrameworkContext): DataFrame = {

        val storageService =
            if (isPrivate)
                fc.getStorageService("azure", "azure_storage_key", "azure_storage_secret")
            else
                fc.getStorageService("azure", "report_storage_key", "report_storage_secret")
        val keys = storageService.searchObjects(container, filePath)

        sqlContext.read.options(Map("header" -> "true")).csv(storageService.getPaths(container, keys).toArray.mkString(","))
    }

}
