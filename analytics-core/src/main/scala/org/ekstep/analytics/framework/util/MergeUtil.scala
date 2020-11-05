package org.ekstep.analytics.framework.util

import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.ekstep.analytics.framework.{FrameworkContext, MergeScriptConfig, StorageConfig}


class MergeUtil {

    def mergeFile(mergeConfig: MergeScriptConfig)(implicit sc: SparkContext,fc:FrameworkContext): Unit = {
        implicit val sqlContext = new SQLContext(sc)
        import org.ekstep.analytics.framework.util.DatasetUtil.extensions
        mergeConfig.merge.files.foreach(filePaths => {
            val mergeDF = mergeConfig.`type`.toLowerCase() match {
                case "local" =>
                    val deltaDF = sqlContext.read.options(Map("header" -> "true")).csv(filePaths("deltaPath"))
                    val reportDF = sqlContext.read.options(Map("header" -> "true")).csv(filePaths("reportPath"))
                    reportMerge(deltaDF, reportDF, mergeConfig)
                case "azure" =>
                    val deltaDF = downloadAzureFile(filePaths("deltaPath"),
                        mergeConfig.deltaFileAccess.getOrElse(true), mergeConfig.container)

                    val reportDF = downloadAzureFile(filePaths("reportPath"), mergeConfig.reportFileAccess.getOrElse(true),
                        mergeConfig.postContainer.getOrElse("reports"))
                    reportMerge(deltaDF, reportDF, mergeConfig)
                case _ =>
                    throw new Exception("Merge type unknown");
            }
            mergeDF.show(false)
            val path = new Path(filePaths("reportPath"))
            val storageConfig =
                mergeConfig.`type`.toLowerCase() match {
                    case "local" =>
                        StorageConfig(mergeConfig.`type`, null, FilenameUtils.getFullPathNoEndSeparator(filePaths("reportPath")))
                    case "azure" =>
                        StorageConfig(mergeConfig.`type`, mergeConfig.postContainer.get, path.getParent.getName)
                    case _ =>
                        throw new Exception("Merge type unknown");
                }
            mergeDF.saveToBlobStore(storageConfig, "csv", FilenameUtils.removeExtension(path.getName), Option(Map("header" -> "true", "mode" -> "overwrite")), None)
            mergeDF.saveToBlobStore(storageConfig, "json", FilenameUtils.removeExtension(path.getName), Option(Map("header" -> "true", "mode" -> "overwrite")), None)
            })
    }



    def reportMerge(delta: DataFrame, reportDF: DataFrame, mergeConfig: MergeScriptConfig): DataFrame =
    {

        val reportDfColumns = reportDF.columns
        import org.apache.spark.sql.functions._

        val deltaDF =if(mergeConfig.rollup > 0 && reportDF.columns.contains(mergeConfig.rollupCol.get))
            delta.withColumn("Date", date_format(col("Date"), "dd-MM-yyyy"))
        else
            delta
        val filteredDf = reportDF.as("report").join(deltaDF.as("delta"),
            col("report."+ mergeConfig.rollupCol.get) === col("delta."+mergeConfig.rollupCol.get), "inner")
          .select("report.*")

        val finalDf = reportDF.except(filteredDf).union(deltaDF.dropDuplicates()
          .drop(deltaDF.columns.filter(p => !reportDfColumns.contains(p)): _*)
          .select(reportDfColumns.head, reportDfColumns.tail: _*)).orderBy(unix_timestamp(col("Date"), "dd-MM-yyyy"))
        finalDf

    }

    def downloadAzureFile(filePath: String, isPrivate: Boolean, container: String)(implicit sqlContext: SQLContext, fc: FrameworkContext): DataFrame = {

        val storageService =
            if (isPrivate)
                fc.getStorageService("azure", "azure_storage_key", "azure_storage_secret")
            else
                fc.getStorageService("azure", "report_storage_key", "report_storage_secret")
        val keys = storageService.searchObjects(container, filePath)

        sqlContext.read.options(Map("header"->"true")).csv(storageService.getPaths(container, keys).toArray.mkString(","))
    }

}
