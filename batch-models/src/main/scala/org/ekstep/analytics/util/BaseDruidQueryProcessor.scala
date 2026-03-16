package org.ekstep.analytics.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.util.LongAccumulator
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.analytics.framework.driver.BatchJobDriver.getMetricJson
import org.ekstep.analytics.framework.exception.DruidConfigException
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CloudStorageProviders, CommonUtil, HTTPClient, JSONUtils, JobLogger, MergeUtil}
import org.ekstep.analytics.model.{OutputConfig, QueryInterval, ReportConfig}
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.immutable.List

trait BaseDruidQueryProcessor {

  implicit val className: String = "org.ekstep.analytics.util.BaseDruidQueryProcessor"

  def setStorageConf(store: String, accountKey: Option[String], accountSecret: Option[String])(implicit sc: SparkContext) {

    CloudStorageProviders.setSparkCSPConfigurations(sc, store, accountKey, accountSecret)
//    store.toLowerCase() match {
//      case "s3" =>
//        sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", AppConf.getConfig(accountKey.getOrElse("aws_storage_key")));
//        sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", AppConf.getConfig(accountSecret.getOrElse("aws_storage_secret")));
//      case "oci" =>
//        sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", AppConf.getConfig(accountKey.getOrElse("aws_storage_key")));
//        sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", AppConf.getConfig(accountSecret.getOrElse("aws_storage_secret")));
//      case "azure" =>
//        sc.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
//        sc.hadoopConfiguration.set("fs.azure.account.key." + AppConf.getConfig(accountKey.getOrElse("azure_storage_key")) + ".blob.core.windows.net", AppConf.getConfig(accountSecret.getOrElse("azure_storage_secret")))
//      case _ =>
//      // Do nothing
//    }

  }

  // Fetches data from druid and return back RDD
  def fetchDruidData(reportConfig: ReportConfig, streamQuery: Boolean = false, exhaustQuery: Boolean = false, foldByKey: Boolean = true)(implicit sc: SparkContext, fc: FrameworkContext): RDD[DruidOutput] = {

    val queryDims = reportConfig.metrics.map { f =>
      f.druidQuery.dimensions.getOrElse(List()).map(f => f.aliasName.getOrElse(f.fieldName))
    }.distinct

    if (queryDims.length > 1) throw new DruidConfigException("Query dimensions are not matching")

    val interval = reportConfig.dateRange
    val granularity = interval.granularity
    var reportInterval = if (interval.staticInterval.nonEmpty) {
      interval.staticInterval.get
    } else if (interval.interval.nonEmpty) {
      interval.interval.get
    } else {
      throw new DruidConfigException("Both staticInterval and interval cannot be missing. Either of them should be specified")
    }
    if(exhaustQuery) {
      val config = reportConfig.metrics.head.druidQuery
      val queryConfig = JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(config)) ++
        Map("intervalSlider" -> interval.intervalSlider, "intervals" ->
          (if (interval.staticInterval.isEmpty && interval.interval.nonEmpty) getDateRange(interval.interval.get,
            interval.intervalSlider, config.dataSource) else reportInterval), "granularity" -> granularity.get)
      DruidDataFetcher.executeSQLQuery(JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(queryConfig)), fc.getAkkaHttpUtil())
    }
    else {
      val metrics = reportConfig.metrics.map { f =>

        val queryInterval = if (interval.staticInterval.isEmpty && interval.interval.nonEmpty) {
          val dateRange = interval.interval.get
          getDateRange(dateRange, interval.intervalSlider, f.druidQuery.dataSource)
        } else
          reportInterval

        val queryConfig = if (granularity.nonEmpty)
          JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(f.druidQuery)) ++ Map("intervalSlider" -> interval.intervalSlider, "intervals" -> queryInterval, "granularity" -> granularity.get)
        else
          JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(f.druidQuery)) ++ Map("intervalSlider" -> interval.intervalSlider, "intervals" -> queryInterval)
        val data = if (streamQuery) {
          DruidDataFetcher.getDruidData(JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(queryConfig)), true)
        }
        else {
          DruidDataFetcher.getDruidData(JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(queryConfig)))
        }
        data.map { x =>
          val dataMap = JSONUtils.deserialize[Map[String, AnyRef]](x)
          val key = dataMap.filter(m => (queryDims.flatten ++ List("date")).contains(m._1)).values.map(f => f.toString).toList.sorted(Ordering.String.reverse).mkString(",")
          (key, dataMap)

        }

      }
      val finalResult = if (foldByKey) metrics.fold(sc.emptyRDD)(_ union _).foldByKey(Map())(_ ++ _) else metrics.fold(sc.emptyRDD)(_ union _)
      finalResult.map { f =>
        DruidOutput(f._2)
      }
    }
  }

  // Converts RDD data into Dataframe with optional location mapping feature
  def getReportDF(restUtil: HTTPClient, config: OutputConfig, data: RDD[DruidOutput] , dataCount: LongAccumulator)(implicit sc:SparkContext, sqlContext: SQLContext): DataFrame =
  {
    if (config.locationMapping.getOrElse(false)) {
      DruidQueryUtil.removeInvalidLocations(sqlContext.read.json(data.map(f => {
        dataCount.add(1)
        JSONUtils.serialize(f)
      })),
        DruidQueryUtil.getValidLocations(restUtil), List("state", "district"))
    } else
      sqlContext.read.json(data.map(f => {
        dataCount.add(1)
        JSONUtils.serialize(f)
      }))
  }

  // get date range from query interval
  def getDateRange(interval: QueryInterval, intervalSlider: Integer = 0, dataSource: String): String = {
    val offset :Long = if(dataSource.contains("rollup") || dataSource.contains("distinct")) 0 else DateTimeZone.forID("Asia/Kolkata").getOffset(DateTime.now())
    val startDate = DateTime.parse(interval.startDate).withTimeAtStartOfDay().minusDays(intervalSlider).plus(offset).toString("yyyy-MM-dd'T'HH:mm:ss")
    val endDate = DateTime.parse(interval.endDate).withTimeAtStartOfDay().minusDays(intervalSlider).plus(offset).toString("yyyy-MM-dd'T'HH:mm:ss")
    startDate + "/" + endDate
  }

  // get string property from config
  def getStringProperty(config: Map[String, AnyRef], key: String, defaultValue: String) : String = {
    config.getOrElse(key, defaultValue).asInstanceOf[String]
  }

  // saves report data as csv/zip file to specified path and also has merge report logic
  def saveReport(data: DataFrame, config: Map[String, AnyRef], zip: Option[Boolean], columnOrder: Option[List[String]])(implicit sc: SparkContext, fc:FrameworkContext): List[(String, Long)] = {
    import org.apache.spark.sql.functions.udf
    val container =  getStringProperty(config, "container", "test-container")
    val storageConfig = StorageConfig(getStringProperty(config, "store", "local"),container, getStringProperty(config, "key", "/tmp/druid-reports"), config.get("accountKey").asInstanceOf[Option[String]]);
    val format = config.get("format").get.asInstanceOf[String]
    val filePath = config.getOrElse("filePath", AppConf.getConfig("spark_output_temp_dir")).asInstanceOf[String]
    val key = config.getOrElse("key", null).asInstanceOf[String]
    val reportId = config.get("reportId").get.asInstanceOf[String]
    val fileParameters = config.get("fileParameters").get.asInstanceOf[List[String]]
    val configMap = config.getOrElse("reportConfig", Map()).asInstanceOf[Map[String, AnyRef]]
    val reportMergeConfig = if(configMap.nonEmpty) JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap)).mergeConfig else None
    val dims = if (fileParameters.nonEmpty && fileParameters.contains("date")) config.get("dims").get.asInstanceOf[List[String]] ++ List("Date") else config.get("dims").get.asInstanceOf[List[String]]
    val quoteColumns =config.get("quoteColumns").getOrElse(List()).asInstanceOf[List[String]]
    val deltaFiles = if (dims.nonEmpty) {
      val duplicateDims = dims.map(f => f.concat("Duplicate"))
      var duplicateDimsDf = data
      dims.foreach { f =>
        duplicateDimsDf = duplicateDimsDf.withColumn(f.concat("Duplicate"), col(f))
      }
      if(quoteColumns.nonEmpty) {
        val quoteStr = udf((column: String) =>  "\'"+column+"\'")
        quoteColumns.map(column => {
          duplicateDimsDf = duplicateDimsDf.withColumn(column, quoteStr(col(column)))
        })
      }
      if(zip.getOrElse(false)){
        val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(config.getOrElse("reportConfig", Map()).asInstanceOf[Map[String, AnyRef]]))
        storageConfig.store.toLowerCase match {
          case "local" => duplicateDimsDf.saveToBlobStore(storageConfig, format, reportId, Option(Map("header" -> "true")), Option(duplicateDims),
            None,zip)
          case _ => duplicateDimsDf.saveToBlobStore(storageConfig, format, reportId, Option(Map("header" -> "true")), Option(duplicateDims),
            Option(fc.getStorageService(storageConfig.store,
              reportConfig.storageKey.getOrElse("azure_storage_key") , reportConfig.storageSecret.getOrElse("azure_storage_secret"))),zip)
        }
      }
      else
        duplicateDimsDf.saveToBlobStore(storageConfig, format, reportId, Option(Map("header" -> "true")), Option(duplicateDims),None,None,columnOrder)
    } else {
      data.saveToBlobStore(storageConfig, format, reportId, Option(Map("header" -> "true")), None)
    }
    if(reportMergeConfig.nonEmpty){
      val mergeConf = reportMergeConfig.get
      val reportPath = mergeConf.reportPath
      val filesList = deltaFiles.map{f =>
        if(dims.size == 1 && dims.contains("Date")) {
          val finalReportPath = if (key != null) key + reportPath else reportPath
          Map("reportPath" -> finalReportPath, "deltaPath" -> f.substring(f.indexOf(storageConfig.fileName, 0)))
        } else {
          val reportPrefix = f.substring(0, f.lastIndexOf("/")).split(reportId)(1)
          Map("reportPath" -> (reportPrefix.substring(1) + "/" + reportPath), "deltaPath" -> f.substring(f.indexOf(storageConfig.fileName, 0)))
        }
      }
      val mergeScriptConfig = MergeConfig(mergeConf.`type`,reportId, mergeConf.frequency, mergeConf.basePath, mergeConf.rollup,
        mergeConf.rollupAge, mergeConf.rollupCol, None, mergeConf.rollupRange, MergeFiles(filesList, List("Date")), container, mergeConf.postContainer,
        mergeConf.deltaFileAccess, mergeConf.reportFileAccess,mergeConf.dateFieldRequired,columnOrder,
        Some(config.get("metricLabels").get.asInstanceOf[List[String]]))
      new MergeUtil().mergeFile(mergeScriptConfig)
    }
    else {
      JobLogger.log(s"Merge report is not configured, hence skipping that step", None, INFO)
    }
    val filesWithSize = deltaFiles.map{f =>
      val fileSize = fc.getHadoopFileUtil().size(f, sc.hadoopConfiguration)
      sendMetricsEventToKafka(getMetricJson(reportId, Option(new DateTime().toString(CommonUtil.dateFormat)), "SUCCESS",
        List(Map("id" -> "output-file-path", "value" -> f.asInstanceOf[AnyRef]),Map("id" -> "output-file-size", "value" -> fileSize.asInstanceOf[AnyRef]))))
      (f, fileSize)
    }
    filesWithSize
  }

  // $COVERAGE-OFF$
  def sendMetricsEventToKafka(metricEvent: String)(implicit  fc: FrameworkContext):Unit = {
    if (AppConf.getConfig("push.metrics.kafka").toBoolean)
      KafkaDispatcher.dispatch(Array(metricEvent), Map("topic" -> AppConf.getConfig("metric.kafka.topic"), "brokerList" -> AppConf.getConfig("metric.kafka.broker")))
  }

}
