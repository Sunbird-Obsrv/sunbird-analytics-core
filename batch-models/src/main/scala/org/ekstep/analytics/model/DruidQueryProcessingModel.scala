package org.ekstep.analytics.model


import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.util.LongAccumulator
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.driver.BatchJobDriver.getMetricJson
import org.ekstep.analytics.framework.exception.DruidConfigException
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util._
import org.ekstep.analytics.util.{BaseDruidQueryProcessor, DruidQueryUtil}
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.immutable.List
import scala.collection.mutable.LinkedHashMap

case class ReportMergeConfig(`type`:Option[String]=None, frequency: String, basePath: String, rollup: Integer, rollupAge: Option[String] = None,
                             rollupCol: Option[String] = None, rollupRange: Option[Integer] = None,reportPath: String,
                             postContainer: Option[String] = None, deltaFileAccess: Option[Boolean] = Option(true),
                             reportFileAccess: Option[Boolean] = Option(true),dateFieldRequired:Option[Boolean] = Option(true))
case class ReportConfig(id: String, queryType: String, dateRange: QueryDateRange, metrics: List[Metrics], labels: LinkedHashMap[String, String], output: List[OutputConfig],
                        mergeConfig: Option[ReportMergeConfig] = None, storageKey: Option[String] = Option(AppConf.getConfig("storage.key.config")),
                        storageSecret: Option[String] = Option(AppConf.getConfig("storage.secret.config")))
case class QueryDateRange(interval: Option[QueryInterval], staticInterval: Option[String], granularity: Option[String], intervalSlider: Int = 0)
case class QueryInterval(startDate: String, endDate: String)
case class Metrics(metric: String, label: String, druidQuery: DruidQueryModel)
case class OutputConfig(`type`: String, label: Option[String], metrics: List[String], dims: List[String] = List(), fileParameters: List[String] = List("id", "dims"),
                        locationMapping: Option[Boolean] = Option(false),zip: Option[Boolean] = Option(false))



object DruidQueryProcessingModel extends IBatchModelTemplate[DruidOutput, DruidOutput, DruidOutput, DruidOutput] with BaseDruidQueryProcessor with Serializable {

  implicit override val className: String = "org.ekstep.analytics.model.DruidQueryProcessingModel"
  override def name: String = "DruidQueryProcessingModel"



  override def preProcess(data: RDD[DruidOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DruidOutput] = {

    val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(config.getOrElse("reportConfig", Map()).asInstanceOf[Map[String, AnyRef]]))
    val key = reportConfig.storageKey.getOrElse(AppConf.getConfig("storage.key.config"))
    val secret = reportConfig.storageSecret.getOrElse(AppConf.getConfig("storage.secret.config"))
    setStorageConf(getStringProperty(config, "store", "local"), Option(key), Option(secret))
    data
  }

  override def algorithm(data: RDD[DruidOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DruidOutput] = {
    val streamQuery = config.getOrElse("streamQuery", false).asInstanceOf[Boolean]
    val exhaustQuery = config.getOrElse("exhaustQuery", false).asInstanceOf[Boolean]
    val strConfig = config("reportConfig").asInstanceOf[Map[String, AnyRef]]
    val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(strConfig))

    fetchDruidData(reportConfig, streamQuery, exhaustQuery)
  }


  override def postProcess(data: RDD[DruidOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DruidOutput] = {
      val configMap = config("reportConfig").asInstanceOf[Map[String, AnyRef]]
      val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap))

      val dimFields = reportConfig.metrics.flatMap { m =>
        if (m.druidQuery.dimensions.nonEmpty) m.druidQuery.dimensions.get.map(f => f.aliasName.getOrElse(f.fieldName))
        else if(m.druidQuery.sqlDimensions.nonEmpty) m.druidQuery.sqlDimensions.get.map(f => f.fieldName)
        else List()
      }
      val labelsLookup = reportConfig.labels ++ Map("date" -> "Date")
      implicit val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._
      //Using foreach as parallel execution might conflict with local file path
      val dataCount = sc.longAccumulator("DruidReportCount")
      reportConfig.output.foreach { f =>
        val df = getReportDF(RestUtil,f,data,dataCount).na.fill(0)
        if (dataCount.value > 0) {
          val metricFields = f.metrics.distinct
          val fieldsList = (dimFields ++ metricFields ++ List("date")).distinct
          val metricsLabels=  metricFields.map(f=> labelsLookup.getOrElse(f,f)).distinct
          val columnOrder = (List("Date") ++ dimFields.map(f=> labelsLookup.getOrElse(f,f))
            .filter(f => !metricFields.contains(f)) ++ metricsLabels).distinct
          val dimsLabels = labelsLookup.filter(x => f.dims.contains(x._1)).values.toList
          val filteredDf = df.select(fieldsList.head, fieldsList.tail: _*)
          val renamedDf = filteredDf.select(filteredDf.columns.map(c => filteredDf.col(c).as(labelsLookup.getOrElse(c, c))): _*).na.fill("unknown")
          val reportFinalId = if (f.label.nonEmpty && f.label.get.nonEmpty) reportConfig.id + "/" + f.label.get else reportConfig.id
          val filesWithSize = saveReport(renamedDf, config ++ Map("dims" -> dimsLabels, "metricLabels" -> metricsLabels,
          "reportId" -> reportFinalId, "fileParameters" -> f.fileParameters, "format" -> f.`type`), f.zip, Option(columnOrder))
          val totalFileSize = filesWithSize.map(f => f._2).sum
          sendMetricsEventToKafka(getMetricJson(reportFinalId, Option(new DateTime().toString(CommonUtil.dateFormat)), "SUCCESS",
            List(Map("id" -> "output-file-size", "value" -> totalFileSize.asInstanceOf[AnyRef]))))
          JobLogger.log(reportConfig.id + "Total Records :"+ dataCount.value , None, Level.INFO)
        }
        else {
          JobLogger.log("No data found from druid", None, Level.INFO)
        }
      }
    data
    }
}
