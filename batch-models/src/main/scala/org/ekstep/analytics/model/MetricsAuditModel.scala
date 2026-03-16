package org.ekstep.analytics.model

import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils}

case class DruidMetrics(total_count: Long, date: String)
@scala.beans.BeanInfo
case class V3Flags(flags: V3FlagContent)
case class V3ContextEvent(context: V3Context)

object MetricsAuditModel extends IBatchModelTemplate[Empty, Empty, V3DerivedEvent, V3DerivedEvent] with Serializable {

  val className = "org.ekstep.analytics.model.MetricsAuditJob"
  override def name: String = "MetricsAuditJob"

  override def preProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
    events;
  }

  override def algorithm(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[V3DerivedEvent] = {

    val auditConfig = config("auditConfig").asInstanceOf[List[Map[String, AnyRef]]]
    val metrics = auditConfig.map{f =>
      var metricsEvent: V3DerivedEvent = null
      val queryConfig = JSONUtils.deserialize[JobConfig](JSONUtils.serialize(f))
      val queryType = queryConfig.search.`type`

      queryType match {
        case "azure" | "local" =>
          metricsEvent = getSecorMetrics(queryConfig)
        case "druid" =>
          metricsEvent = getDruidCount(queryConfig)
      }
      metricsEvent
    }
    sc.parallelize(metrics)
  }

  override def postProcess(events: RDD[V3DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[V3DerivedEvent] = {
    events;
  }


  def getSecorMetrics(queryConfig: JobConfig)(implicit sc: SparkContext, fc: FrameworkContext): V3DerivedEvent = {
    var metricEvent: List[V3MetricEdata] = null
    val name = queryConfig.name.get
    name match {
      case "denorm" =>
        val rdd = DataFetcher.fetchBatchData[V3Flags](queryConfig.search)
        metricEvent = getDenormSecorAudit(rdd, queryConfig.filters.get)
      case "failed" =>
        val rdd = DataFetcher.fetchBatchData[V3ContextEvent](queryConfig.search)
        metricEvent = getFailedSecorAudit(rdd)
      case _ =>
        val rdd = DataFetcher.fetchBatchData[String](queryConfig.search)
        metricEvent = getTotalSecorCountAudit(rdd)
    }
    val v3metric = CommonUtil.getMetricEvent(Map("system" -> "SecorAuditBackup", "subsystem" -> name,
      "metrics" -> metricEvent), AppConf.getConfig("metric.producer.id"), AppConf.getConfig("metric.producer.pid"))
    v3metric
  }

  def getTotalSecorCountAudit(rdd: RDD[String]): List[V3MetricEdata] = {

    val totalCount = rdd.count()
    val metricData = List(V3MetricEdata("date", Option(new Date())), V3MetricEdata("inputEvents", Option(totalCount)))
    metricData
  }

  def getFailedSecorAudit(rdd: RDD[V3ContextEvent]): List[V3MetricEdata] = {
    val failedCountByPID = rdd.filter(f => null != f.context.pdata).groupBy(f => f.context.pdata.get.id)
    val metricData = failedCountByPID.map(f => V3MetricEdata(f._1, Some(f._2.size)))
    metricData.collect().toList
  }

  def getDenormSecorAudit(rdd: RDD[V3Flags], filters: Array[Filter])(implicit sc: SparkContext): List[V3MetricEdata] = {
    val denormCount = rdd.count()
    val metricData = filters.map{f =>
      val filteredRdd = rdd.filter(x => null != x.flags)
      val filterCount = DataFilter.filter(filteredRdd, f).count()
      val diff = if(denormCount > 0) filterCount * 100 / denormCount else 0
      List(V3MetricEdata(f.name.substring(6), Option(filterCount)), V3MetricEdata("percentage_events_with_" + f.name.substring(6), Some(diff)))
    }.flatten.toList
    metricData ++ List(V3MetricEdata("count", Some(denormCount)))
  }

  def getDruidCount(queryConfig: JobConfig)(implicit sc: SparkContext, fc: FrameworkContext): V3DerivedEvent = {
    val name = queryConfig.name.get
    val data = DataFetcher.fetchBatchData[DruidMetrics](queryConfig.search).first()
    val metrics = List(V3MetricEdata(name, Some(data.total_count)),V3MetricEdata("date", Some(data.date)))
    val metricData = CommonUtil.getMetricEvent(Map("system" -> "DruidCount", "subsystem" -> name, "metrics" -> metrics), AppConf.getConfig("metric.producer.id"), AppConf.getConfig("metric.producer.pid"))
    metricData
  }
}
