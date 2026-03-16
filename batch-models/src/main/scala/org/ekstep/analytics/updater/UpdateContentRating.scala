package org.ekstep.analytics.updater

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{HTTPClient, JSONUtils, JobLogger, RestUtil}
import org.joda.time.DateTime

import java.io.Serializable

case class Response(id: String, ver: String, ts: String, params: Params, responseCode: String, result: Map[String, AnyRef])

case class ContentMetrics(
                           contentId: String,
                           totalRatingsCount: Option[Long],
                           averageRating: Option[Double],
                           totalTimeSpentInApp: Option[Long],
                           totalTimeSpentInPortal: Option[Long],
                           totalTimeSpentInDeskTop: Option[Long],
                           totalPlaySessionCountInApp: Option[Long],
                           totalPlaySessionCountInPortal: Option[Long],
                           totalPlaySessionCountInDeskTop: Option[Long]
                         ) extends AlgoOutput with Output

object UpdateContentRating extends IBatchModelTemplate[Empty, Empty, ContentMetrics, ContentMetrics] with Serializable {

  implicit val className = "org.ekstep.analytics.updater.UpdateContentRating"

  override def name: String = "UpdateContentRating"


  override def preProcess(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
    data
  }

  override def algorithm(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[ContentMetrics] = {
    getContentConsumptionMetrics(config, RestUtil)
  }

  override def postProcess(data: RDD[ContentMetrics], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[ContentMetrics] = {
    val baseURL = AppConf.getConfig("lp.system.update.base.url")
    if (data.count() > 0) {
      data.foreach { contentMetrics: ContentMetrics =>
        if (contentMetrics.contentId != null && !contentMetrics.contentId.isEmpty) {
          val response = publishMetricsToContentModel(contentMetrics, baseURL, RestUtil)
          val msg = response.result.getOrElse("messages", List()).asInstanceOf[List[String]].mkString(",")
          JobLogger.log("System Update API request for " + contentMetrics.contentId + " is " + response.params.status.getOrElse(""), Option(Map("error" -> response.params.errmsg.getOrElse(""), "error_msg" -> msg)), Level.INFO)
        }
        else {
          JobLogger.log("Skipping system Update API request as content id is empty or null", None, Level.INFO)
        }
      }
    } else {
      JobLogger.log("No records to update", None, Level.INFO)
    }
    data
  }

  def getContentConsumptionMetrics(config: Map[String, AnyRef], restUtil: HTTPClient)(implicit sc: SparkContext, fc: FrameworkContext): RDD[ContentMetrics] = {
    val contentList = getRatedContents(config, restUtil)
    JobLogger.log("Rated Content Identifiers are" + JSONUtils.serialize(contentList), None, Level.INFO)
    val contentRatingList = getContentMetrics(restUtil, AppConf.getConfig("druid.content.rating.query"))
    val contentConsumptionList = getContentMetrics(restUtil, AppConf.getConfig("druid.content.consumption.query"))
    val finalContentRating = contentRatingList.filter(f => contentList.contains(f.contentId)).map { f =>
      (f.contentId, f)
    }
    val finalContentConsumptionList = contentConsumptionList.filter(f => contentList.contains(f.contentId)).map { f =>
      (f.contentId, f)
    }
    val defaultContentMetrics = ContentMetrics("", None, None, None, None, None, None, None, None)
    val finalData = finalContentRating.fullOuterJoin(finalContentConsumptionList)
    finalData.map { f =>
        val ratingData: ContentMetrics = f._2._1.getOrElse(defaultContentMetrics)
        val consumptionData: ContentMetrics = f._2._2.getOrElse(defaultContentMetrics)
        ContentMetrics(f._1,
          ratingData.totalRatingsCount,
          ratingData.averageRating,
          consumptionData.totalTimeSpentInApp,
          consumptionData.totalTimeSpentInPortal,
          consumptionData.totalTimeSpentInDeskTop,
          consumptionData.totalPlaySessionCountInApp,
          consumptionData.totalPlaySessionCountInPortal,
          consumptionData.totalPlaySessionCountInDeskTop
        )
      }
  }

  def getRatedContents(config: Map[String, AnyRef], restUtil: HTTPClient): List[String] = {
    val apiURL = AppConf.getConfig("druid.sql.host")
    val startDate = config.getOrElse("startDate", new DateTime().minusDays(1).toString("yyyy-MM-dd")).asInstanceOf[String]
    var endDate = config.getOrElse("endDate", new DateTime().toString("yyyy-MM-dd")).asInstanceOf[String]
    if (startDate.equals(endDate)) endDate = new DateTime(endDate).plusDays(1).toString("yyyy-MM-dd")
    val contentRequest = AppConf.getConfig("druid.unique.content.query").format(new DateTime(startDate).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss"), new DateTime(endDate).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss"))
    val contentResponse = restUtil.post[List[Map[String, AnyRef]]](apiURL, contentRequest)
    if (contentResponse != null)
      contentResponse.map(x => x.getOrElse("Id", "").toString)
    else List()
  }

  def getContentMetrics(restUtil: HTTPClient, query: String)(implicit sc: SparkContext): RDD[ContentMetrics] = {
    val apiURL = AppConf.getConfig("druid.sql.host")
    val metricsData = restUtil.post[List[Map[String, AnyRef]]](apiURL, query)
    if (null != metricsData)
      sc.parallelize(compute(metricsData))
    else
      sc.emptyRDD[ContentMetrics]
  }

  def compute(contentData: List[Map[String, AnyRef]]): List[ContentMetrics] = {
    contentData.map(x => {
      if (x.getOrElse("dimensions_pdata_id", "").toString.contains(".app")) {
        ContentMetrics(
          x.getOrElse("contentId", "").toString, None, None,
          Some(x.getOrElse("total_time_spent", 0).asInstanceOf[Number].longValue()), None, None,
          Some(x.getOrElse("play_sessions_count", 0).asInstanceOf[Number].longValue()), None, None
        )
      }
      else if (x.getOrElse("dimensions_pdata_id", "").toString.contains(".portal")) {
        ContentMetrics(
          x.getOrElse("contentId", "").toString, None, None,
          None, Some(x.getOrElse("total_time_spent", 0).asInstanceOf[Number].longValue()), None,
          None, Some(x.getOrElse("play_sessions_count", 0).asInstanceOf[Number].longValue()), None
        )
      }
      else if (x.getOrElse("dimensions_pdata_id", "").toString.contains(".desktop")) {
        ContentMetrics(
          x.getOrElse("contentId", "").toString, None, None,
          None, None, Some(x.getOrElse("total_time_spent", 0).asInstanceOf[Number].longValue()),
          None, None, Some(x.getOrElse("play_sessions_count", 0).asInstanceOf[Number].longValue())
        )
      } else {
        ContentMetrics(x.getOrElse("contentId", "").toString, Some(x.getOrElse("totalRatingsCount", 0L).asInstanceOf[Number].longValue()), Some(x.getOrElse("averageRating", 0.0).asInstanceOf[Number].doubleValue()), None, None, None, None, None, None)
      }
    }
    )
  }

  def publishMetricsToContentModel(contentMetrics: ContentMetrics, baseURL: String, restUtil: HTTPClient): Response = {
    val systemUpdateURL = baseURL + "/" + contentMetrics.contentId
    val meTotalTimeSpent = Map("app" -> contentMetrics.totalTimeSpentInApp.getOrElse(null), "portal" -> contentMetrics.totalTimeSpentInPortal.getOrElse(null), "desktop" -> contentMetrics.totalTimeSpentInDeskTop.getOrElse(null)).filter(_._2 != null)
    val meTotalPlaySessionCount = Map("app" -> contentMetrics.totalPlaySessionCountInApp.getOrElse(null), "portal" -> contentMetrics.totalPlaySessionCountInPortal.getOrElse(null), "desktop" -> contentMetrics.totalPlaySessionCountInDeskTop.getOrElse(null)).filter(_._2 != null)
    val request =
      s"""
         |{
         |  "request": {
         |    "content": {
         |      "me_totalRatingsCount": ${contentMetrics.totalRatingsCount.orNull},
         |      "me_averageRating": ${contentMetrics.averageRating.orNull},
         |      "me_totalTimeSpentInSec":${if(meTotalTimeSpent.isEmpty) null else JSONUtils.serialize(meTotalTimeSpent) },
         |      "me_totalPlaySessionCount":${if(meTotalPlaySessionCount.isEmpty) null else JSONUtils.serialize(meTotalPlaySessionCount)}
         |    }
         |  }
         |}
               """.stripMargin
    JobLogger.log("URL Is" + systemUpdateURL, None, Level.INFO)
    JobLogger.log("Request Is" + request, None, Level.INFO)
    val response = restUtil.patch[String](systemUpdateURL, JSONUtils.serialize(JSONUtils.deserialize[Map[String, AnyRef]](request)))
    JobLogger.log("Response Is" + response, None, Level.INFO)
    if (null != response)
      JSONUtils.deserialize[Response](response)
    else
      org.ekstep.analytics.updater.Response("", "", "", Params(Option(""), Option(""), Option("INTERNAL SERVER ERROR"), Option("500"), Option("System Update Request failed")), "", Map())
  }
}
