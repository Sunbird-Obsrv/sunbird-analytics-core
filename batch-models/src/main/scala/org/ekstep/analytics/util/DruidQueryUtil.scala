package org.ekstep.analytics.util

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.ekstep.analytics.framework.Params
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{HTTPClient, RestUtil}


case class LocationList(count: Int, response: List[Map[String, String]])

case class LocationResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: LocationList)

case class LookUpMap(`type`: String, map: Map[String, String])

case class StateLookup(lookupExtractorFactory: LookUpMap)

object DruidQueryUtil {
    def removeInvalidLocations(mainDf: DataFrame, filterDf: DataFrame, columns: List[String])(implicit sc: SparkContext): DataFrame = {
        if (filterDf.count() > 0) {
            mainDf.join(filterDf, columns, "inner")
        }
        else {
            mainDf
        }
    }


    def getValidLocations(restUtil: HTTPClient)(implicit sc: SparkContext): DataFrame = {
        implicit val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        val locationUrl = AppConf.getConfig("location.search.url")
        val headers = Map("Authorization" -> AppConf.getConfig("location.search.token"))
        val request = AppConf.getConfig("location.search.request")
        val response = restUtil.post[LocationResponse](locationUrl, request, Option(headers))
        val stateLookup = getStateLookup(restUtil)
        val masterDf = if (null != response && !stateLookup.isEmpty) {
            val states = response.result.response.map(f => {
                if (f.getOrElse("type", "").equalsIgnoreCase("state"))
                    (f.get("id").get, f.get("name").get)
                else
                    ("", "")
            }).filter(f => !f._1.isEmpty)
            val districts = response.result.response.map(f => {
                if (f.getOrElse("type", "").equalsIgnoreCase("district"))
                    (f.get("parentId").get, f.get("name").get)
                else
                    ("", "")
            }).filter(f => !f._1.isEmpty)
            val masterData = states.map(tup1 => districts.filter(tup2 => tup2._1 == tup1._1)
              .map(tup2 => (if (stateLookup.contains(tup1._2)) stateLookup(tup1._2) else tup1._2, tup2._2))).flatten.distinct
            sc.parallelize(masterData).toDF("state", "district")
        } else
            sqlContext.emptyDataFrame
        masterDf
    }

    def getStateLookup(restUtil: HTTPClient): Map[String, String] = {
        val response = restUtil.get[StateLookup](AppConf.getConfig("druid.state.lookup.url"))

        if (null != response && null != response.lookupExtractorFactory) response.lookupExtractorFactory.map else Map.empty
    }
}
