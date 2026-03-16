package org.ekstep.analytics.util

import org.ekstep.analytics.framework.Input
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.GData
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.DtRange
import org.joda.time.DateTime
import org.ekstep.analytics.framework.CassandraTable
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.Output
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.ETags

class CaseClasses extends Serializable {}

/* Computed Event Without Optional Fields - Start */

@scala.beans.BeanInfo
class DerivedEvent(val eid: String, val ets: Long, val syncts: Long, val ver: String, val mid: String, val uid: String, val content_id: String,
                   val context: Context, val dimensions: Dimensions, val edata: MEEdata, val etags: Option[ETags] = Option(ETags(None, None, None))) extends Input with AlgoInput;

@scala.beans.BeanInfo
class Dimensions(val uid: String, val did: String, val gdata: GData, val domain: String, val loc: String, val group_user: Boolean, val anonymous_user: Boolean, val app_id: Option[String] = Option(AppConf.getConfig("default.app.id")), val client: Option[Map[String, AnyRef]] = None, val textbook_id: Option[String] = None, val channel_id: Option[String] = Option(AppConf.getConfig("default.channel.id"))) extends Serializable;

@scala.beans.BeanInfo
class Context(val pdata: PData, val dspec: Map[String, String], val granularity: String, val date_range: DtRange) extends Serializable;

@scala.beans.BeanInfo
class Eks(val id: String, val ver: String, val levels: Array[Map[String, Any]], val noOfAttempts: Int, val timeSpent: Double,
          val interruptTime: Double, val timeDiff: Double, val start_time: Long, val end_time: Long, val currentLevel: Map[String, String],
          val noOfLevelTransitions: Int, val interactEventsPerMin: Double, val completionStatus: Boolean, val screenSummary: Array[AnyRef],
          val noOfInteractEvents: Int, val eventsSummary: Array[AnyRef], val syncDate: Long, val contentType: AnyRef, val mimeType: AnyRef,
          val did: String, val tags: AnyRef, val telemetryVer: String, val itemResponses: Array[AnyRef])

@scala.beans.BeanInfo
class MEEdata(val eks: Eks) extends Serializable;
/* Computed Event Without Optional Fields - End */

/* Cassandra Models */
case class WorkFlowSummaryIndex(d_period: Int, d_channel: String, d_app_id: String, d_tag: String, d_type: String, d_mode: String, d_device_id: String, d_content_id: String, d_user_id: String) extends Output
case class WorkFlowUsageSummaryFact(d_period: Int, d_channel: String, d_app_id: String, d_tag: String, d_type: String, d_mode: String, d_device_id: String, d_content_id: String, d_user_id: String, m_publish_date: DateTime, m_last_sync_date: DateTime, m_last_gen_date: DateTime, m_total_ts: Double, m_total_sessions: Long, m_avg_ts_session: Double, m_total_interactions: Long, m_avg_interactions_min: Double, m_total_pageviews_count: Long, m_avg_pageviews: Double, m_total_users_count: Long, m_total_content_count: Long, m_total_devices_count: Long, m_unique_users: Array[Byte], m_device_ids: Array[Byte], m_contents: Array[Byte], m_content_type: Option[String], m_updated_date: Option[DateTime] = Option(DateTime.now())) extends AlgoOutput with CassandraTable

case class RequestFilter(start_date: String, end_date: String, tags: Option[List[String]], events: Option[List[String]], app_id: Option[String], channel: Option[String]);
case class RequestConfig(filter: RequestFilter, dataset_id: Option[String] = Option("eks-consumption-raw"), output_format: Option[String] = None);
case class RequestOutput(request_id: String, output_events: Int)
case class DataExhaustJobInput(eventDate: Long, event: String, eid: String) extends AlgoInput;
case class JobResponse(client_key: String, request_id: String, job_id: String, output_events: Long, bucket: String, prefix: String, first_event_date: Long, last_event_date: Long);

case class RequestDetails(client_key: String, request_id: String, status: String, dt_job_submitted: String, input_events: Option[Long], output_events: Option[Long], execution_time: Option[Long])
