package org.ekstep.analytics.util

import org.ekstep.analytics.framework.Input
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.GData
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.DtRange
import org.joda.time.DateTime
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.Output
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.ETags
import scala.beans.BeanProperty

class CaseClasses extends Serializable {}

/* Computed Event Without Optional Fields - Start */

class DerivedEvent(@BeanProperty val eid: String, @BeanProperty val ets: Long, @BeanProperty val syncts: Long, @BeanProperty val ver: String, @BeanProperty val mid: String, @BeanProperty val uid: String, @BeanProperty val content_id: String,
                   @BeanProperty val context: Context, @BeanProperty val dimensions: Dimensions, @BeanProperty val edata: MEEdata, @BeanProperty val etags: Option[ETags] = Option(ETags(None, None, None))) extends Input with AlgoInput;

class Dimensions(@BeanProperty val uid: String, @BeanProperty val did: String, @BeanProperty val gdata: GData, @BeanProperty val domain: String, @BeanProperty val loc: String, @BeanProperty val group_user: Boolean, @BeanProperty val anonymous_user: Boolean, @BeanProperty val app_id: Option[String] = Option(AppConf.getConfig("default.app.id")), @BeanProperty val client: Option[Map[String, AnyRef]] = None, @BeanProperty val textbook_id: Option[String] = None, @BeanProperty val channel_id: Option[String] = Option(AppConf.getConfig("default.channel.id"))) extends Serializable;

class Context(@BeanProperty val pdata: PData, @BeanProperty val dspec: Map[String, String], @BeanProperty val granularity: String, @BeanProperty val date_range: DtRange) extends Serializable;

class Eks(@BeanProperty val id: String, @BeanProperty val ver: String, @BeanProperty val levels: Array[Map[String, Any]], @BeanProperty val noOfAttempts: Int, @BeanProperty val timeSpent: Double,
          @BeanProperty val interruptTime: Double, @BeanProperty val timeDiff: Double, @BeanProperty val start_time: Long, @BeanProperty val end_time: Long, @BeanProperty val currentLevel: Map[String, String],
          @BeanProperty val noOfLevelTransitions: Int, @BeanProperty val interactEventsPerMin: Double, @BeanProperty val completionStatus: Boolean, @BeanProperty val screenSummary: Array[AnyRef],
          @BeanProperty val noOfInteractEvents: Int, @BeanProperty val eventsSummary: Array[AnyRef], @BeanProperty val syncDate: Long, @BeanProperty val contentType: AnyRef, @BeanProperty val mimeType: AnyRef,
          @BeanProperty val did: String, @BeanProperty val tags: AnyRef, @BeanProperty val telemetryVer: String, @BeanProperty val itemResponses: Array[AnyRef])

class MEEdata(@BeanProperty val eks: Eks) extends Serializable;
/* Computed Event Without Optional Fields - End */

/* Cassandra Models */
case class WorkFlowSummaryIndex(d_period: Int, d_channel: String, d_app_id: String, d_tag: String, d_type: String, d_mode: String, d_device_id: String, d_content_id: String, d_user_id: String) extends Output
case class WorkFlowUsageSummaryFact(d_period: Int, d_channel: String, d_app_id: String, d_tag: String, d_type: String, d_mode: String, d_device_id: String, d_content_id: String, d_user_id: String, m_publish_date: DateTime, m_last_sync_date: DateTime, m_last_gen_date: DateTime, m_total_ts: Double, m_total_sessions: Long, m_avg_ts_session: Double, m_total_interactions: Long, m_avg_interactions_min: Double, m_total_pageviews_count: Long, m_avg_pageviews: Double, m_total_users_count: Long, m_total_content_count: Long, m_total_devices_count: Long, m_unique_users: Array[Byte], m_device_ids: Array[Byte], m_contents: Array[Byte], m_content_type: Option[String], m_updated_date: Option[DateTime] = Option(DateTime.now())) extends AlgoOutput

case class RequestFilter(start_date: String, end_date: String, tags: Option[List[String]], events: Option[List[String]], app_id: Option[String], channel: Option[String]);
case class RequestConfig(filter: RequestFilter, dataset_id: Option[String] = Option("eks-consumption-raw"), output_format: Option[String] = None);
case class RequestOutput(request_id: String, output_events: Int)
case class DataExhaustJobInput(eventDate: Long, event: String, eid: String) extends AlgoInput;
case class JobResponse(client_key: String, request_id: String, job_id: String, output_events: Long, bucket: String, prefix: String, first_event_date: Long, last_event_date: Long);

case class RequestDetails(client_key: String, request_id: String, status: String, dt_job_submitted: String, input_events: Option[Long], output_events: Option[Long], execution_time: Option[Long])
