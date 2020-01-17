package org.ekstep.analytics.framework

import java.io.Serializable
import java.sql.Timestamp

class Models extends Serializable {}

@scala.beans.BeanInfo
class GData(val id: String, val ver: String) extends Serializable {}

@scala.beans.BeanInfo
class Eks(val dspec: Map[String, AnyRef], val loc: String, val pass: String, val qid: String, val score: Int, val res: Array[String], val length: AnyRef,
          val atmpts: Int, val failedatmpts: Int, val category: String, val current: String, val max: String, val `type`: String, val extype: String,
          val id: String, val gid: String, val itype: String, val stageid: String, val stageto: String, val resvalues: Array[Map[String, AnyRef]],
          val params: Array[Map[String, AnyRef]], val uri: String, val state: String, val subtype: String, val pos: Array[Map[String, AnyRef]],
          val values: Array[AnyRef], val tid: String, val direction: String, val datatype: String, val count: AnyRef, val contents: Array[Map[String, AnyRef]],
          val comments: String, val rating: Double, val qtitle: String, val qdesc: String, val mmc: Array[String], val context: Map[String, AnyRef],
          val method: String, val request: AnyRef) extends Serializable {}

@scala.beans.BeanInfo
class Ext(val stageId: String, val `type`: String) extends Serializable {}

@scala.beans.BeanInfo
class EData(val eks: Eks, val ext: Ext) extends Serializable {}

@scala.beans.BeanInfo
class EventMetadata(val sync_timestamp: String, val public: String) extends Serializable {}

@scala.beans.BeanInfo
class Event(val eid: String, val ts: String, val ets: Long, val `@timestamp`: String, val ver: String, val gdata: GData, val sid: String,
            val uid: String, val did: String, val channel: Option[String], val pdata: Option[PData], val edata: EData, val etags: Option[ETags], val tags: AnyRef = null, val cdata: List[CData] = List(), val metadata: EventMetadata = null) extends AlgoInput with Input {}

// Computed Event Model
@scala.beans.BeanInfo
case class CData(id: String, `type`: Option[String])
@scala.beans.BeanInfo
case class DerivedEvent(eid: String, ets: Long, syncts: Long, ver: String, mid: String, uid: String, channel: String, content_id: Option[String] = None, cdata: Option[CData], context: Context, dimensions: Dimensions, edata: MEEdata, etags: Option[ETags] = Option(ETags(None, None, None)), tags: Option[List[AnyRef]] = None, `object`: Option[V3Object] = None) extends Input with AlgoInput
@scala.beans.BeanInfo
case class MeasuredEvent(eid: String, ets: Long, syncts: Long, ver: String, mid: String, uid: String, channel: String, content_id: Option[String] = None, cdata: Option[CData], context: Context, dimensions: Dimensions, edata: MEEdata, etags: Option[ETags] = None, tags: Option[List[AnyRef]] = None, `object`: Option[V3Object] = None) extends Output  with AlgoOutput
@scala.beans.BeanInfo
case class Dimensions(uid: Option[String], did: Option[String], gdata: Option[GData], cdata: Option[CData], domain: Option[String], user: Option[UserProfile], pdata: Option[PData], loc: Option[String] = None, group_user: Option[Boolean] = None, anonymous_user: Option[Boolean] = None, tag: Option[String] = None, period: Option[Int] = None, content_id: Option[String] = None, ss_mid: Option[String] = None, item_id: Option[String] = None, sid: Option[String] = None, stage_id: Option[String] = None, funnel: Option[String] = None, dspec: Option[Map[String, AnyRef]] = None, onboarding: Option[Boolean] = None, genieVer: Option[String] = None, author_id: Option[String] = None, partner_id: Option[String] = None, concept_id: Option[String] = None, client: Option[Map[String, AnyRef]] = None, textbook_id: Option[String] = None, channel: Option[String] = None, `type`: Option[String] = None, mode: Option[String] = None, content_type: Option[String] = None, dial_code: Option[String] = None)
@scala.beans.BeanInfo
case class PData(id: String, ver: String, model: Option[String] = None, pid: Option[String] = None)
@scala.beans.BeanInfo
case class DtRange(from: Long, to: Long)
@scala.beans.BeanInfo
case class Context(pdata: PData, dspec: Option[Map[String, String]] = None, granularity: String, date_range: DtRange, status: Option[String] = None, client_id: Option[String] = None, attempt: Option[Int] = None, rollup: Option[RollUp] = None, cdata: Option[List[V3CData]] = None)
@scala.beans.BeanInfo
case class MEEdata(eks: AnyRef)
@scala.beans.BeanInfo
case class ETags(app: Option[List[String]] = None, partner: Option[List[String]] = None, dims: Option[List[String]] = None)

// User profile event models

@scala.beans.BeanInfo
class ProfileEks(val ueksid: String, val utype: String, val loc: String, val err: String, val attrs: Array[AnyRef], val uid: String, val age: Int, val day: Int, val month: Int, val gender: String, val language: String, val standard: Int, val is_group_user: Boolean, val dspec: Map[String, AnyRef]) extends Serializable {}
@scala.beans.BeanInfo
class ProfileData(val eks: ProfileEks, val ext: Ext) extends Serializable {}
@scala.beans.BeanInfo
class ProfileEvent(val eid: String, val ts: String, val `@timestamp`: String, val ver: String, val gdata: GData, val sid: String, val uid: String, val did: String, val pdata: Option[PData] = None, val channel: Option[String] = None, val edata: ProfileData) extends Input with AlgoInput with Serializable {}

// User Model
case class UserProfile(uid: String, gender: String, age: Int)

// Analytics Framework Job Models
case class Query(bucket: Option[String] = None, prefix: Option[String] = None, startDate: Option[String] = None, endDate: Option[String] = None, delta: Option[Int] = None, brokerList: Option[String] = None, topic: Option[String] = None, windowType: Option[String] = None, windowDuration: Option[Int] = None, file: Option[String] = None, excludePrefix: Option[String] = None, datePattern: Option[String] = None, folder: Option[String] = None, creationDate: Option[String] = None)
@scala.beans.BeanInfo
case class Filter(name: String, operator: String, value: Option[AnyRef] = None)
@scala.beans.BeanInfo
case class Sort(name: String, order: Option[String])
@scala.beans.BeanInfo
case class Dispatcher(to: String, params: Map[String, AnyRef])
@scala.beans.BeanInfo
case class Fetcher(`type`: String, query: Option[Query], queries: Option[Array[Query]], druidQuery: Option[DruidQueryModel] = None)
@scala.beans.BeanInfo
case class JobConfig(search: Fetcher, filters: Option[Array[Filter]], sort: Option[Sort], model: String, modelParams: Option[Map[String, AnyRef]], output: Option[Array[Dispatcher]], parallelization: Option[Int], appName: Option[String], deviceMapping: Option[Boolean] = Option(false), exhaustConfig: Option[Map[String, DataSet]] = None, name: Option[String] = None)

//Druid Query Models
@scala.beans.BeanInfo
case class DruidQueryModel(queryType: String, dataSource: String, intervals: String, granularity: Option[String] = Option("all"), aggregations: Option[List[Aggregation]] = Option(List(Aggregation(Option("count"), "count", "count"))), dimensions: Option[List[DruidDimension]] = None, filters: Option[List[DruidFilter]] = None, having: Option[DruidHavingFilter] = None, postAggregation: Option[List[PostAggregation]] = None, threshold: Option[Long] = None, metric: Option[String] = None, descending: Option[String] = Option("false"))
@scala.beans.BeanInfo
case class DruidDimension(fieldName: String, aliasName: Option[String])
@scala.beans.BeanInfo
case class Aggregation(name: Option[String], `type`: String, fieldName: String, fnAggregate: Option[String] = None, fnCombine: Option[String] = None, fnReset: Option[String] = None)
@scala.beans.BeanInfo
case class PostAggregation(`type`: String, name: String, fields: PostAggregationFields, fn: String, ordering: Option[String] = None)
// only right field can have type as FieldAccess or Constant. Only if it Constant, need to specify "rightFieldType"
@scala.beans.BeanInfo
case class PostAggregationFields(leftField: String, rightField: AnyRef, rightFieldType: String = "FieldAccess")
@scala.beans.BeanInfo
case class DruidFilter(`type`: String, dimension: String, value: Option[AnyRef], values: Option[List[AnyRef]] = None)
@scala.beans.BeanInfo
case class DruidHavingFilter(`type`: String, aggregation: String, value: AnyRef)

// LP API Response Model
case class Params(resmsgid: Option[String], msgid: Option[String], err: Option[String], status: Option[String], errmsg: Option[String])
case class Result(content: Option[Map[String, AnyRef]], contents: Option[Array[Map[String, AnyRef]]], questionnaire: Option[Map[String, AnyRef]],
                  assessment_item: Option[Map[String, AnyRef]], assessment_items: Option[Array[Map[String, AnyRef]]], assessment_item_set: Option[Map[String, AnyRef]],
                  games: Option[Array[Map[String, AnyRef]]], concepts: Option[Array[String]], maxScore: Double, items: Option[Array[Map[String, AnyRef]]]);
case class Response(id: String, ver: String, ts: String, params: Params, responseCode: String, result: Result)

// Search Items
case class SearchFilter(property: String, operator: String, value: Option[AnyRef])
case class Metadata(filters: Array[SearchFilter])
case class Request(metadata: Metadata, resultSize: Int)
case class Search(request: Request)

// Item Models
case class MicroConcept(id: String, metadata: Map[String, AnyRef])
case class Item(id: String, metadata: Map[String, AnyRef], tags: Option[Array[String]], mc: Option[Array[String]], mmc: Option[Array[String]])
case class ItemSet(id: String, metadata: Map[String, AnyRef], items: Array[Item], tags: Option[Array[String]], count: Int)
case class Questionnaire(id: String, metadata: Map[String, AnyRef], itemSets: Array[ItemSet], items: Array[Item], tags: Option[Array[String]])
case class ItemConcept(concepts: Array[String], maxScore: Int)

// Content Models
case class Content(id: String, metadata: Map[String, AnyRef], tags: Option[Array[String]], concepts: Array[String])
case class Game(identifier: String, code: String, subject: String, objectType: String)


// Common models for all data products
case class Empty() extends Input with AlgoInput with AlgoOutput with Output
case class RegisteredTag(tag_id: String, last_updated: Long, active: Boolean)
trait CassandraTable extends AnyRef with Serializable

/* Data Exhaust*/
case class DataSet(events: List[String], eventConfig: Map[String, EventId])
case class EventId(eventType: String, searchType: String, fetchConfig: FetchConfig, csvConfig: CsvConfig, filterMapping: Map[String, Filter])
case class FetchConfig(params: Map[String, String])
case class CsvColumnMapping(to: String, hidden: Boolean, mapFunc: String)
case class CsvConfig(auto_extract_column_names: Boolean, columnMappings: Map[String, CsvColumnMapping])
case class SaveConfig(params: Map[String, String])

/* hierarchy store keyspace */
case class ContentHierarchyTable(identifier: String, hierarchy: String)

object Period extends Enumeration {
  type Period = Value
  val DAY, WEEK, MONTH, CUMULATIVE, LAST7, LAST30, LAST90 = Value
}

object Level extends Enumeration {
  type Level = Value
  val INFO, DEBUG, WARN, ERROR = Value
}

object JobStatus extends Enumeration {
  type Status = Value;
  val SUBMITTED, PROCESSING, COMPLETED, RETRY, FAILED = Value
}

object ExperimentStatus extends Enumeration {
  type ExperimentStatus = Value;
  val SUBMITTED, PROCESSING, ACTIVE, FAILED = Value
}

trait Stage extends Enumeration {
  type Stage = Value
  val contentPlayed = Value
}

trait DataExStage extends Enumeration {
  type DataExStage = Value
  val FETCHING_ALL_REQUEST, FETCHING_DATA, FETCHING_THE_REQUEST, FILTERING_DATA, SAVE_DATA_TO_S3, SAVE_DATA_TO_LOCAL, DOWNLOAD_AND_ZIP_OUTPUT_FILE, UPLOAD_ZIP, UPDATE_RESPONSE_TO_DB = Value
}

trait JobStageStatus extends Enumeration {
  type JobStageStatus = Value
  val COMPLETED, FAILED = Value
}

object OnboardStage extends Stage {
  override type Stage = Value
  val welcomeContent, addChild, firstLesson, gotoLibrary, searchLesson, loadOnboardPage = Value
}

object OtherStage extends Stage {
  override type Stage = Value
  val listContent, selectContent, downloadInitiated, downloadComplete = Value
}

// telemetry v3 case classes
@scala.beans.BeanInfo
case class Actor(id: String, `type`: String)
@scala.beans.BeanInfo
case class V3PData(id: String, ver: Option[String] = None, pid: Option[String] = None, model: Option[String] = None)
@scala.beans.BeanInfo
case class Question(id: String, maxscore: Int, exlength: Int, params: Array[Map[String, AnyRef]], uri: String, desc: String, title: String, mmc: Array[String], mc: Array[String])
@scala.beans.BeanInfo
case class V3CData(id: String, `type`: String)
@scala.beans.BeanInfo
case class RollUp(l1: String, l2: String, l3: String, l4: String)
@scala.beans.BeanInfo
case class V3Context(channel: String, pdata: Option[V3PData], env: String, sid: Option[String], did: Option[String], cdata: Option[List[V3CData]], rollup: Option[RollUp])
@scala.beans.BeanInfo
case class Visit(objid: String, objtype: String, objver: Option[String], section: Option[String], index: Option[Int])
@scala.beans.BeanInfo
case class V3Object(id: String, `type`: String, ver: Option[String], rollup: Option[RollUp], subtype: Option[String] = None, parent: Option[CommonObject] = None)
@scala.beans.BeanInfo
case class CommonObject(id: String, `type`: String, ver: Option[String] = None)
@scala.beans.BeanInfo
case class ShareItems(id: String, `type`: String, ver: String, params: List[Map[String, AnyRef]], origin: CommonObject, to: CommonObject)

@scala.beans.BeanInfo
class V3EData(val datatype: String, val `type`: String, val dspec: Map[String, AnyRef], val uaspec: Map[String, String], val loc: String, val mode: String, val duration: Long, val pageid: String,
              val subtype: String, val uri: String, val visits: List[Visit], val id: String, val target: Map[String, AnyRef],
              val plugin: Map[String, AnyRef], val extra: Map[String, AnyRef], val item: Question, val pass: String, val score: Int, val resvalues: Array[Map[String, AnyRef]], 
              val values: AnyRef, val rating: Double, val comments: String, val dir: String, val items: List[ShareItems], val props : List[String], 
              val state: String, val prevstate: String, val err: AnyRef, val errtype: String, val stacktrace: String, val `object`: Map[String, AnyRef],
              val level: String, val message: String, val params: List[Map[String, AnyRef]], val summary: List[Map[String, AnyRef]], val index: Int, val `class`: String, val status: String, val query: String, val data: Option[AnyRef], val sort: Option[AnyRef], val correlationid: Option[String], val topn: List[AnyRef], val filters: Option[AnyRef] = None, val size: Int = 0) extends Serializable {}

@scala.beans.BeanInfo
class V3Event(val eid: String, val ets: Long, val `@timestamp`: String, val ver: String, val mid: String, val actor: Actor, val context: V3Context, val `object`: Option[V3Object], val edata: V3EData, val tags: List[AnyRef] = null, val flags : V3FlagContent = null) extends AlgoInput with Input {}

@scala.beans.BeanInfo
case class V3DerivedEvent(eid: String, ets: Long, `@timestamp`: String, ver: String, mid: String, actor: Actor, context: V3Context, `object`: Option[V3Object], edata: AnyRef, tags: List[AnyRef] = null) extends AlgoOutput with Output

@scala.beans.BeanInfo
case class V3MetricEdata(metric: String, value: AnyRef, range: Option[AnyRef] = None)

@scala.beans.BeanInfo
case class V3FlagContent(derived_location_retrieved: Boolean, device_data_retrieved: Boolean,
                   user_data_retrieved: Boolean, dialcode_data_retrieved: Boolean,
                   content_data_retrieved: Boolean, collection_data_retrieved: Boolean)

// Experiment Models
@scala.beans.BeanInfo
case class DeviceProfileModel(device_id: Option[String], state: Option[String], city: Option[String], first_access: Option[Timestamp])

case class GraphUpdateEvent(ets: Long, nodeUniqueId: String, transactionData: Map[String, Map[String, Map[String, Any]]], objectType: String, operationType: String = "UPDATE", nodeType: String = "DATA_NODE", graphId: String = "domain", nodeGraphId: Int = 0) extends AlgoOutput with Output

// WFS Models
case class ItemResponse(itemId: String, itype: Option[AnyRef], ilevel: Option[AnyRef], timeSpent: Option[Double], exTimeSpent: Option[AnyRef], res: Option[Array[String]], resValues: Option[Array[AnyRef]], exRes: Option[AnyRef], incRes: Option[AnyRef], mc: Option[AnyRef], mmc: Option[AnyRef], score: Int, time_stamp: Long, maxScore: Option[AnyRef], domain: Option[AnyRef], pass: String, qtitle: Option[String], qdesc: Option[String]);
case class EventSummary(id: String, count: Int)
case class EnvSummary(env: String, time_spent: Double, count: Long)
case class PageSummary(id: String, `type`: String, env: String, time_spent: Double, visit_count: Long)

// Device Summary Model
case class DeviceProfileOutput(device_id: String, first_access: Option[Timestamp], last_access: Option[Timestamp], total_ts: Option[Double],
                               total_launches: Option[Long], avg_ts: Option[Double],
                               device_spec: Option[String], uaspec: Option[String], state: Option[String], city: Option[String],
                               country: Option[String], country_code:Option[String], state_code:Option[String],
                               state_custom: Option[String], state_code_custom: Option[String], district_custom: Option[String],
                               fcm_token: Option[String], producer_id: Option[String], user_declared_state: Option[String],
                               user_declared_district: Option[String], api_last_updated_on: Option[Timestamp], user_declared_on: Option[Timestamp],
                               updated_date: Option[Timestamp] = Option(new Timestamp(System.currentTimeMillis()))) extends AlgoOutput