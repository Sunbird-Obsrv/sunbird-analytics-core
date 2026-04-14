package org.ekstep.analytics.framework

import java.io.Serializable
import java.sql.Timestamp
import scala.beans.BeanProperty

class Models extends Serializable {}

class GData(@BeanProperty val id: String, @BeanProperty val ver: String) extends Serializable {}

class Eks(@BeanProperty val dspec: Map[String, AnyRef], @BeanProperty val loc: String, @BeanProperty val pass: String, @BeanProperty val qid: String, @BeanProperty val score: Int, @BeanProperty val res: Array[String], @BeanProperty val length: AnyRef,
          @BeanProperty val atmpts: Int, @BeanProperty val failedatmpts: Int, @BeanProperty val category: String, @BeanProperty val current: String, @BeanProperty val max: String, @BeanProperty val `type`: String, @BeanProperty val extype: String,
          @BeanProperty val id: String, @BeanProperty val gid: String, @BeanProperty val itype: String, @BeanProperty val stageid: String, @BeanProperty val stageto: String, @BeanProperty val resvalues: Array[Map[String, AnyRef]],
          @BeanProperty val params: Array[Map[String, AnyRef]], @BeanProperty val uri: String, @BeanProperty val state: String, @BeanProperty val subtype: String, @BeanProperty val pos: Array[Map[String, AnyRef]],
          @BeanProperty val values: Array[AnyRef], @BeanProperty val tid: String, @BeanProperty val direction: String, @BeanProperty val datatype: String, @BeanProperty val count: AnyRef, @BeanProperty val contents: Array[Map[String, AnyRef]],
          @BeanProperty val comments: String, @BeanProperty val rating: Double, @BeanProperty val qtitle: String, @BeanProperty val qdesc: String, @BeanProperty val mmc: Array[String], @BeanProperty val context: Map[String, AnyRef],
          @BeanProperty val method: String, @BeanProperty val request: AnyRef) extends Serializable {}

class Ext(@BeanProperty val stageId: String, @BeanProperty val `type`: String) extends Serializable {}

class EData(@BeanProperty val eks: Eks, @BeanProperty val ext: Ext) extends Serializable {}

class EventMetadata(@BeanProperty val sync_timestamp: String, @BeanProperty val public: String) extends Serializable {}

class Event(@BeanProperty val eid: String, @BeanProperty val ts: String, @BeanProperty val ets: Long, val `@timestamp`: String, @BeanProperty val ver: String, @BeanProperty val gdata: GData, @BeanProperty val sid: String,
            @BeanProperty val uid: String, @BeanProperty val did: String, @BeanProperty val channel: Option[String], @BeanProperty val pdata: Option[PData], @BeanProperty val edata: EData, @BeanProperty val etags: Option[ETags], @BeanProperty val tags: AnyRef = null, @BeanProperty val cdata: List[CData] = List(), @BeanProperty val metadata: EventMetadata = null) extends AlgoInput with Input {}

// Computed Event Model
case class CData(@BeanProperty id: String, @BeanProperty `type`: Option[String])
case class DerivedEvent(@BeanProperty eid: String, @BeanProperty ets: Long, @BeanProperty syncts: Long, @BeanProperty ver: String, @BeanProperty mid: String, @BeanProperty uid: String, @BeanProperty channel: String, @BeanProperty content_id: Option[String] = None, @BeanProperty cdata: Option[CData], @BeanProperty context: Context, @BeanProperty dimensions: Dimensions, @BeanProperty edata: MEEdata, @BeanProperty etags: Option[ETags] = Option(ETags(None, None, None)), @BeanProperty tags: Option[List[AnyRef]] = None, @BeanProperty `object`: Option[V3Object] = None) extends Input with AlgoInput
case class MeasuredEvent(@BeanProperty eid: String, @BeanProperty ets: Long, @BeanProperty syncts: Long, @BeanProperty ver: String, @BeanProperty mid: String, @BeanProperty uid: String, @BeanProperty channel: String, @BeanProperty content_id: Option[String] = None, @BeanProperty cdata: Option[CData], @BeanProperty context: Context, @BeanProperty dimensions: Dimensions, @BeanProperty edata: MEEdata, @BeanProperty etags: Option[ETags] = None, @BeanProperty tags: Option[List[AnyRef]] = None, @BeanProperty `object`: Option[V3Object] = None) extends Output  with AlgoOutput
case class Dimensions(@BeanProperty uid: Option[String], @BeanProperty did: Option[String], @BeanProperty gdata: Option[GData], @BeanProperty cdata: Option[CData], @BeanProperty domain: Option[String], @BeanProperty user: Option[UserProfile], @BeanProperty pdata: Option[PData], @BeanProperty loc: Option[String] = None, @BeanProperty group_user: Option[Boolean] = None, @BeanProperty anonymous_user: Option[Boolean] = None, @BeanProperty tag: Option[String] = None, @BeanProperty period: Option[Int] = None, @BeanProperty content_id: Option[String] = None, @BeanProperty ss_mid: Option[String] = None, @BeanProperty item_id: Option[String] = None, @BeanProperty sid: Option[String] = None, @BeanProperty stage_id: Option[String] = None, @BeanProperty funnel: Option[String] = None, @BeanProperty dspec: Option[Map[String, AnyRef]] = None, @BeanProperty onboarding: Option[Boolean] = None, @BeanProperty genieVer: Option[String] = None, @BeanProperty author_id: Option[String] = None, @BeanProperty partner_id: Option[String] = None, @BeanProperty concept_id: Option[String] = None, @BeanProperty client: Option[Map[String, AnyRef]] = None, @BeanProperty textbook_id: Option[String] = None, @BeanProperty channel: Option[String] = None, @BeanProperty `type`: Option[String] = None, @BeanProperty mode: Option[String] = None, @BeanProperty content_type: Option[String] = None, @BeanProperty dial_code: Option[String] = None)
case class PData(@BeanProperty id: String, @BeanProperty ver: String, @BeanProperty model: Option[String] = None, @BeanProperty pid: Option[String] = None)
case class DtRange(@BeanProperty from: Long, @BeanProperty to: Long)
case class Context(@BeanProperty pdata: PData, @BeanProperty dspec: Option[Map[String, String]] = None, @BeanProperty granularity: String, @BeanProperty date_range: DtRange, @BeanProperty status: Option[String] = None, @BeanProperty client_id: Option[String] = None, @BeanProperty attempt: Option[Int] = None, @BeanProperty rollup: Option[RollUp] = None, @BeanProperty cdata: Option[List[V3CData]] = None)
case class MEEdata(@BeanProperty eks: AnyRef)
case class ETags(@BeanProperty app: Option[List[String]] = None, @BeanProperty partner: Option[List[String]] = None, @BeanProperty dims: Option[List[String]] = None)

// User profile event models

class ProfileEks(@BeanProperty val ueksid: String, @BeanProperty val utype: String, @BeanProperty val loc: String, @BeanProperty val err: String, @BeanProperty val attrs: Array[AnyRef], @BeanProperty val uid: String, @BeanProperty val age: Int, @BeanProperty val day: Int, @BeanProperty val month: Int, @BeanProperty val gender: String, @BeanProperty val language: String, @BeanProperty val standard: Int, @BeanProperty val is_group_user: Boolean, @BeanProperty val dspec: Map[String, AnyRef]) extends Serializable {}
class ProfileData(@BeanProperty val eks: ProfileEks, @BeanProperty val ext: Ext) extends Serializable {}
class ProfileEvent(@BeanProperty val eid: String, @BeanProperty val ts: String, val `@timestamp`: String, @BeanProperty val ver: String, @BeanProperty val gdata: GData, @BeanProperty val sid: String, @BeanProperty val uid: String, @BeanProperty val did: String, @BeanProperty val pdata: Option[PData] = None, @BeanProperty val channel: Option[String] = None, @BeanProperty val edata: ProfileData) extends Input with AlgoInput with Serializable {}

// User Model
case class UserProfile(uid: String, gender: String, age: Int)

// Analytics Framework Job Models
case class Query(bucket: Option[String] = None, prefix: Option[String] = None, startDate: Option[String] = None, endDate: Option[String] = None, delta: Option[Int] = None, brokerList: Option[String] = None, topic: Option[String] = None, windowType: Option[String] = None, windowDuration: Option[Int] = None, file: Option[String] = None, excludePrefix: Option[String] = None, datePattern: Option[String] = None, folder: Option[String] = None, creationDate: Option[String] = None, partitions: Option[List[Int]] = None)
case class Filter(name: String, operator: String, value: Option[AnyRef] = None)
case class Sort(name: String, order: Option[String])
case class Dispatcher(to: String, params: Map[String, AnyRef])
case class Fetcher(`type`: String, query: Option[Query], queries: Option[Array[Query]], druidQuery: Option[DruidQueryModel] = None)
case class JobConfig(search: Fetcher, filters: Option[Array[Filter]], sort: Option[Sort], model: String, modelParams: Option[Map[String, AnyRef]], output: Option[Array[Dispatcher]], parallelization: Option[Int], appName: Option[String], deviceMapping: Option[Boolean] = Option(false), exhaustConfig: Option[Map[String, DataSet]] = None, name: Option[String] = None)

//Druid Query Models
case class DruidQueryModel(queryType: String, dataSource: String, intervals: String, granularity: Option[String] = Option("all"), aggregations: Option[List[Aggregation]] = Option(List(Aggregation(Option("count"), "count", "count"))), dimensions: Option[List[DruidDimension]] = None, filters: Option[List[DruidFilter]] = None, having: Option[DruidHavingFilter] = None, postAggregation: Option[List[PostAggregation]] = None, columns: Option[List[String]] = None, sqlDimensions: Option[List[DruidSQLDimension]] = None, sqlQueryStr: Option[String] = None, threshold: Option[Long] = None, metric: Option[String] = None, descending: Option[String] = Option("false"), intervalSlider: Int = 0)

case class DruidSQLQuery(query: String, resultFormat : String = "objectLines", header:Boolean =true )

case class DruidSQLDimension(fieldName: String, function: Option[String])

case class DruidDimension(fieldName: String, aliasName: Option[String], `type`: Option[String] = Option("Default"), outputType: Option[String] = None, extractionFn: Option[List[ExtractFn]] = None)
case class ExtractFn(`type`: String, fn: String, retainMissingValue: Option[Boolean] = Option(false), replaceMissingValueWith: Option[String] = None)
case class Aggregation(name: Option[String], `type`: String, fieldName: String, fnAggregate: Option[String] = None, fnCombine: Option[String] = None, fnReset: Option[String] = None, lgK: Option[Int] = Option(12), tgtHllType: Option[String] = Option("HLL_4"), round: Option[Boolean] = None, filterAggType: Option[String] = None, filterFieldName: Option[String] = None, filterValue: Option[AnyRef] = None)
case class PostAggregation(`type`: String, name: String, fields: PostAggregationFields, fn: String, ordering: Option[String] = None)
// only right field can have type as FieldAccess or Constant. Only if it Constant, need to specify "rightFieldType"
case class PostAggregationFields(leftField: String, rightField: AnyRef, rightFieldType: String = "FieldAccess")
case class DruidFilter(`type`: String, dimension: String, value: Option[AnyRef], values: Option[List[AnyRef]] = None)
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
@deprecated("CassandraTable is no longer used", "2.0")
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
case class Actor(@BeanProperty id: String, @BeanProperty `type`: String)
case class V3PData(@BeanProperty id: String, @BeanProperty ver: Option[String] = None, @BeanProperty pid: Option[String] = None, @BeanProperty model: Option[String] = None)
case class Question(@BeanProperty id: String, @BeanProperty maxscore: Int, @BeanProperty exlength: Int, @BeanProperty params: Array[Map[String, AnyRef]], @BeanProperty uri: String, @BeanProperty desc: String, @BeanProperty title: String, @BeanProperty mmc: Array[String], @BeanProperty mc: Array[String], @BeanProperty `type`: String)
case class V3CData(@BeanProperty id: String, @BeanProperty `type`: String)
case class RollUp(@BeanProperty l1: String, @BeanProperty l2: String, @BeanProperty l3: String, @BeanProperty l4: String)
case class V3Context(@BeanProperty channel: String, @BeanProperty pdata: Option[V3PData], @BeanProperty env: String, @BeanProperty sid: Option[String], @BeanProperty did: Option[String], @BeanProperty cdata: Option[List[V3CData]], @BeanProperty rollup: Option[RollUp])
case class Visit(@BeanProperty objid: String, @BeanProperty objtype: String, @BeanProperty objver: Option[String], @BeanProperty section: Option[String], @BeanProperty index: Option[Int])
case class V3Object(@BeanProperty id: String, @BeanProperty `type`: String, @BeanProperty ver: Option[String], @BeanProperty rollup: Option[RollUp], @BeanProperty subtype: Option[String] = None, @BeanProperty parent: Option[CommonObject] = None)
case class CommonObject(@BeanProperty id: String, @BeanProperty `type`: String, @BeanProperty ver: Option[String] = None)
case class ShareItems(@BeanProperty id: String, @BeanProperty `type`: String, @BeanProperty ver: String, @BeanProperty params: List[Map[String, AnyRef]], @BeanProperty origin: CommonObject, @BeanProperty to: CommonObject)

class V3EData(@BeanProperty val datatype: String, @BeanProperty val `type`: String, @BeanProperty val dspec: Map[String, AnyRef], @BeanProperty val uaspec: Map[String, String], @BeanProperty val loc: String, @BeanProperty val mode: String, @BeanProperty val duration: Long, @BeanProperty val pageid: String,
              @BeanProperty val subtype: String, @BeanProperty val uri: String, @BeanProperty val visits: List[Visit], @BeanProperty val id: String, @BeanProperty val target: Map[String, AnyRef],
              @BeanProperty val plugin: Map[String, AnyRef], @BeanProperty val extra: AnyRef, @BeanProperty val item: Question, @BeanProperty val pass: String, @BeanProperty val score: Int, @BeanProperty val resvalues: Array[Map[String, AnyRef]],
              @BeanProperty val values: AnyRef, @BeanProperty val rating: Double, @BeanProperty val comments: String, @BeanProperty val dir: String, @BeanProperty val items: List[ShareItems], @BeanProperty val props : List[String],
              @BeanProperty val state: String, @BeanProperty val prevstate: String, @BeanProperty val err: AnyRef, @BeanProperty val errtype: String, @BeanProperty val stacktrace: String, @BeanProperty val `object`: Map[String, AnyRef],
              @BeanProperty val level: String, @BeanProperty val message: String, @BeanProperty val params: List[Map[String, AnyRef]], @BeanProperty val summary: List[Map[String, AnyRef]], @BeanProperty val index: Int, val `class`: String, @BeanProperty val status: String, @BeanProperty val query: String, @BeanProperty val data: Option[AnyRef], @BeanProperty val sort: Option[AnyRef], @BeanProperty val correlationid: Option[String], @BeanProperty val topn: List[AnyRef], @BeanProperty val filters: Option[AnyRef] = None, @BeanProperty val size: Int = 0) extends Serializable {}

class V3Event(@BeanProperty val eid: String, @BeanProperty val ets: Long, val `@timestamp`: String, @BeanProperty val ver: String, @BeanProperty val mid: String, @BeanProperty val actor: Actor, @BeanProperty val context: V3Context, @BeanProperty val `object`: Option[V3Object], @BeanProperty val edata: V3EData, @BeanProperty val tags: List[AnyRef] = null, @BeanProperty val flags : V3FlagContent = null) extends AlgoInput with Input {}

case class V3DerivedEvent(@BeanProperty eid: String, @BeanProperty ets: Long, `@timestamp`: String, @BeanProperty ver: String, @BeanProperty mid: String, @BeanProperty actor: Actor, @BeanProperty context: V3Context, @BeanProperty `object`: Option[V3Object], @BeanProperty edata: AnyRef, @BeanProperty tags: List[AnyRef] = null) extends AlgoOutput with Output

case class V3MetricEdata(@BeanProperty metric: String, @BeanProperty value: AnyRef, @BeanProperty range: Option[AnyRef] = None)

case class V3FlagContent(@BeanProperty derived_location_retrieved: Boolean, @BeanProperty device_data_retrieved: Boolean,
                   @BeanProperty user_data_retrieved: Boolean, @BeanProperty dialcode_data_retrieved: Boolean,
                   @BeanProperty content_data_retrieved: Boolean, @BeanProperty collection_data_retrieved: Boolean)

// Experiment Models
case class DeviceProfileModel(@BeanProperty device_id: Option[String], @BeanProperty state: Option[String], @BeanProperty city: Option[String], @BeanProperty first_access: Option[Timestamp])

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
                               
                               
case class StorageConfig(store: String, container: String, fileName: String, accountKey: Option[String] = None, secretKey: Option[String] = None);

case class OnDemandJobRequest(request_id: String, request_data : String,download_urls :List[String], status: String)

case class MergeConfig(`type`: Option[String], id: String, frequency: String, basePath: String, rollup: Integer, rollupAge: Option[String] = None,
                       rollupCol: Option[String] = None, rollupFormat : Option[String] = None, rollupRange: Option[Integer] = None,
                       merge: MergeFiles, container: String, postContainer: Option[String] = None,
                       deltaFileAccess: Option[Boolean] = Option(true), reportFileAccess: Option[Boolean] = Option(true),
                       dateRequired:Option[Boolean] = Option(true), columnOrder:Option[List[String]] =Option(List()),
                       metricLabels :Option[List[String]] =Option(List()))

case class MergeFiles(files: List[Map[String, String]], dims: List[String])

case class DruidOutput(t: Map[String, Any]) extends Map[String,Any]  with Input with AlgoInput with AlgoOutput with Output {
  private val internalMap = t

  override def get(key: String): Option[Any] = internalMap.get(key)

  override def iterator: Iterator[(String, Any)] = internalMap.iterator

  override def updated[V1 >: Any](key: String, value: V1): Map[String, V1] =
    new DruidOutput(internalMap.updated(key, value.asInstanceOf[Any]))

  override def removed(key: String): Map[String, Any] =
    new DruidOutput(internalMap - key)
}
