package org.ekstep.analytics.framework.fetcher

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.time.format.DateTimeFormatter
import java.util.Properties

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Framing, Keep, Sink, Source}
import akka.util.ByteString
import com.ing.wbaa.druid._
import com.ing.wbaa.druid.definitions._
import com.ing.wbaa.druid.dql.DSL._
import com.ing.wbaa.druid.dql.Dim
import com.ing.wbaa.druid.dql.expressions.{AggregationExpression, FilteringExpression, PostAggregationExpression}
import io.circe.Json
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.exception.DataFetcherException
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, ResultAccumulator}
import org.sunbird.cloud.storage.conf.AppConf

import scala.concurrent.{Await, ExecutionContextExecutor, Future}


trait AkkaHttpClient {
  def sendRequest(httpRequest: HttpRequest)(implicit actorSystem: ActorSystem): Future[HttpResponse]
}

object AkkaHttpUtil extends AkkaHttpClient {
  def sendRequest(httpRequest: HttpRequest)(implicit actorSystem: ActorSystem): Future[HttpResponse] ={
    Http().singleRequest(httpRequest)
  }
}

object DruidDataFetcher {
  @throws(classOf[DataFetcherException])
  def getDruidData(query: DruidQueryModel, queryAsStream: Boolean = false)(implicit sc: SparkContext, fc: FrameworkContext): RDD[String] = {
    val request = getDruidQuery(query)
    fc.inputEventsCount = sc.longAccumulator("DruidDataCount")
    if (queryAsStream) {
      query.queryType.toLowerCase() match {
        case "topn" =>
          executeQueryAsStream(query, request).flatMap(f => processResult(query, f).asInstanceOf[List[String]])
        case _  =>
          executeQueryAsStream(query, request).map(f => processResult(query, f).toString)
      }
    } else {
      val response = executeDruidQuery(query, request)
      query.queryType.toLowerCase() match {
        case "timeseries" | "groupby" =>
          val df = sc.parallelize(response.asInstanceOf[DruidResponseTimeseriesImpl].results)
            df.map(result => processResult(query,result).toString)
         case "topn" =>
           val df = sc.parallelize(response.asInstanceOf[DruidResponseTimeseriesImpl].results)
           df.flatMap(result => processResult(query,result).asInstanceOf[List[String]])
        case "scan" =>
          sc.parallelize(response.asInstanceOf[DruidScanResponse].results.flatMap(f => f.events)).map(result => processResult(query,result).toString)
      }
    }

  }

  def getDruidQuery(query: DruidQueryModel): DruidNativeQuery = {
    val dims = query.dimensions.getOrElse(List())
    val druidQuery = DQL
      .from(query.dataSource)
      .granularity(CommonUtil.getGranularity(query.granularity.getOrElse("all")))
      .interval(getIntervals(query))
    query.queryType.toLowerCase() match {
      case "groupby" =>
        val DQLQuery = druidQuery.agg(getAggregation(query.aggregations): _*)
          .groupBy(dims.map(f => getDimensionByType(f.`type`, f.fieldName, f.aliasName, f.outputType, f.extractionFn)): _*)
        if (query.filters.nonEmpty) DQLQuery.where(getFilter(query.filters).get)
        if (query.postAggregation.nonEmpty) DQLQuery.postAgg(getPostAggregation(query.postAggregation).get: _*)
        if (query.having.nonEmpty) DQLQuery.having(getGroupByHaving(query.having).get)
        DQLQuery.build()

      case "topn" =>
        val DQLQuery = druidQuery.topN(getDimensionByType(dims.head.`type`, dims.head.fieldName,
          dims.head.aliasName, dims.head.outputType, dims.head.extractionFn),
          query.metric.getOrElse("count"), query.threshold.getOrElse(100).asInstanceOf[Int])
          .agg(getAggregation(query.aggregations): _*)
        if (query.filters.nonEmpty) DQLQuery.where(getFilter(query.filters).get)
        if (query.postAggregation.nonEmpty) DQLQuery.postAgg(getPostAggregation(query.postAggregation).get: _*)
        DQLQuery.build()

      case "timeseries" =>
        val DQLQuery = druidQuery.agg(getAggregation(query.aggregations): _*)
        if (query.filters.nonEmpty) DQLQuery.where(getFilter(query.filters).get)
        if (query.postAggregation.nonEmpty) DQLQuery.postAgg(getPostAggregation(query.postAggregation).get: _*)
        DQLQuery.build()

      case "scan" =>
        val DQLQuery = druidQuery.scan()
        if (query.filters.nonEmpty) DQLQuery.where(getFilter(query.filters).get)
        if (query.columns.nonEmpty) DQLQuery.columns(query.columns.get)
        DQLQuery.batchSize(AppConf.getConfig("druid.scan.batch.size").toInt)
//        DQLQuery.setQueryContextParam("maxQueuedBytes", AppConf.getConfig("druid.scan.batch.bytes"))
        DQLQuery.build()

      case _ =>
        throw new DataFetcherException("Unknown druid query type found");
    }
  }

  def getIntervals(query: DruidQueryModel): String = {
    if (query.granularity.getOrElse("all").toUpperCase == "LATEST_INDEX") {
      var connection: Connection = null
      var statement: Statement = null
      try {
        val connProperties: Properties = CommonUtil.getPostgresConnectionUserProps(AppConf.getConfig("postgres.druid.user")
          , AppConf.getConfig("postgres.druid.pass"))
        val db: String = AppConf.getConfig("postgres.druid.db")
        val url: String = AppConf.getConfig("postgres.druid.url") + s"$db"
        val getLatestIndexQuery = s"""select segment.start, segment.end from druid_segments segment where datasource = '${query.dataSource}'  and used='t' order by start desc"""
        connection = DriverManager.getConnection(url, connProperties)
        statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        val result: ResultSet = statement.executeQuery(getLatestIndexQuery)
        if (result.first())
          result.getString("start") + "/" + result.getString("end")
        else
          CommonUtil.getIntervalRange(query.intervals, query.dataSource, query.intervalSlider)
      } finally {
        statement.close()
        connection.close()
      }
    } else {
      CommonUtil.getIntervalRange(query.intervals, query.dataSource, query.intervalSlider)
    }
  }

  def executeDruidQuery(model: DruidQueryModel, query: DruidNativeQuery)(implicit sc: SparkContext, fc: FrameworkContext): DruidResponse = {
    val response = if (query.dataSource.contains("rollup") || query.dataSource.contains("distinct")
      || query.dataSource.contains("snapshot")) fc.getDruidRollUpClient().doQuery(query)
    else fc.getDruidClient().doQuery(query)
    val queryWaitTimeInMins = AppConf.getConfig("druid.query.wait.time.mins").toLong
    Await.result(response, scala.concurrent.duration.Duration.apply(queryWaitTimeInMins, "minute"))


  }

  def getSQLDruidQuery(model: DruidQueryModel): DruidSQLQuery = {
    val intervals = CommonUtil.getIntervalRange(model.intervals, model.dataSource, model.intervalSlider)
    val from = intervals.split("/").apply(0).split("T").apply(0)
    val to = intervals.split("/").apply(1).split("T").apply(0)
    if(model.sqlQueryStr.nonEmpty) {
      val queryStr = model.sqlQueryStr.get.format(from, to)
      DruidSQLQuery(queryStr)
    } else {
      val columns = model.sqlDimensions.get.map({ f =>
        if (f.function.isEmpty)
          f.fieldName
        else
          f.function.get + "AS \"" + f.fieldName + "\""

      })
      val sqlString = "SELECT " + columns.mkString(",") +
        " from \"druid\".\"" + model.dataSource + "\" where " +
        "__time >= '" + from + "' AND  __time < '" + to + "'"

      DruidSQLQuery(sqlString)
    }
  }

  def executeQueryAsStream(model: DruidQueryModel, query: DruidNativeQuery)(implicit sc: SparkContext, fc: FrameworkContext): RDD[BaseResult] = {

    implicit val system = if (query.dataSource.contains("rollup") || query.dataSource.contains("distinct") || query.dataSource.contains("snapshot"))
      fc.getDruidRollUpClient().actorSystem
    else
      fc.getDruidClient().actorSystem
    implicit val materializer = ActorMaterializer()

    val response =
      if (query.dataSource.contains("rollup") || query.dataSource.contains("distinct") || query.dataSource.contains("snapshot"))
        fc.getDruidRollUpClient().doQueryAsStream(query)
      else
        fc.getDruidClient().doQueryAsStream(query)

    val druidResult: Future[RDD[BaseResult]] = {
      response
        .via(new ResultAccumulator[BaseResult])
        .map(f => {
          fc.inputEventsCount.add(f.asInstanceOf[List[BaseResult]].length)
          sc.parallelize(f.asInstanceOf[List[BaseResult]])
        })
        .toMat(Sink.fold[RDD[BaseResult], RDD[BaseResult]](sc.emptyRDD[BaseResult])(_ union _))(Keep.right).run()
    }
    val queryWaitTimeInMins = AppConf.getConfig("druid.query.wait.time.mins").toLong
    Await.result(druidResult, scala.concurrent.duration.Duration.apply(queryWaitTimeInMins, "minute"))
  }

  def getNumericColumnValue(value: Double): AnyRef = {
    // Coverting BigDecimal to String if it has more than 8 digits (length of 10 as roundToBigDecimal will add . & 0)
    // since it is converted to scientific format while writing the data frame
    val numValue = CommonUtil.roundToBigDecimal(value, 1)
    if (numValue.toString().length > 10) numValue.toString() else numValue
  }

  def processResult(query: DruidQueryModel, result: BaseResult): AnyRef = {
    query.queryType.toLowerCase match {
      case "timeseries" | "groupby" =>
        JSONUtils.serialize(result.asInstanceOf[DruidResult].result.asObject.get.+:("date",
          Json.fromString(result.timestamp.get.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))).toMap.map { f =>
          if (f._2.isNull)
            f._1 -> "unknown"
          else if ("String".equalsIgnoreCase(f._2.name))
            f._1 -> f._2.asString.get
          else if ("Number".equalsIgnoreCase(f._2.name)) {
            f._1 -> getNumericColumnValue(f._2.asNumber.get.toDouble)
          } else f._1 -> f._2
        })
      case "topn" =>
        val timeMap = Map("date" -> result.timestamp.get.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))
        val test = result.asInstanceOf[DruidResult].result.asArray.get.map(data => {
          JSONUtils.serialize(timeMap ++ data.asObject.get.toMap.map({ f =>
            if (f._2.isNull)
              f._1 -> "unknown"
            else if ("String".equalsIgnoreCase(f._2.name))
              f._1 -> f._2.asString.get
            else if ("Number".equalsIgnoreCase(f._2.name)) {
              f._1 -> getNumericColumnValue(f._2.asNumber.get.toDouble)
            }
            else f._1 -> f._2
          }))
        })
         test.toList
      case "scan" =>
        JSONUtils.serialize(result.asInstanceOf[DruidScanResult].result.asObject.get.+:("date", Json.fromString(result.
          timestamp.get.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))).toMap.map { f =>
          if (f._2.isNull)
            f._1 -> "unknown"
          else if ("String".equalsIgnoreCase(f._2.name))
            f._1 -> f._2.asString.get
          else if ("Number".equalsIgnoreCase(f._2.name)) {
            f._1 -> getNumericColumnValue(f._2.asNumber.get.toDouble)
          } else {
            f._1 -> JSONUtils.deserialize[Map[String, Any]](JSONUtils.serialize(f._2)).get("value").get
          }
        })
    }
    }


  def executeSQLQuery(model: DruidQueryModel, client: AkkaHttpClient)(implicit sc: SparkContext, fc: FrameworkContext): RDD[DruidOutput] = {

    val druidQuery = getSQLDruidQuery(model)
    fc.inputEventsCount = sc.longAccumulator("DruidDataCount")
    implicit val system = fc.getDruidRollUpClient().actorSystem
    implicit val materializer = ActorMaterializer()
    implicit val ec: ExecutionContextExecutor = system.dispatcher
    val url = String.format("%s://%s:%s%s%s", "http", AppConf.getConfig("druid.rollup.host"),
      AppConf.getConfig("druid.rollup.port"), AppConf.getConfig("druid.url"), "sql")
    val request = HttpRequest(method = HttpMethods.POST,
      uri = url,
      entity = HttpEntity(ContentTypes.`application/json`, JSONUtils.serialize(druidQuery)))
    val responseFuture: Future[HttpResponse] = client.sendRequest(request)

    val convertStringFlow =
      Flow[ByteString].map(s => s.utf8String.trim)

    val result = Source.fromFuture[HttpResponse](responseFuture)
      .flatMapConcat(response => response.entity.withoutSizeLimit()
        .dataBytes.via(Framing.delimiter(ByteString("\n"),
        AppConf.getConfig("druid.scan.batch.bytes").toInt, true)))
      .via(convertStringFlow).via(new ResultAccumulator[String])
      .map(events => {
        fc.inputEventsCount.add(events.filter(p=> p.nonEmpty).length)
        sc.parallelize(events)
      })
      .toMat(Sink.fold[RDD[String], RDD[String]](sc.emptyRDD[String])(_ union _))(Keep.right).run()

    val data = Await.result(result, scala.concurrent.duration.Duration.
      apply(AppConf.getConfig("druid.query.wait.time.mins").toLong, "minute"))
    data.filter(f => f.nonEmpty).map(f=> processSqlResult(f))
  }
  
  def processSqlResult(result: String): DruidOutput = {

    val finalResult = JSONUtils.deserialize[Map[String,Any]](result)
    val finalMap  =finalResult.map(m => {
      if(m._2== null)
        (m._1, "unknown")
      else if (m._2.isInstanceOf[String])
        (m._1, if(m._2.toString.isEmpty) "unknown" else m._2)
      else (m._1,m._2)})
    DruidOutput(finalMap)
  }

  def getAggregation(aggregations: Option[List[org.ekstep.analytics.framework.Aggregation]]): List[AggregationExpression] = {
    aggregations.getOrElse(List(org.ekstep.analytics.framework.Aggregation(None, "count", "count"))).map { f =>
      val aggType = AggregationType.decode(f.`type`).right.getOrElse(AggregationType.Count)
      getAggregationByType(aggType, f.name, f.fieldName, f.fnAggregate, f.fnCombine, f.fnReset, f.lgK, f.tgtHllType, f.round, f.filterAggType, f.filterFieldName, f.filterValue)
    }
  }

  def getAggregationByType(aggType: AggregationType, name: Option[String], fieldName: String, fnAggregate: Option[String] = None, fnCombine: Option[String] = None, fnReset: Option[String] = None, lgk: Option[Int] = None, tgtHllType: Option[String] = None, round: Option[Boolean] = None, filterAggType: Option[String] = None, filterFieldName: Option[String] = None, filterValue: Option[AnyRef] = None): AggregationExpression = {
    aggType match {
      case AggregationType.Count       => count as name.getOrElse(s"${AggregationType.Count.toString.toLowerCase()}_${fieldName.toLowerCase()}")
      case AggregationType.HyperUnique => dim(fieldName).hyperUnique as name.getOrElse(s"${AggregationType.HyperUnique.toString.toLowerCase()}_${fieldName.toLowerCase()}")
      case AggregationType.ThetaSketch => thetaSketch(Dim(fieldName)) as name.getOrElse(s"${AggregationType.ThetaSketch.toString.toLowerCase()}_${fieldName.toLowerCase()}")
      case AggregationType.Cardinality => cardinality(Dim(fieldName)) as name.getOrElse(s"${AggregationType.Cardinality.toString.toLowerCase}_${fieldName.toLowerCase()}")
      case AggregationType.LongSum     => longSum(Dim(fieldName)) as name.getOrElse(s"${AggregationType.LongSum.toString.toLowerCase()}_${fieldName.toLowerCase()}")
      case AggregationType.DoubleSum   => doubleSum(Dim(fieldName)) as name.getOrElse(s"${AggregationType.DoubleSum.toString.toLowerCase()}_${fieldName.toLowerCase()}")
      case AggregationType.DoubleMax   => doubleMax(Dim(fieldName)) as name.getOrElse(s"${AggregationType.DoubleMax.toString.toLowerCase()}_${fieldName.toLowerCase()}")
      case AggregationType.DoubleMin   => doubleMin(Dim(fieldName)) as name.getOrElse(s"${AggregationType.DoubleMin.toString.toLowerCase()}_${fieldName.toLowerCase()}")
      case AggregationType.LongMax     => longMax(Dim(fieldName)) as name.getOrElse(s"${AggregationType.LongMax.toString.toLowerCase()}_${fieldName.toLowerCase()}")
      case AggregationType.LongMin     => longMin(Dim(fieldName)) as name.getOrElse(s"${AggregationType.LongMin.toString.toLowerCase()}_${fieldName.toLowerCase()}")
      case AggregationType.DoubleFirst => doubleFirst(Dim(fieldName)) as name.getOrElse(s"${AggregationType.DoubleFirst.toString.toLowerCase()}_${fieldName.toLowerCase()}")
      case AggregationType.DoubleLast  => doubleLast(Dim(fieldName)) as name.getOrElse(s"${AggregationType.DoubleLast.toString.toLowerCase()}_${fieldName.toLowerCase()}")
      case AggregationType.LongLast    => longLast(Dim(fieldName)) as name.getOrElse(s"${AggregationType.LongLast.toString.toLowerCase()}_${fieldName.toLowerCase()}")
      case AggregationType.LongFirst   => longFirst(Dim(fieldName)) as name.getOrElse(s"${AggregationType.LongFirst.toString.toLowerCase()}_${fieldName.toLowerCase()}")
      case AggregationType.Javascript  => com.ing.wbaa.druid.dql.AggregationOps.javascript(name.getOrElse(""), Iterable(fieldName), fnAggregate.get, fnCombine.get, fnReset.get)
      case AggregationType.HLLSketchMerge => com.ing.wbaa.druid.dql.AggregationOps.hllAggregator(fieldName, name.getOrElse(s"${AggregationType.HLLSketchMerge.toString.toLowerCase()}_${fieldName.toLowerCase()}"), lgk.getOrElse(12), tgtHllType.getOrElse("HLL_4"), round.getOrElse(true))
      case AggregationType.Filtered    => getFilteredAggregationByType(filterAggType, name, fieldName, filterFieldName, filterValue)
      //      case _                           => throw new Exception("Unsupported aggregation type")
    }
  }

  def getFilteredAggregationByType(aggType: Option[String], name: Option[String], fieldName: String, filterFieldName: Option[String], filterValue: Option[AnyRef]): AggregationExpression = {
    if (aggType.nonEmpty || filterFieldName.nonEmpty || filterValue.nonEmpty)
      com.ing.wbaa.druid.dql.AggregationOps.selectorFiltered(filterFieldName.get, getAggregationByType(AggregationType.decode(aggType.get).right.get, name, fieldName), filterValue.get.toString)
    else
      throw new DataFetcherException("Missing fields for filter type aggregation")
  }

  def getFilter(filters: Option[List[DruidFilter]]): Option[FilteringExpression] = {

    if (filters.nonEmpty) {
      val filterExprs = filters.get.map { f =>
        val values = if (f.values.isEmpty && f.value.isEmpty) List() else if (f.values.isEmpty) List(f.value.get) else f.values.get
        getFilterByType(f.`type`, f.dimension, values)
      }
      Option(conjunction(filterExprs: _*))
    } else None

  }

  def getFilterByType(filterType: String, dimension: String, values: List[AnyRef]): FilteringExpression = {
    filterType.toLowerCase match {
      case "isnull"             => Dim(dimension).isNull
      case "isnotnull"          => Dim(dimension).isNotNull
      case "equals"             => Dim(dimension) === values.head.asInstanceOf[String]
      case "notequals"          => Dim(dimension) =!= values.head.asInstanceOf[String]
      case "containsignorecase" => Dim(dimension).containsIgnoreCase(values.head.asInstanceOf[String])
      case "contains"           => Dim(dimension).contains(values.head.asInstanceOf[String], true)
      case "in"                 => Dim(dimension) in values.asInstanceOf[List[String]]
      case "notin"              => Dim(dimension) notIn values.asInstanceOf[List[String]]
      case "regex"              => Dim(dimension) regex values.head.asInstanceOf[String]
      case "like"               => Dim(dimension) like values.head.asInstanceOf[String]
      case "greaterthan"        => Dim(dimension).between(values.head.asInstanceOf[Number].doubleValue(), Integer.MAX_VALUE, true, false)
      case "lessthan"           => Dim(dimension).between(0, values.head.asInstanceOf[Number].doubleValue(), false, true)
      case _                    => throw new Exception("Unsupported filter type")
    }
  }

  def getPostAggregation(postAggregation: Option[List[org.ekstep.analytics.framework.PostAggregation]]): Option[List[PostAggregationExpression]] = {
    if (postAggregation.nonEmpty) {
      Option(postAggregation.get.map { f =>
        PostAggregationType.decode(f.`type`) match {
          case Right(x) => getPostAggregationByType(x, f.name, f.fields, f.fn)
          case Left(x)  => throw x
        }
      })
    } else None
  }

  def getPostAggregationByType(postAggType: PostAggregationType, name: String, fields: PostAggregationFields, fn: String): PostAggregationExpression = {
    postAggType match {
      case PostAggregationType.Arithmetic =>
        fn match {
          // only right field can have type as Constant or FieldAccess
          case "+" => if ("constant".equalsIgnoreCase(fields.rightFieldType)) Dim(fields.leftField).+(fields.rightField.asInstanceOf[Number].doubleValue()) as name else Dim(fields.leftField).+(Dim(fields.rightField.asInstanceOf[String])) as name
          case "-" => if ("constant".equalsIgnoreCase(fields.rightFieldType)) Dim(fields.leftField).-(fields.rightField.asInstanceOf[Number].doubleValue()) as name else Dim(fields.leftField).-(Dim(fields.rightField.asInstanceOf[String])) as name
          case "*" => if ("constant".equalsIgnoreCase(fields.rightFieldType)) Dim(fields.leftField).*(fields.rightField.asInstanceOf[Number].doubleValue()) as name else Dim(fields.leftField).*(Dim(fields.rightField.asInstanceOf[String])) as name
          case "/" => if ("constant".equalsIgnoreCase(fields.rightFieldType)) Dim(fields.leftField)./(fields.rightField.asInstanceOf[Number].doubleValue()) as name else Dim(fields.leftField)./(Dim(fields.rightField.asInstanceOf[String])) as name
        }
      case PostAggregationType.Javascript =>
        if(fields.rightField.asInstanceOf[String].isEmpty) javascript(name, Seq(Dim(fields.leftField)), fn)
        else javascript(name, Seq(Dim(fields.leftField), Dim(fields.rightField.asInstanceOf[String])), fn)
      case _                              => throw new Exception("Unsupported post aggregation type")
    }
  }

  def getGroupByHaving(having: Option[DruidHavingFilter]): Option[FilteringExpression] = {

    if (having.nonEmpty) {
      HavingType.decode(having.get.`type`) match {
        case Right(x) => Option(getGroupByHavingByType(x, having.get.aggregation, having.get.value))
        case Left(x)  => throw x
      }
    } else None
  }

  def getGroupByHavingByType(postAggType: HavingType, field: String, value: AnyRef): FilteringExpression = {
    postAggType match {
      case HavingType.EqualTo     => Dim(field) === value.asInstanceOf[String]
      case HavingType.Not         => Dim(field) =!= value.asInstanceOf[String]
      case HavingType.GreaterThan => Dim(field) > value.asInstanceOf[Number].doubleValue()
      case HavingType.LessThan    => Dim(field) < value.asInstanceOf[Number].doubleValue()
      case _                      => throw new Exception("Unsupported group by having type")
    }
  }

  def getDimensionByType(`type`: Option[String], fieldName: String, aliasName: Option[String], outputType: Option[String] = None, extractionFn: Option[List[ExtractFn]] = None): Dim = {
    `type`.getOrElse("default").toLowerCase match {
      case "default" => Dim(fieldName, aliasName)
      case "extraction" =>  Dim(fieldName,aliasName,outputType).extract(getExtractionFn(extractionFn.get.head))
      case "cascade"    => Dim(fieldName, aliasName, outputType).extract(CascadeExtractionFn(Seq(extractionFn.get.map(f => getExtractionFn(f)): _*)))
    }
  }

  def getExtractionFn(extractionFunc: ExtractFn): ExtractionFn = {
    extractionFunc.`type`.toLowerCase match {
      case "javascript" => JavascriptExtractionFn(extractionFunc.fn).asInstanceOf[ExtractionFn]
      case "registeredlookup" => RegisteredLookupExtractionFn(extractionFunc.fn, extractionFunc.retainMissingValue, extractionFunc.replaceMissingValueWith).asInstanceOf[ExtractionFn]
    }
  }
}