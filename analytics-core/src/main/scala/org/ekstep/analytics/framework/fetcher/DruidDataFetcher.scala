package org.ekstep.analytics.framework.fetcher

import java.time.format.DateTimeFormatter

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import ing.wbaa.druid._
import ing.wbaa.druid.definitions._
import ing.wbaa.druid.dql.DSL._
import ing.wbaa.druid.dql.Dim
import ing.wbaa.druid.dql.expressions.{AggregationExpression, FilteringExpression, PostAggregationExpression}
import io.circe.Json
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.exception.DataFetcherException
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils}

import scala.concurrent.{Await, Future}

object DruidDataFetcher {

  @throws(classOf[DataFetcherException])
  def getDruidData(query: DruidQueryModel, queryAsStream: Boolean = false)(implicit fc: FrameworkContext): List[String] = {
    val request = getDruidQuery(query)
    if(queryAsStream) {
      val result = executeQueryAsStream(request)
      processResult(query, result.toList)
    } else {
      val result = executeDruidQuery(request)
      processResult(query, result.results)
    }
  }

  def getDruidQuery(query: DruidQueryModel): DruidQuery = {
    val dims = query.dimensions.getOrElse(List())
    query.queryType.toLowerCase() match {
      case "groupby" => {
        val DQLQuery = DQL
          .from(query.dataSource)
          .granularity(CommonUtil.getGranularity(query.granularity.getOrElse("all")))
          .interval(CommonUtil.getIntervalRange(query.intervals, query.dataSource))
          .agg(getAggregation(query.aggregations): _*)
          .groupBy(dims.map(f => getDimensionByType(f.`type`, f.fieldName, f.aliasName, f.outputType, f.extractionFn)): _*)
        if (query.filters.nonEmpty) DQLQuery.where(getFilter(query.filters).get)
        if (query.postAggregation.nonEmpty) DQLQuery.postAgg(getPostAggregation(query.postAggregation).get: _*)
        if (query.having.nonEmpty) DQLQuery.having(getGroupByHaving(query.having).get)
        DQLQuery.build()
      }
      case "topn" => {
        val DQLQuery = DQL
          .from(query.dataSource)
          .granularity(CommonUtil.getGranularity(query.granularity.getOrElse("all")))
          .interval(CommonUtil.getIntervalRange(query.intervals, query.dataSource))
          .topN(getDimensionByType(dims.head.`type`, dims.head.fieldName, dims.head.aliasName, dims.head.outputType, dims.head.extractionFn), query.metric.getOrElse("count"), query.threshold.getOrElse(100).asInstanceOf[Int])
          .agg(getAggregation(query.aggregations): _*)
        if (query.filters.nonEmpty) DQLQuery.where(getFilter(query.filters).get)
        if (query.postAggregation.nonEmpty) DQLQuery.postAgg(getPostAggregation(query.postAggregation).get: _*)
        DQLQuery.build()
      }
      case "timeseries" => {
        val DQLQuery = DQL
          .from(query.dataSource)
          .granularity(CommonUtil.getGranularity(query.granularity.getOrElse("all")))
          .interval(CommonUtil.getIntervalRange(query.intervals, query.dataSource))
          .agg(getAggregation(query.aggregations): _*)
        if (query.filters.nonEmpty) DQLQuery.where(getFilter(query.filters).get)
        if (query.postAggregation.nonEmpty) DQLQuery.postAgg(getPostAggregation(query.postAggregation).get: _*)
        DQLQuery.build()
      }
      case _ =>
        throw new DataFetcherException("Unknown druid query type found");
    }
  }

  def executeDruidQuery(query: DruidQuery)(implicit fc: FrameworkContext): DruidResponse = {
    val response = if(query.dataSource.contains("rollup") || query.dataSource.contains("distinct")) fc.getDruidRollUpClient().doQuery(query)
                else fc.getDruidClient().doQuery(query)
    val queryWaitTimeInMins = AppConf.getConfig("druid.query.wait.time.mins").toLong
    Await.result(response, scala.concurrent.duration.Duration.apply(queryWaitTimeInMins, "minute"))
  }

  def executeQueryAsStream(query: DruidQuery)(implicit fc: FrameworkContext): Seq[DruidResult] = {
    implicit val system = ActorSystem("ExecuteQuery")
    implicit val materializer = ActorMaterializer()

    val response = if(query.dataSource.contains("rollup") || query.dataSource.contains("distinct")) fc.getDruidRollUpClient().doQueryAsStream(query)
    else fc.getDruidClient().doQueryAsStream(query)

    val druidResult: Future[Seq[DruidResult]] = response
      .runWith(Sink.seq[DruidResult])

    val queryWaitTimeInMins = AppConf.getConfig("druid.query.wait.time.mins").toLong
    Await.result(druidResult, scala.concurrent.duration.Duration.apply(queryWaitTimeInMins, "minute"))
  }

  def processResult(query: DruidQueryModel, result: List[DruidResult]): List[String] = {
    if (result.nonEmpty) {
      query.queryType.toLowerCase match {
        case "timeseries" | "groupby" =>
          val series = result.map { f =>
            f.result.asObject.get.+:("date", Json.fromString(f.timestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))).toMap.map { f =>
              if (f._2.isNull)
                (f._1 -> "unknown")
              else if ("String".equalsIgnoreCase(f._2.name))
                (f._1 -> f._2.asString.get)
              else if ("Number".equalsIgnoreCase(f._2.name)) {
                (f._1 -> CommonUtil.roundDouble(f._2.asNumber.get.toDouble, 2))
              } else (f._1 -> f._2)
            }
          }
          series.map(f => JSONUtils.serialize(f))
        case "topn" =>
          val timeMap = Map("date" -> result.head.timestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))
          val series = result.map(f => f).head.result.asArray.get.map { f =>
            val dataMap = f.asObject.get.toMap.map { f =>
              if (f._2.isNull)
                (f._1 -> "unknown")
              else if ("String".equalsIgnoreCase(f._2.name))
                (f._1 -> f._2.asString.get)
              else if ("Number".equalsIgnoreCase(f._2.name))
                (f._1 -> f._2.asNumber.get.toBigDecimal.get)
              else (f._1 -> f._2)
            }
            timeMap ++ dataMap
          }.toList
          series.map(f => JSONUtils.serialize(f))
      }
    } else
      List();
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
      case AggregationType.Javascript  => ing.wbaa.druid.dql.AggregationOps.javascript(name.getOrElse(""), Iterable(fieldName), fnAggregate.get, fnCombine.get, fnReset.get)
      case AggregationType.HLLSketchMerge => ing.wbaa.druid.dql.AggregationOps.hllAggregator(fieldName, name.getOrElse(s"${AggregationType.HLLSketchMerge.toString.toLowerCase()}_${fieldName.toLowerCase()}"), lgk.getOrElse(12), tgtHllType.getOrElse("HLL_4"), round.getOrElse(true))
      case AggregationType.Filtered    => getFilteredAggregationByType(filterAggType, name, fieldName, filterFieldName, filterValue)
//      case _                           => throw new Exception("Unsupported aggregation type")
    }
  }

  def getFilteredAggregationByType(aggType: Option[String], name: Option[String], fieldName: String, filterFieldName: Option[String], filterValue: Option[AnyRef]): AggregationExpression = {
    if (aggType.nonEmpty || filterFieldName.nonEmpty || filterValue.nonEmpty)
      ing.wbaa.druid.dql.AggregationOps.selectorFiltered(filterFieldName.get, getAggregationByType(AggregationType.decode(aggType.get).right.get, name, fieldName), filterValue.get.toString)
    else
      throw new DataFetcherException("Missing fields for filter type aggregation");
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