package org.ekstep.analytics.framework.fetcher

import java.time.format.DateTimeFormatter

import ing.wbaa.druid._
import ing.wbaa.druid.client.DruidHttpClient
import ing.wbaa.druid.definitions._
import ing.wbaa.druid.dql.DSL._
import ing.wbaa.druid.dql.Dim
import ing.wbaa.druid.dql.expressions.{AggregationExpression, FilteringExpression, PostAggregationExpression}
import io.circe.Json
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.exception.DataFetcherException
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils}
import org.ekstep.analytics.framework.{DruidQueryModel, FrameworkContext, PostAggregationFields}

import scala.concurrent.Await

object DruidDataFetcher {
  
    @throws(classOf[DataFetcherException])
    def getDruidData(query: DruidQueryModel)(implicit fc: FrameworkContext): List[String] = {
        val request = getDruidQuery(query)
        val result = executeDruidQuery(request);
        processResult(query, result);
    }

    def getDruidQuery(query: DruidQueryModel): DruidQuery = {

        query.queryType.toLowerCase() match {
            case "groupby" => {
                val DQLQuery = DQL
                  .from(query.dataSource)
                  .granularity(CommonUtil.getGranularity(query.granularity.getOrElse("all")))
                  .interval(CommonUtil.getIntervalRange(query.intervals))
                  .agg(getAggregation(query): _*)
                  .groupBy(query.dimensions.get.map(f => Dim(f.fieldName, f.aliasName)): _*)
                if(query.filters.nonEmpty) DQLQuery.where(getFilter(query).get)
                if(query.postAggregation.nonEmpty) DQLQuery.postAgg(getPostAggregation(query).get: _*)
                if(query.having.nonEmpty) DQLQuery.having(getGroupByHaving(query).get)
                DQLQuery.build()
            }
            case "topn" => {
                val DQLQuery = DQL
                  .from(query.dataSource)
                  .granularity(CommonUtil.getGranularity(query.granularity.getOrElse("all")))
                  .interval(CommonUtil.getIntervalRange(query.intervals))
                  .topN(Dim(query.dimensions.get.head.fieldName, query.dimensions.get.head.aliasName), query.metric.getOrElse("count"), query.threshold.getOrElse(100).asInstanceOf[Int])
                  .agg(getAggregation(query): _*)
                if(query.filters.nonEmpty) DQLQuery.where(getFilter(query).get)
                if(query.postAggregation.nonEmpty) DQLQuery.postAgg(getPostAggregation(query).get: _*)
                DQLQuery.build()
            }
            case "timeseries" => {
                val DQLQuery = DQL
                  .from(query.dataSource)
                  .granularity(CommonUtil.getGranularity(query.granularity.getOrElse("all")))
                  .interval(CommonUtil.getIntervalRange(query.intervals))
                  .agg(getAggregation(query): _*)
                if(query.filters.nonEmpty) DQLQuery.where(getFilter(query).get)
                if(query.postAggregation.nonEmpty) DQLQuery.postAgg(getPostAggregation(query).get: _*)
                DQLQuery.build()
            }
            case _ =>
                throw new DataFetcherException("Unknown druid query type found");
        }
    }

    def executeDruidQuery(query: DruidQuery)(implicit fc: FrameworkContext) : DruidResponse = {
        val response = fc.getDruidClient().doQuery(query);
        val queryWaitTimeInMins = AppConf.getConfig("druid.query.wait.time.mins").toLong
        Await.result(response, scala.concurrent.duration.Duration.apply(queryWaitTimeInMins, "minute"))
    }
    
    def processResult(query: DruidQueryModel, result: DruidResponse) : List[String] = {
        if(result.results.length > 0) {
            query.queryType.toLowerCase match {
                case "timeseries" | "groupby" =>
                    val series = result.results.map { f =>
                        f.result.asObject.get.+:("date", Json.fromString(f.timestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))).toMap.map { f =>
                            if(f._2.isNull)
                                (f._1 -> "unknown")
                            else if ("String".equalsIgnoreCase(f._2.name))
                                (f._1 -> f._2.asString.get)
                            else if("Number".equalsIgnoreCase(f._2.name))
                              {
                                (f._1 -> CommonUtil.roundDouble(f._2.asNumber.get.toDouble, 2))
                              }

                            else (f._1 -> f._2)
                        }
                    }
                    series.map(f => JSONUtils.serialize(f))
                case "topn" =>
                    val timeMap = Map("date" -> result.results.head.timestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))
                    val series = result.results.map(f => f).head.result.asArray.get.map{f =>
                        val dataMap = f.asObject.get.toMap.map{f =>
                            if(f._2.isNull)
                                (f._1 -> "unknown")
                            else if ("String".equalsIgnoreCase(f._2.name))
                                (f._1 -> f._2.asString.get)
                            else if("Number".equalsIgnoreCase(f._2.name))
                                (f._1 -> f._2.asNumber.get.toBigDecimal.get)
                            else (f._1 -> f._2)
                        }
                        timeMap ++ dataMap
                    }.toList
                    series.map(f => JSONUtils.serialize(f))
            }
        }
        else
            List();
    }
    
    def getAggregation(query: DruidQueryModel): List[AggregationExpression] = {
        query.aggregations.getOrElse(List(org.ekstep.analytics.framework.Aggregation(None, "count", "count"))).map{f =>
            val aggType = AggregationType.decode(f.`type`).right.getOrElse(AggregationType.Count)
            getAggregationByType(aggType, f.name, f.fieldName, f.fnAggregate, f.fnCombine, f.fnReset)
        }
    }

    def getAggregationByType(aggType: AggregationType, name: Option[String], fieldName: String, fnAggregate: Option[String], fnCombine: Option[String], fnReset: Option[String]): AggregationExpression = {
        aggType match {
            case AggregationType.Count => count as name.getOrElse(s"${AggregationType.Count.toString.toLowerCase()}_${fieldName.toLowerCase()}")
            case AggregationType.HyperUnique => dim(fieldName).hyperUnique as name.getOrElse(s"${AggregationType.HyperUnique.toString.toLowerCase()}_${fieldName.toLowerCase()}")
            case AggregationType.ThetaSketch => thetaSketch(Dim(fieldName)) as name.getOrElse(s"${AggregationType.ThetaSketch.toString.toLowerCase()}_${fieldName.toLowerCase()}")
            case AggregationType.Cardinality => cardinality(Dim(fieldName)) as name.getOrElse(s"${AggregationType.Cardinality.toString.toLowerCase}_${fieldName.toLowerCase()}")
            case AggregationType.LongSum => longSum(Dim(fieldName)) as name.getOrElse(s"${AggregationType.LongSum.toString.toLowerCase()}_${fieldName.toLowerCase()}")
            case AggregationType.DoubleSum => doubleSum(Dim(fieldName)) as name.getOrElse(s"${AggregationType.DoubleSum.toString.toLowerCase()}_${fieldName.toLowerCase()}")
            case AggregationType.DoubleMax => doubleMax(Dim(fieldName)) as name.getOrElse(s"${AggregationType.DoubleMax.toString.toLowerCase()}_${fieldName.toLowerCase()}")
            case AggregationType.DoubleMin => doubleMin(Dim(fieldName)) as name.getOrElse(s"${AggregationType.DoubleMin.toString.toLowerCase()}_${fieldName.toLowerCase()}")
            case AggregationType.LongMax => longMax(Dim(fieldName)) as name.getOrElse(s"${AggregationType.LongMax.toString.toLowerCase()}_${fieldName.toLowerCase()}")
            case AggregationType.LongMin => longMin(Dim(fieldName)) as name.getOrElse(s"${AggregationType.LongMin.toString.toLowerCase()}_${fieldName.toLowerCase()}")
            case AggregationType.DoubleFirst => doubleFirst(Dim(fieldName)) as name.getOrElse(s"${AggregationType.DoubleFirst.toString.toLowerCase()}_${fieldName.toLowerCase()}")
            case AggregationType.DoubleLast => doubleLast(Dim(fieldName)) as name.getOrElse(s"${AggregationType.DoubleLast.toString.toLowerCase()}_${fieldName.toLowerCase()}")
            case AggregationType.LongLast => longLast(Dim(fieldName)) as name.getOrElse(s"${AggregationType.LongLast.toString.toLowerCase()}_${fieldName.toLowerCase()}")
            case AggregationType.LongFirst =>longFirst(Dim(fieldName)) as name.getOrElse(s"${AggregationType.LongFirst.toString.toLowerCase()}_${fieldName.toLowerCase()}")
            case AggregationType.Javascript => ing.wbaa.druid.dql.AggregationOps.javascript(name.getOrElse(""), Iterable(fieldName), fnAggregate.get, fnCombine.get, fnReset.get)
        }
    }

    def getFilter(query: DruidQueryModel): Option[FilteringExpression] = {

        if (query.filters.nonEmpty) {
            val filters = query.filters.get.map { f =>
                val values = if (f.values.isEmpty && f.value.isEmpty) List() else if (f.values.isEmpty) List(f.value.get) else f.values.get
                getFilterByType(f.`type`, f.dimension, values)
            }
            Option(conjunction(filters: _*))
        }
        else None

    }

    def getFilterByType(filterType: String, dimension: String, values: List[AnyRef]): FilteringExpression = {
        filterType.toLowerCase match {
            case "isnull" => Dim(dimension).isNull
            case "isnotnull" => Dim(dimension).isNotNull
            case "equals" => Dim(dimension) === values.head.asInstanceOf[String]
            case "notequals" => Dim(dimension) =!= values.head.asInstanceOf[String]
            case "containsignorecase" => Dim(dimension).containsIgnoreCase(values.head.asInstanceOf[String])
            case "contains" => Dim(dimension).contains(values.head.asInstanceOf[String], true)
            case "in" => Dim(dimension) in values.asInstanceOf[List[String]]
            case "notin" => Dim(dimension) notIn values.asInstanceOf[List[String]]
            case "regex" => Dim(dimension) regex values.head.asInstanceOf[String]
            case "like" => Dim(dimension) like values.head.asInstanceOf[String]
            case "greaterthan" => Dim(dimension).between(values.head.asInstanceOf[Number].doubleValue(), Integer.MAX_VALUE, true, false)
            case "lessthan" => Dim(dimension).between(0, values.head.asInstanceOf[Number].doubleValue(), false, true)
        }
    }

    def getPostAggregation(query: DruidQueryModel): Option[List[PostAggregationExpression]] = {
            if (query.postAggregation.nonEmpty) {
                Option(query.postAggregation.get.map { f =>
                    PostAggregationType.decode(f.`type`) match {
                        case Right(x) => getPostAggregationByType(x, f.name, f.fields, f.fn)
                        case Left(x) => throw x
                    }
                })
            }
            else None
    }

    def getPostAggregationByType(postAggType: PostAggregationType, name: String, fields: PostAggregationFields, fn: String): PostAggregationExpression = {
        postAggType match {
            case PostAggregationType.Arithmetic =>
                fn match {
                    // only right field can have type as Constant or FieldAccess
                    case "+" => if("constant".equalsIgnoreCase(fields.rightFieldType)) Dim(fields.leftField).+(fields.rightField.asInstanceOf[Number].doubleValue()) as name else Dim(fields.leftField).+(Dim(fields.rightField.asInstanceOf[String])) as name
                    case "-" => if("constant".equalsIgnoreCase(fields.rightFieldType)) Dim(fields.leftField).-(fields.rightField.asInstanceOf[Number].doubleValue()) as name else Dim(fields.leftField).-(Dim(fields.rightField.asInstanceOf[String])) as name
                    case "*" => if("constant".equalsIgnoreCase(fields.rightFieldType)) Dim(fields.leftField).*(fields.rightField.asInstanceOf[Number].doubleValue()) as name else Dim(fields.leftField).*(Dim(fields.rightField.asInstanceOf[String])) as name
                    case "/" => if("constant".equalsIgnoreCase(fields.rightFieldType)) Dim(fields.leftField)./(fields.rightField.asInstanceOf[Number].doubleValue()) as name else Dim(fields.leftField)./(Dim(fields.rightField.asInstanceOf[String])) as name
                }
            case PostAggregationType.Javascript => javascript(name, Seq(Dim(fields.leftField),Dim(fields.rightField.asInstanceOf[String])), fn)
        }
    }

    def getGroupByHaving(query: DruidQueryModel): Option[FilteringExpression] = {

        if (query.having.nonEmpty) {
            HavingType.decode(query.having.get.`type`) match {
                case Right(x) => Option(getGroupByHavingByType(x, query.having.get.aggregation, query.having.get.value))
                case Left(x) => throw x
            }
        }
        else None
    }

    def getGroupByHavingByType(postAggType: HavingType, field: String, value: AnyRef): FilteringExpression = {
        postAggType match {
            case HavingType.EqualTo => Dim(field) === value.asInstanceOf[String]
            case HavingType.Not => Dim(field) =!= value.asInstanceOf[String]
            case HavingType.GreaterThan => Dim(field) > value.asInstanceOf[Number].doubleValue()
            case HavingType.LessThan => Dim(field) < value.asInstanceOf[Number].doubleValue()
        }
    }
}

