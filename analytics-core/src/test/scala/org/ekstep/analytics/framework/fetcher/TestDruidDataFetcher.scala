package org.ekstep.analytics.framework.fetcher

import java.time.{ZoneOffset, ZonedDateTime}

import cats.syntax.either._
import ing.wbaa.druid._
import ing.wbaa.druid.client.DruidClient
import ing.wbaa.druid.definitions.{AggregationType, PostAggregationType}
import io.circe._
import io.circe.parser._
import org.ekstep.analytics.framework._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class TestDruidDataFetcher extends SparkSpec with Matchers with MockFactory {

    it should "check for getAggregationTypes methods" in {

        val uniqueExpr = DruidDataFetcher.getAggregationByType(AggregationType.HyperUnique, Option("Unique"), "field", None, None, None)
        val uniqueExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.HyperUnique, None, "field", None, None, None)
        uniqueExpr.toString should be ("HyperUniqueAgg(field,Some(Unique),false,false)")

        val thetaSketchExpr = DruidDataFetcher.getAggregationByType(AggregationType.ThetaSketch, Option("Unique"), "field", None, None, None)
        val thetaSketchExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.ThetaSketch, None, "field", None, None, None)
        thetaSketchExpr.toString should be ("ThetaSketchAgg(field,Some(Unique),false,16384)")

        val cardinalityExpr = DruidDataFetcher.getAggregationByType(AggregationType.Cardinality, Option("Unique"), "field", None, None, None)
        val cardinalityExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.Cardinality, None, "field", None, None, None)
        cardinalityExpr.toString should be ("CardinalityAgg(WrappedArray(Dim(field,None,None,None)),Some(Unique),false,false)")

        val longSumExpr = DruidDataFetcher.getAggregationByType(AggregationType.LongSum, Option("Total"), "field", None, None, None)
        val longSumExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.LongSum, None, "field", None, None, None)

        val doubleSumExpr = DruidDataFetcher.getAggregationByType(AggregationType.DoubleSum, Option("Total"), "field", None, None, None)
        val doubleSumExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.DoubleSum, None, "field", None, None, None)

        val doubleMaxExpr = DruidDataFetcher.getAggregationByType(AggregationType.DoubleMax, Option("Max"), "field", None, None, None)
        val doubleMaxExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.DoubleMax, None, "field", None, None, None)

        val doubleMinExpr = DruidDataFetcher.getAggregationByType(AggregationType.DoubleMin, Option("Min"), "field", None, None, None)
        val doubleMinExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.DoubleMin, None, "field", None, None, None)

        val longMaxExpr = DruidDataFetcher.getAggregationByType(AggregationType.LongMax, Option("Max"), "field", None, None, None)
        val longMaxExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.LongMax, None, "field", None, None, None)

        val longMinExpr = DruidDataFetcher.getAggregationByType(AggregationType.LongMin, Option("Min"), "field", None, None, None)
        val longMinExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.LongMin, None, "field", None, None, None)

        val doubleFirstExpr = DruidDataFetcher.getAggregationByType(AggregationType.DoubleFirst, Option("First"), "field", None, None, None)
        val doubleFirstExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.DoubleFirst, None, "field", None, None, None)

        val doubleLastExpr = DruidDataFetcher.getAggregationByType(AggregationType.DoubleLast, Option("Last"), "field", None, None, None)
        val doubleLastExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.DoubleLast, None, "field", None, None, None)

        val longFirstExpr = DruidDataFetcher.getAggregationByType(AggregationType.LongFirst, Option("First"), "field", None, None, None)
        val longFirstExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.LongFirst, None, "field", None, None, None)

        val longLastExpr = DruidDataFetcher.getAggregationByType(AggregationType.LongLast, Option("Last"), "field", None, None, None)
        val longLastExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.LongLast, None, "field", None, None, None)

        val javascriptExpr = DruidDataFetcher.getAggregationByType(AggregationType.Javascript, Option("OutPut"), "field",
            Option("function(current, edata_size) { return current + (edata_size == 0 ? 1 : 0); }"),
            Option("function(partialA, partialB) { return partialA + partialB; }"), Option("function () { return 0; }"))
        javascriptExpr.toString should be ("JavascriptAgg(List(field),function(current, edata_size) { return current + (edata_size == 0 ? 1 : 0); },function(partialA, partialB) { return partialA + partialB; },function () { return 0; },Some(OutPut))")

        val javascriptExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.Javascript, None, "field",
            Option("function(current, edata_size) { return current + (edata_size == 0 ? 1 : 0); }"),
            Option("function(partialA, partialB) { return partialA + partialB; }"), Option("function () { return 0; }"))
    }

    it should "check for getFilterTypes methods" in {

        val isNullExpr = DruidDataFetcher.getFilterByType("isnull", "field", List())
        isNullExpr.asFilter.`type`.toString should be ("Selector")

        val isNotNullExpr = DruidDataFetcher.getFilterByType("isnotnull", "field", List())

        val equalsExpr = DruidDataFetcher.getFilterByType("equals", "field", List("abc"))

        val notEqualsExpr = DruidDataFetcher.getFilterByType("notequals", "field", List("xyz"))

        val containsIgnorecaseExpr = DruidDataFetcher.getFilterByType("containsignorecase", "field", List("abc"))

        val containsExpr = DruidDataFetcher.getFilterByType("contains", "field", List("abc"))

        val inExpr = DruidDataFetcher.getFilterByType("in", "field", List("abc", "xyz"))

        val notInExpr = DruidDataFetcher.getFilterByType("notin", "field", List("abc", "xyz"))

        val regexExpr = DruidDataFetcher.getFilterByType("regex", "field", List("%abc%"))

        val likeExpr = DruidDataFetcher.getFilterByType("like", "field", List("%abc%"))

        val greaterThanExpr = DruidDataFetcher.getFilterByType("greaterthan", "field", List(0.asInstanceOf[AnyRef]))

        val lessThanExpr = DruidDataFetcher.getFilterByType("lessthan", "field", List(1000.asInstanceOf[AnyRef]))
    }

    it should "check for getPostAggregation methods" in {

        val additionExpr = DruidDataFetcher.getPostAggregationByType(PostAggregationType.Arithmetic, "Addition", PostAggregationFields("field", ""), "+")
        additionExpr.getName.toString should be ("Addition")

        val subtractionExpr = DruidDataFetcher.getPostAggregationByType(PostAggregationType.Arithmetic, "Subtraction", PostAggregationFields("field", ""), "-")
        subtractionExpr.getName.toString should be ("Subtraction")

        val multiplicationExpr = DruidDataFetcher.getPostAggregationByType(PostAggregationType.Arithmetic, "Product", PostAggregationFields("field", ""), "*")
        multiplicationExpr.getName.toString should be ("Product")

        val divisionExpr = DruidDataFetcher.getPostAggregationByType(PostAggregationType.Arithmetic, "Division", PostAggregationFields("field", ""), "/")
        divisionExpr.getName.toString should be ("Division")

        val javaScriptExpr = DruidDataFetcher.getPostAggregationByType(PostAggregationType.Javascript, "Percentage", PostAggregationFields("fieldA", "fieldB"), "function(a, b) { return (a / b) * 100; }")
    }
    
    it should "fetch the data from druid using groupBy query type" in {

        val query = DruidQueryModel("groupBy", "telemetry-events", "2019-11-01/2019-11-02", Option("all"), Option(List(Aggregation(Option("count"), "count", ""),Aggregation(Option("total_duration"), "doubleSum", "edata_duration"))), Option(List(DruidDimension("context_pdata_id", Option("producer_id")), DruidDimension("context_pdata_pid", Option("producer_pid")))), Option(List(DruidFilter("in", "eid", None, Option(List("START", "END"))))))
        val druidQuery = DruidDataFetcher.getDruidQuery(query)
        druidQuery.toString() should be ("GroupByQuery(List(CountAggregation(count), DoubleSumAggregation(total_duration,edata_duration)),List(2019-11-01/2019-11-02),Some(AndFilter(List(InFilter(eid,List(START, END),None)))),List(DefaultDimension(context_pdata_id,Some(producer_id),None), DefaultDimension(context_pdata_pid,Some(producer_pid),None)),All,None,None,List(),Map())")
        
        val json: String = """
          {
              "total_scans" : 9007,
              "producer_id" : "dev.sunbird.learning.platform"
          }
        """
        val doc: Json = parse(json).getOrElse(Json.Null);
        val results = List(DruidResult.apply(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC), doc));
        val druidResponse = DruidResponse.apply(results, QueryType.GroupBy)

        implicit val mockFc = mock[FrameworkContext];
        implicit val druidConfig = mock[DruidConfig];
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.doQuery(_:DruidQuery)(_:DruidConfig)).expects(druidQuery, *).returns(Future(druidResponse))
        (mockFc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient);

        val druidResult = DruidDataFetcher.getDruidData(query)

        druidResult.size should be (1)
        druidResult.head should be ("""{"total_scans":9007.0,"producer_id":"dev.sunbird.learning.platform","date":"2019-11-28"}""")
    }
    
    it should "fetch the data from druid using timeseries query type" in {

        val query = DruidQueryModel("timeSeries", "telemetry-events", "2019-11-01/2019-11-02", Option("day"), None, None, Option(List(DruidFilter("in", "eid", None, Option(List("START", "END"))))))
        val druidQuery = DruidDataFetcher.getDruidQuery(query);
        druidQuery.toString() should be ("TimeSeriesQuery(List(CountAggregation(count_count)),List(2019-11-01/2019-11-02),Some(AndFilter(List(InFilter(eid,List(START, END),None)))),Day,false,List(),Map())");
        
        val json: String = """
          {
              "total_scans" : 9007,
              "producer_id" : "dev.sunbird.learning.platform"
          }
        """
        val doc: Json = parse(json).getOrElse(Json.Null);
        val results = List(DruidResult.apply(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC), doc));
        val druidResponse = DruidResponse.apply(results, QueryType.Timeseries)

        implicit val mockFc = mock[FrameworkContext];
        implicit val druidConfig = mock[DruidConfig];
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.doQuery(_:DruidQuery)(_:DruidConfig)).expects(druidQuery, *).returns(Future(druidResponse))
        (mockFc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient);

        val druidResult = DruidDataFetcher.getDruidData(query)
        
        druidResult.size should be (1)
        druidResult.head should be ("""{"total_scans":9007.0,"producer_id":"dev.sunbird.learning.platform","date":"2019-11-28"}""")
    }

    it should "fetch the data from druid using topN query type" in {

        val query = DruidQueryModel("topN", "telemetry-events", "2019-11-01/2019-11-02", Option("day"), Option(List(Aggregation(Option("count"), "count", ""))), Option(List(DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "eid", None, Option(List("START", "END"))))))
        val druidQuery = DruidDataFetcher.getDruidQuery(query);
        druidQuery.toString() should be ("TopNQuery(DefaultDimension(context_pdata_id,Some(producer_id),None),100,count,List(CountAggregation(count)),List(2019-11-01/2019-11-02),Day,Some(AndFilter(List(InFilter(eid,List(START, END),None)))),List(),Map())")

        val json: String = """
          [
            {
              "count" : 5,
              "producer_id" : "dev.sunbird.portal"
            },
            {
              "count" : 1,
              "producer_id" : "local.sunbird.desktop"
            }
          ]
        """
        val doc: Json = parse(json).getOrElse(Json.Null);
        val results = List(DruidResult.apply(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC), doc));
        val druidResponse = DruidResponse.apply(results, QueryType.TopN)

        implicit val mockFc = mock[FrameworkContext];
        implicit val druidConfig = mock[DruidConfig];
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.doQuery(_:DruidQuery)(_:DruidConfig)).expects(druidQuery, *).returns(Future(druidResponse))
        (mockFc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient);

        val druidResult = DruidDataFetcher.getDruidData(query)

        druidResult.size should be (2)
        druidResult.head should be ("""{"date":"2019-11-28","count":5,"producer_id":"dev.sunbird.portal"}""")
        druidResult.last should be ("""{"date":"2019-11-28","count":1,"producer_id":"local.sunbird.desktop"}""")
    }
}
