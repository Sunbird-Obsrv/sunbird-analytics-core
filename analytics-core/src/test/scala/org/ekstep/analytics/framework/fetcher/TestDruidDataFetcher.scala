package org.ekstep.analytics.framework.fetcher

import java.time.{ZoneOffset, ZonedDateTime}
import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.scaladsl.Source
import akka.util.ByteString
import cats.syntax.either._
import com.ing.wbaa.druid._
import com.ing.wbaa.druid.client.DruidClient
import com.ing.wbaa.druid.definitions.{AggregationType, PostAggregationType}
import io.circe._
import io.circe.parser._
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher.getSQLDruidQuery
import org.ekstep.analytics.framework.util.{CommonUtil, EmbeddedPostgresqlService, HTTPClient, JSONUtils}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import org.joda.time.DateTimeUtils
import org.sunbird.cloud.storage.conf.AppConf

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future



class TestDruidDataFetcher extends SparkSpec with Matchers with MockFactory {
    override def beforeAll () {
        super.beforeAll()
        EmbeddedPostgresqlService.start()
        EmbeddedPostgresqlService.createNominationTable()
    }

    it should "check for getDimensionByType methods" in {
        val defaultExpr = DruidDataFetcher.getDimensionByType(None, "field", Option("field1"))
        defaultExpr.toString should be ("Dim(field,Some(field1),None,None)")

        val javascriptExtractionExpr = DruidDataFetcher.getDimensionByType(Option("extraction"), "field", Option("field1"), Option("String"), Option(List(ExtractFn("javascript", "function(x) { return x + 10; }"))))
        javascriptExtractionExpr.toString should be ("Dim(field,Some(field1),Some(String),Some(JavascriptExtractionFn(function(x) { return x + 10; },Some(false))))")

        val lookupExtractionExpr = DruidDataFetcher.getDimensionByType(Option("extraction"), "field", Option("field1"), Option("String"), Option(List(ExtractFn("registeredlookup", "channel"))))
        lookupExtractionExpr.toString should be ("Dim(field,Some(field1),Some(String),Some(RegisteredLookupExtractionFn(channel,Some(false),None)))")

        val cascadeExtractionExpr = DruidDataFetcher.getDimensionByType(Option("cascade"), "field", Option("field1"), Option("String"), Option(List(ExtractFn("registeredlookup", "channel"),ExtractFn("javascript", "function(x) { return x + 10; }"))))
        cascadeExtractionExpr.toString should be ("Dim(field,Some(field1),Some(String),Some(CascadeExtractionFn(List(RegisteredLookupExtractionFn(channel,Some(false),None), JavascriptExtractionFn(function(x) { return x + 10; },Some(false))))))")
    }

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

        a[Exception] should be thrownBy {
            DruidDataFetcher.getAggregationByType(AggregationType.Filtered, Option("Last"), "field", None, None, None)
        }

        a[Exception] should be thrownBy {
            DruidDataFetcher.getAggregationByType(AggregationType.Filtered, Option("Last"), "field", None, None, None, None, None, None, Option("longSum"))
        }

        a[Exception] should be thrownBy {
            DruidDataFetcher.getAggregationByType(AggregationType.Filtered, Option("Last"), "field", None, None, None, None, None, None, Option("longSum"), Option("edata_size"))
        }

        val filteredExp = DruidDataFetcher.getAggregationByType(AggregationType.Filtered, Option("Last"), "field", None, None, None, None, None, None, Option("longSum"), Option("edata_size"), Option(0.asInstanceOf[AnyRef]))
        filteredExp.toString should be("SelectorFilteredAgg(edata_size,Some(0),LongSumAggregation(Last,field),None)")

        DruidDataFetcher.getAggregation(Option(List(Aggregation(Option("count"), "test", "field")))).head.getName should be ("count");

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

        a[Exception] should be thrownBy {
            DruidDataFetcher.getFilterByType("test", "field", List(1000.asInstanceOf[AnyRef]))
        }

        DruidDataFetcher.getFilter(None) should be (None)

        DruidDataFetcher.getFilter(Option(List(DruidFilter("in", "eid", None, None)))).get.asFilter.toString() should be ("AndFilter(List(InFilter(eid,List(),None)))")
        DruidDataFetcher.getFilter(Option(List(DruidFilter("in", "eid", Option("START"), None)))).get.asFilter.toString() should be ("AndFilter(List(InFilter(eid,List(START),None)))")

    }

    it should "check for getGroupByHaving methods" in {

        var filteringExpr = DruidDataFetcher.getGroupByHaving(Option(DruidHavingFilter("lessThan", "doubleSum", 20.asInstanceOf[AnyRef])));
        filteringExpr.get.asFilter.toString() should be ("BoundFilter(doubleSum,None,Some(20.0),None,Some(true),Some(Numeric),None)")

        filteringExpr = DruidDataFetcher.getGroupByHaving(Option(DruidHavingFilter("equalTo", "user_id", "user1")));
        filteringExpr.get.asFilter.toString() should be ("SelectFilter(user_id,Some(user1),None)")

        filteringExpr = DruidDataFetcher.getGroupByHaving(Option(DruidHavingFilter("not", "user_id", "user1")));
        filteringExpr.get.asFilter.toString() should be ("NotFilter(SelectFilter(user_id,Some(user1),None))")

        filteringExpr = DruidDataFetcher.getGroupByHaving(Option(DruidHavingFilter("greaterThan", "doubleSum", 20.asInstanceOf[AnyRef])));
        filteringExpr.get.asFilter.toString() should be ("BoundFilter(doubleSum,Some(20.0),None,Some(true),None,Some(Numeric),None)")

        a[Exception] should be thrownBy {
            DruidDataFetcher.getGroupByHaving(Option(DruidHavingFilter("and", "doubleSum", 20.asInstanceOf[AnyRef])));
        }

        a[Exception] should be thrownBy {
            DruidDataFetcher.getGroupByHaving(Option(DruidHavingFilter("in", "doubleSum", 20.asInstanceOf[AnyRef])));
        }

        DruidDataFetcher.getGroupByHaving(None) should be (None);

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

        val javaScriptExpr1 = DruidDataFetcher.getPostAggregationByType(PostAggregationType.Javascript, "Percentage", PostAggregationFields("fieldA", "fieldB"), "function(a, b) { return (a / b) * 100; }")
        javaScriptExpr1.toString should be ("JavascriptPostAgg(List(fieldA, fieldB),function(a, b) { return (a / b) * 100; },Some(Percentage))")

        val javaScriptExpr2 = DruidDataFetcher.getPostAggregationByType(PostAggregationType.Javascript, "MultiplyBy100", PostAggregationFields("fieldA", ""), "function(a) { return a * 100; }")
        javaScriptExpr2.toString should be ("JavascriptPostAgg(List(fieldA),function(a) { return a * 100; },Some(MultiplyBy100))")

        val additionExpr2 = DruidDataFetcher.getPostAggregationByType(PostAggregationType.Arithmetic, "Addition", PostAggregationFields("field", 1.asInstanceOf[AnyRef], "constant"), "+")
        additionExpr2.getName.toString should be ("Addition")

        val subtractionExpr2 = DruidDataFetcher.getPostAggregationByType(PostAggregationType.Arithmetic, "Subtraction", PostAggregationFields("field", 1.asInstanceOf[AnyRef], "constant"), "-")
        subtractionExpr2.getName.toString should be ("Subtraction")

        val multiplicationExpr2 = DruidDataFetcher.getPostAggregationByType(PostAggregationType.Arithmetic, "Product", PostAggregationFields("field", 1.asInstanceOf[AnyRef], "constant"), "*")
        multiplicationExpr2.getName.toString should be ("Product")

        val divisionExpr2 = DruidDataFetcher.getPostAggregationByType(PostAggregationType.Arithmetic, "Division", PostAggregationFields("field", 1.asInstanceOf[AnyRef], "constant"), "/")
        divisionExpr2.getName.toString should be ("Division")

        a[Exception] should be thrownBy {
            DruidDataFetcher.getPostAggregation(Option(List(PostAggregation("longLeast", "Division", PostAggregationFields("field", 1.asInstanceOf[AnyRef], "constant"), "/"))))
        }

        a[Exception] should be thrownBy {
            DruidDataFetcher.getPostAggregation(Option(List(PostAggregation("test", "Division", PostAggregationFields("field", 1.asInstanceOf[AnyRef], "constant"), "/"))))
        }

        DruidDataFetcher.getPostAggregation(None) should be (None);

    }

    it should "test the getDruidQuery method" in {
        var query = DruidQueryModel("groupBy", "telemetry-events", "2019-11-01/2019-11-02", Option("all"), Option(List(Aggregation(Option("count"), "count", ""),Aggregation(Option("total_duration"), "doubleSum", "edata_duration"))), Option(List(DruidDimension("context_pdata_id", Option("producer_id")), DruidDimension("context_pdata_pid", Option("producer_pid")))), None, None, None)
        var druidQuery = DruidDataFetcher.getDruidQuery(query)
        druidQuery.toString() should be ("GroupByQuery(List(CountAggregation(count), DoubleSumAggregation(total_duration,edata_duration)),List(2019-11-01/2019-11-02),None,List(DefaultDimension(context_pdata_id,Some(producer_id),None), DefaultDimension(context_pdata_pid,Some(producer_pid),None)),All,None,None,List(),Map())");

        query = DruidQueryModel("topN", "telemetry-events", "2019-11-01/2019-11-02", Option("day"), Option(List(Aggregation(Option("count"), "count", ""))), Option(List(DruidDimension("context_pdata_id", Option("producer_id")))), None, None, None)
        druidQuery = DruidDataFetcher.getDruidQuery(query)
        druidQuery.toString() should be ("TopNQuery(DefaultDimension(context_pdata_id,Some(producer_id),None),100,count,List(CountAggregation(count)),List(2019-11-01/2019-11-02),Day,None,List(),Map())");

        query = DruidQueryModel("timeSeries", "telemetry-events", "2019-11-01/2019-11-02", Option("day"), None, None, None, None, None)
        druidQuery = DruidDataFetcher.getDruidQuery(query)
        druidQuery.toString() should be ("TimeSeriesQuery(List(CountAggregation(count_count)),List(2019-11-01/2019-11-02),None,Day,false,List(),Map())");

        DateTimeUtils.setCurrentMillisFixed(1577836800000L); // Setting Jan 1 2020 as current time
        query = DruidQueryModel("topN", "telemetry-events", "Last7Days", Option("day"), Option(List(Aggregation(Option("count"), "count", ""))), Option(List(DruidDimension("context_pdata_id", Option("producer_id")))), None, None, None, intervalSlider = 2)
        druidQuery = DruidDataFetcher.getDruidQuery(query)
        druidQuery.toString() should be ("TopNQuery(DefaultDimension(context_pdata_id,Some(producer_id),None),100,count,List(CountAggregation(count)),List(2019-12-23T05:30:00+00:00/2019-12-30T05:30:00+00:00),Day,None,List(),Map())");
        DateTimeUtils.setCurrentMillisSystem();
    }

    it should "fetch the data from druid using groupBy query type" in {

        val query = DruidQueryModel("groupBy", "telemetry-events", "2019-11-01/2019-11-02", Option("all"), Option(List(Aggregation(Option("count"), "count", ""),Aggregation(Option("total_duration"), "doubleSum", "edata_duration"))), Option(List(DruidDimension("context_pdata_id", Option("producer_id")), DruidDimension("context_pdata_pid", Option("producer_pid")))), Option(List(DruidFilter("in", "eid", None, Option(List("START", "END"))))), Option(DruidHavingFilter("lessThan", "doubleSum", 20.asInstanceOf[AnyRef])), Option(List(PostAggregation("arithmetic", "Addition", PostAggregationFields("field", ""), "+"))))
        val druidQuery = DruidDataFetcher.getDruidQuery(query)
        druidQuery.toString() should be ("GroupByQuery(List(CountAggregation(count), DoubleSumAggregation(total_duration,edata_duration)),List(2019-11-01/2019-11-02),Some(AndFilter(List(InFilter(eid,List(START, END),None)))),List(DefaultDimension(context_pdata_id,Some(producer_id),None), DefaultDimension(context_pdata_pid,Some(producer_pid),None)),All,Some(LessThanHaving(doubleSum,20.0)),None,List(ArithmeticPostAggregation(Addition,PLUS,List(FieldAccessPostAggregation(field,None), FieldAccessPostAggregation(,None)),Some(FloatingPoint))),Map())")

        val json: String = """
          {
              "total_scans" : 9007,
              "producer_id" : "dev.sunbird.learning.platform"
          }
        """
        val doc: Json = parse(json).getOrElse(Json.Null);
        val results = List(DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc));
        val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)

        implicit val mockFc = mock[FrameworkContext];
        implicit val druidConfig = mock[DruidConfig];
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.doQuery[DruidResponse](_:DruidNativeQuery)(_:DruidConfig)).expects(druidQuery, *).returns(Future(druidResponse)).anyNumberOfTimes()
        (mockFc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes();

        val druidResult = DruidDataFetcher.getDruidData(query).collect

        druidResult.size should be (1)
        druidResult.head should be ("""{"total_scans":9007.0,"producer_id":"dev.sunbird.learning.platform","date":"2019-11-28"}""")
    }

    it should "fetch the data from druid using timeseries query type" in {

        val query = DruidQueryModel("timeSeries", "telemetry-events", "2019-11-01/2019-11-02", Option("day"), None, None, Option(List(DruidFilter("in", "eid", None, Option(List("START", "END"))))), None, Option(List(PostAggregation("arithmetic", "Addition", PostAggregationFields("field", ""), "+"))))
        val druidQuery = DruidDataFetcher.getDruidQuery(query);
        druidQuery.toString() should be ("TimeSeriesQuery(List(CountAggregation(count_count)),List(2019-11-01/2019-11-02),Some(AndFilter(List(InFilter(eid,List(START, END),None)))),Day,false,List(ArithmeticPostAggregation(Addition,PLUS,List(FieldAccessPostAggregation(field,None), FieldAccessPostAggregation(,None)),Some(FloatingPoint))),Map())");

        var json: String = """
          {
              "total_scans" : 9007,
              "producer_id" : "dev.sunbird.learning.platform"
          }
        """
        var doc: Json = parse(json).getOrElse(Json.Null);
        var results = List(DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc));
        var druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.Timeseries)

        implicit val mockFc = mock[FrameworkContext];
        implicit val druidConfig = mock[DruidConfig];
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.doQuery[DruidResponse](_:DruidQuery)(_:DruidConfig)).expects(druidQuery, *).returns(Future(druidResponse))
        (mockFc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes()

        var druidResult = DruidDataFetcher.getDruidData(query).collect

        druidResult.size should be (1)
        druidResult.head should be ("""{"total_scans":9007.0,"producer_id":"dev.sunbird.learning.platform","date":"2019-11-28"}""")

        json = """
          {
              "total_scans" : null,
              "producer_id" : "dev.sunbird.learning.platform"
          }
        """
        doc = parse(json).getOrElse(Json.Null);
        results = List(DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc));
        druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.Timeseries)
        (mockDruidClient.doQuery[DruidResponse](_:DruidQuery)(_:DruidConfig)).expects(druidQuery, *).returns(Future(druidResponse))
        //        (mockFc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient)

        druidResult = DruidDataFetcher.getDruidData(query).collect()
        druidResult.size should be (1)
        druidResult.head should be ("""{"total_scans":"unknown","producer_id":"dev.sunbird.learning.platform","date":"2019-11-28"}""")

        json = """
          {
              "total_scans" : {},
              "producer_id" : "dev.sunbird.learning.platform"
          }
        """
        doc = parse(json).getOrElse(Json.Null);
        results = List(DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc));
        druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.Timeseries)
        (mockDruidClient.doQuery[DruidResponse](_:DruidQuery)(_:DruidConfig)).expects(druidQuery, *).returns(Future(druidResponse))
        //        (mockFc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient)

        druidResult = DruidDataFetcher.getDruidData(query).collect()

        druidResult.size should be (1)
    }

    it should "fetch the data from druid using topN query type" in {

        val query = DruidQueryModel("topN", "telemetry-events", "2019-11-01/2019-11-02", Option("day"), Option(List(Aggregation(Option("count"), "count", ""))), Option(List(DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "eid", None, Option(List("START", "END"))))), None, Option(List(PostAggregation("arithmetic", "Addition", PostAggregationFields("field", ""), "+"))))
        val druidQuery = DruidDataFetcher.getDruidQuery(query);
        druidQuery.toString() should be ("TopNQuery(DefaultDimension(context_pdata_id,Some(producer_id),None),100,count,List(CountAggregation(count)),List(2019-11-01/2019-11-02),Day,Some(AndFilter(List(InFilter(eid,List(START, END),None)))),List(ArithmeticPostAggregation(Addition,PLUS,List(FieldAccessPostAggregation(field,None), FieldAccessPostAggregation(,None)),Some(FloatingPoint))),Map())")

        val json: String = """
          [
            {
              "count" : 5,
              "producer_id" : "dev.sunbird.portal"
            },
            {
              "count" : 1,
              "producer_id" : "local.sunbird.desktop"
            },
            {
              "count" : null,
              "producer_id" : "local.sunbird.app"
            },
            {
              "count" : {},
              "producer_id" : "local.sunbird.app"
            }
          ]
        """
        val doc: Json = parse(json).getOrElse(Json.Null);
        val results = List(DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc));
        val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.TopN)


        implicit val mockFc = mock[FrameworkContext];
        implicit val druidConfig = mock[DruidConfig];
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.doQuery[DruidResponse](_:DruidQuery)(_:DruidConfig)).expects(druidQuery, *).returns(Future(druidResponse))
        (mockFc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient)

        val druidResult = DruidDataFetcher.getDruidData(query).collect()
        druidResult.size should be (4)

        druidResult(0) should be ("""{"date":"2019-11-28","count":5.0,"producer_id":"dev.sunbird.portal"}""")
        druidResult(1) should be ("""{"date":"2019-11-28","count":1.0,"producer_id":"local.sunbird.desktop"}""")
        druidResult(2) should be ("""{"date":"2019-11-28","count":"unknown","producer_id":"local.sunbird.app"}""")

        val druidResponse2 = DruidResponseTimeseriesImpl.apply(List(), QueryType.TopN)
        (mockDruidClient.doQuery[DruidResponse](_:DruidQuery)(_:DruidConfig)).expects(druidQuery, *).returns(Future(druidResponse2))
        (mockFc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient)
        val druidResult2 = DruidDataFetcher.getDruidData(query).collect()
        druidResult2.size should be (0)

    }
    it should "fetch the data from druid rollup cluster using groupBy query type" in {

        val query = DruidQueryModel("groupBy", "telemetry-rollup-events", "2019-11-01/2019-11-02", Option("all"), Option(List(Aggregation(Option("count"), "count", ""),Aggregation(Option("total_duration"), "doubleSum", "edata_duration"))), Option(List(DruidDimension("context_pdata_id", Option("producer_id")), DruidDimension("context_pdata_pid", Option("producer_pid")))), Option(List(DruidFilter("in", "eid", None, Option(List("START", "END"))))), Option(DruidHavingFilter("lessThan", "doubleSum", 20.asInstanceOf[AnyRef])), Option(List(PostAggregation("arithmetic", "Addition", PostAggregationFields("field", ""), "+"))))
        val druidQuery = DruidDataFetcher.getDruidQuery(query)
        druidQuery.toString() should be ("GroupByQuery(List(CountAggregation(count), DoubleSumAggregation(total_duration,edata_duration)),List(2019-11-01/2019-11-02),Some(AndFilter(List(InFilter(eid,List(START, END),None)))),List(DefaultDimension(context_pdata_id,Some(producer_id),None), DefaultDimension(context_pdata_pid,Some(producer_pid),None)),All,Some(LessThanHaving(doubleSum,20.0)),None,List(ArithmeticPostAggregation(Addition,PLUS,List(FieldAccessPostAggregation(field,None), FieldAccessPostAggregation(,None)),Some(FloatingPoint))),Map())")

        val json: String = """
          {
              "total_scans" : 9007,
              "producer_id" : "dev.sunbird.learning.platform"
          }
        """
        val doc: Json = parse(json).getOrElse(Json.Null);
        val results = List(DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc));
        val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)

        implicit val mockFc = mock[FrameworkContext];
        implicit val druidConfig = mock[DruidConfig];
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.doQuery[DruidResponse](_:DruidQuery)(_:DruidConfig)).expects(druidQuery, *).returns(Future(druidResponse))
        (mockFc.getDruidRollUpClient: () => DruidClient).expects().returns(mockDruidClient);

        val druidResult = DruidDataFetcher.getDruidData(query).collect()

        druidResult.size should be (1)
        druidResult.head should be ("""{"total_scans":9007.0,"producer_id":"dev.sunbird.learning.platform","date":"2019-11-28"}""")
    }

    it should "fetch data for groupBy dimensions with extraction fn" in {
        val qrScans = DruidQueryModel("groupBy", "telemetry-rollup-syncts", "2020-03-01/2020-04-01", Option("all"), Option(List(Aggregation(Option("total_scans"),"longSum", "total_count"))), Option(List(DruidDimension("derived_loc_state", Option("state")), DruidDimension("derived_loc_district", Option("district"),Option("Extraction"), Option("STRING"), Option(List(ExtractFn("javascript", "function(str){return str == null ? null: str.toLowerCase().trim().split(' ').map(function(t){return t.substring(0,1).toUpperCase()+t.substring(1,t.length)}).join(' ')}")))))), Option(List(DruidFilter("in", "object_type", None, Option(List("qr", "Qr", "DialCode", "dialcode"))), DruidFilter("equals", "eid", Option("SEARCH")), DruidFilter("equals", "derived_loc_state", Option("Andhra Pradesh")), DruidFilter("isnotnull", "derived_loc_district", None))))
        val druidQuery = DruidDataFetcher.getDruidQuery(qrScans)
        druidQuery.toString should be ("GroupByQuery(List(LongSumAggregation(total_scans,total_count)),List(2020-03-01/2020-04-01),Some(AndFilter(List(InFilter(object_type,List(qr, Qr, DialCode, dialcode),None), SelectFilter(eid,Some(SEARCH),None), SelectFilter(derived_loc_state,Some(Andhra Pradesh),None), NotFilter(SelectFilter(derived_loc_district,None,None))))),List(DefaultDimension(derived_loc_state,Some(state),None), ExtractionDimension(derived_loc_district,Some(district),Some(STRING),JavascriptExtractionFn(function(str){return str == null ? null: str.toLowerCase().trim().split(' ').map(function(t){return t.substring(0,1).toUpperCase()+t.substring(1,t.length)}).join(' ')},Some(false)))),All,None,None,List(),Map())")


        val json = """{"total_scans":7257.0,"district":"Anantapur","state":"Andhra Pradesh","date":"2020-03-01"}"""

        val doc: Json = parse(json).getOrElse(Json.Null);
        val results = List(DruidResult.apply(Some(ZonedDateTime.of(2020, 3, 1, 0, 0, 0, 0, ZoneOffset.UTC)), doc));
        val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)

        implicit val mockFc = mock[FrameworkContext];
        implicit val druidConfig = mock[DruidConfig];
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.doQuery[DruidResponse](_:DruidQuery)(_:DruidConfig)).expects(druidQuery, *).returns(Future(druidResponse)).anyNumberOfTimes()
        (mockFc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes();
        (mockFc.getDruidRollUpClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes();
        val druidResult = DruidDataFetcher.getDruidData(qrScans).collect()

        druidResult.size should be (1)
        druidResult.head should be ("""{"total_scans":7257.0,"district":"Anantapur","state":"Andhra Pradesh","date":"2020-03-01"}""")
    }

    "TesthLL" should "fetch data for groupBy dimension with HLLAggregator" in {
        val districtMonthly = DruidQueryModel("groupBy", "summary-distinct-counts", "2020-05-12/2020-05-13", Option("all"), Option(List(Aggregation(Option("total_unique_devices"), "HLLSketchMerge", "unique_devices", None, None, None, None, None), Aggregation(None, "HLLSketchMerge", "devices", None, None, None, None, None), Aggregation(Option("Count"), "count", ""))), Option(List(DruidDimension("derived_loc_state", Option("state")), DruidDimension("derived_loc_district", Option("district")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))), DruidFilter("isnotnull", "derived_loc_district", None))))
        val druidQuery = DruidDataFetcher.getDruidQuery(districtMonthly)
        druidQuery.toString should be ("GroupByQuery(List(HLLAggregation(total_unique_devices,unique_devices,12,HLL_4,true), HLLAggregation(hllsketchmerge_devices,devices,12,HLL_4,true), CountAggregation(Count)),List(2020-05-12/2020-05-13),Some(AndFilter(List(InFilter(dimensions_pdata_id,List(prod.diksha.app, prod.diksha.portal),None), NotFilter(SelectFilter(derived_loc_district,None,None))))),List(DefaultDimension(derived_loc_state,Some(state),None), DefaultDimension(derived_loc_district,Some(district),None)),All,None,None,List(),Map())")

        val json = """{"state":"Andaman & Nicobar Islands","total_unique_devices":1.0,"Count":9.0,"date":"2020-03-01","district":"Ahmednagar"}"""
        val doc: Json = parse(json).getOrElse(Json.Null);
        val results = List(DruidResult.apply(Some(ZonedDateTime.of(2020, 3, 1, 0, 0, 0, 0, ZoneOffset.UTC)), doc));
        val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)

        implicit val mockFc = mock[FrameworkContext];
        implicit val druidConfig = mock[DruidConfig];
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.doQuery[DruidResponse](_:DruidQuery)(_:DruidConfig)).expects(druidQuery, *).returns(Future(druidResponse))
        (mockFc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes();
        (mockFc.getDruidRollUpClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes();

        val druidResult = DruidDataFetcher.getDruidData(districtMonthly).collect()
        druidResult.size should be (1)
        druidResult.head should be ("""{"state":"Andaman & Nicobar Islands","total_unique_devices":1.0,"Count":9.0,"date":"2020-03-01","district":"Ahmednagar"}""")
    }

    "TestFetcher" should "fetch data for TopN dimension with Lookup" in {
        val query = DruidQueryModel("topN", "telemetry-events", "2020-03-12T00:00:00+00:00/2020-05-12T00:00:00+00:00", Option("all"),
            Option(List(Aggregation(Option("count"), "count", "count"))),
            Option(List(DruidDimension("dialcode_channel", Option("dialcode_slug"), Option("extraction"), None,
                Option(List(ExtractFn("registeredlookup", "channel")))))),
            Option(List(DruidFilter("equals", "dialcode_channel", Option("012315809814749184151")))), None, None, None,None,None, None, Option("count"))

        val druidQuery = DruidDataFetcher.getDruidQuery(query)
        druidQuery.toString should be("TopNQuery(ExtractionDimension(dialcode_channel,Some(dialcode_slug),None,RegisteredLookupExtractionFn(channel,Some(false),None)),100,count,List(CountAggregation(count)),List(2020-03-12T00:00:00+00:00/2020-05-12T00:00:00+00:00),All,Some(AndFilter(List(SelectFilter(dialcode_channel,Some(012315809814749184151),None)))),List(),Map())")

        val json = """[{"date":"2020-03-13","count":9,"dialcode_slug":"Andaman & Nicobar Islands"}]"""
        val doc: Json = parse(json).getOrElse(Json.Null);
        val results = List(DruidResult.apply(Some(ZonedDateTime.of(2020, 3, 1, 0, 0, 0, 0, ZoneOffset.UTC)), doc));
        val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.TopN)

        implicit val mockFc = mock[FrameworkContext];
        implicit val druidConfig = mock[DruidConfig];
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(druidQuery, *).returns(Future(druidResponse)).anyNumberOfTimes()
        (mockFc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes();
        (mockFc.getDruidRollUpClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes();

        val druidResult = DruidDataFetcher.getDruidData(query).collect()
        druidResult.size should be (1)
        druidResult.head should be ("""{"date":"2020-03-13","count":9.0,"dialcode_slug":"Andaman & Nicobar Islands"}""")
    }

    it should "fetch data for GroupBy dimension with Lookup and replaceMissingValue as Unknown" in {
        val lookupQuery = DruidQueryModel("groupBy", "telemetry-events", "2020-05-08T00:00:00+00:00/2020-05-15T00:00:00+00:00", Option("all"),
            Option(List(Aggregation(Option("count"), "count", "count"))),
            Option(List(DruidDimension("derived_loc_state", Option("state_slug"), Option("extraction"), None,
                Option(List(ExtractFn("registeredlookup", "lookup_state", None, Option("Unknown"))))), DruidDimension("derived_loc_district", Option("district_slug"), Option("extraction"), None,
                Option(List(ExtractFn("registeredlookup", "lookup_district", None, Option("Unknown"))))))))

        val query = DruidDataFetcher.getDruidQuery(lookupQuery)
        query.toString should be("GroupByQuery(List(CountAggregation(count)),List(2020-05-08T00:00:00+00:00/2020-05-15T00:00:00+00:00),None,List(ExtractionDimension(derived_loc_state,Some(state_slug),None,RegisteredLookupExtractionFn(lookup_state,None,Some(Unknown))), ExtractionDimension(derived_loc_district,Some(district_slug),None,RegisteredLookupExtractionFn(lookup_district,None,Some(Unknown)))),All,None,None,List(),Map())")

        val json = """{"district_slug":"Andamans","state_slug":"Andaman & Nicobar Islands","count":138.0,"date":"2020-05-08"}"""
        val doc: Json = parse(json).getOrElse(Json.Null);
        val results = List(DruidResult.apply(Some(ZonedDateTime.of(2020, 3, 1, 0, 0, 0, 0, ZoneOffset.UTC)), doc));
        val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)

        implicit val mockFc = mock[FrameworkContext];
        implicit val druidConfig = mock[DruidConfig];
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(query, *).returns(Future(druidResponse)).anyNumberOfTimes()
        (mockFc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes();
        (mockFc.getDruidRollUpClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes();

        val druidResult = DruidDataFetcher.getDruidData(lookupQuery).collect()
        druidResult.size should be (1)
        druidResult.head should be ("""{"district_slug":"Andamans","state_slug":"Andaman & Nicobar Islands","count":138.0,"date":"2020-03-01"}""")
    }

    it should "fetch data for filtered aggregation" in {
        val scansQuery = DruidQueryModel("groupBy", "summary-distinct-counts", "2020-05-12/2020-05-13", Option("all"), Option(List(Aggregation(Option("total_failed_scans"), "filtered", "total_count", None, None, None, None, None, None, Option("longSum"), Option("edata_size"), Option(0.asInstanceOf[AnyRef])))), Option(List(DruidDimension("derived_loc_state", Option("state")), DruidDimension("derived_loc_district", Option("district")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))), DruidFilter("isnotnull", "derived_loc_district", None))))
        val druidQuery = DruidDataFetcher.getDruidQuery(scansQuery)
        druidQuery.toString should be ("GroupByQuery(List(SelectorFilteredAggregation(total_failed_scans,SelectFilter(edata_size,Some(0),None),LongSumAggregation(total_failed_scans,total_count))),List(2020-05-12/2020-05-13),Some(AndFilter(List(InFilter(dimensions_pdata_id,List(prod.diksha.app, prod.diksha.portal),None), NotFilter(SelectFilter(derived_loc_district,None,None))))),List(DefaultDimension(derived_loc_state,Some(state),None), DefaultDimension(derived_loc_district,Some(district),None)),All,None,None,List(),Map())")

        val json = """{"state":"Andaman & Nicobar Islands","total_failed_scans":10,"date":"2020-03-01","district":"Ahmednagar"}"""
        val doc: Json = parse(json).getOrElse(Json.Null);
        val results = List(DruidResult.apply(Some(ZonedDateTime.of(2020, 3, 1, 0, 0, 0, 0, ZoneOffset.UTC)), doc));
        val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)

        implicit val mockFc = mock[FrameworkContext];
        implicit val druidConfig = mock[DruidConfig];
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.doQuery[DruidResponse](_:DruidQuery)(_:DruidConfig)).expects(druidQuery, *).returns(Future(druidResponse))
        (mockFc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes();
        (mockFc.getDruidRollUpClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes();

        val druidResult = DruidDataFetcher.getDruidData(scansQuery).collect()
        druidResult.size should be (1)
        druidResult.head should be ("""{"state":"Andaman & Nicobar Islands","total_failed_scans":10.0,"date":"2020-03-01","district":"Ahmednagar"}""")
    }

    it should "give result for stream query" in {
        val query = DruidQueryModel("groupBy", "telemetry-events", "2019-11-01/2019-11-02", Option("all"), Option(List(Aggregation(Option("count"), "count", ""),Aggregation(Option("total_duration"), "doubleSum", "edata_duration"))), Option(List(DruidDimension("context_pdata_id", Option("producer_id")), DruidDimension("context_pdata_pid", Option("producer_pid")))), Option(List(DruidFilter("in", "eid", None, Option(List("START", "END"))))), Option(DruidHavingFilter("lessThan", "doubleSum", 20.asInstanceOf[AnyRef])), Option(List(PostAggregation("arithmetic", "Addition", PostAggregationFields("field", ""), "+"))))
        val druidQuery = DruidDataFetcher.getDruidQuery(query)
        druidQuery.toString() should be ("GroupByQuery(List(CountAggregation(count), DoubleSumAggregation(total_duration,edata_duration)),List(2019-11-01/2019-11-02),Some(AndFilter(List(InFilter(eid,List(START, END),None)))),List(DefaultDimension(context_pdata_id,Some(producer_id),None), DefaultDimension(context_pdata_pid,Some(producer_pid),None)),All,Some(LessThanHaving(doubleSum,20.0)),None,List(ArithmeticPostAggregation(Addition,PLUS,List(FieldAccessPostAggregation(field,None), FieldAccessPostAggregation(,None)),Some(FloatingPoint))),Map())")

        val json: String = """
          {
              "total_scans" : 9007,
              "producer_id" : "dev.sunbird.learning.platform"
          }
        """
        val doc: Json = parse(json).getOrElse(Json.Null);
        val results = List(DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc));
        val druidResponse = DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc)

        implicit val mockFc = mock[FrameworkContext];
        implicit val druidConfig = mock[DruidConfig];
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.actorSystem _).expects().returning(ActorSystem("TestQuery")).anyNumberOfTimes()
        (mockDruidClient.doQueryAsStream(_:DruidQuery)(_:DruidConfig)).expects(druidQuery, *).returns(Source(List(druidResponse))).anyNumberOfTimes()
        (mockFc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes();

        val druidResult = DruidDataFetcher.getDruidData(query,true).collect()

        druidResult.size should be (1)
        druidResult.head should be ("""{"total_scans":9007.0,"producer_id":"dev.sunbird.learning.platform","date":"2019-11-28"}""")
    }

    it should "give result for stream topn query" in {
        val query = DruidQueryModel("topN", "telemetry-events", "2020-03-12T00:00:00+00:00/2020-05-12T00:00:00+00:00", Option("all"),
            Option(List(Aggregation(Option("count"), "count", "count"))),
            Option(List(DruidDimension("dialcode_channel", Option("dialcode_slug"), Option("extraction"), None,
                Option(List(ExtractFn("registeredlookup", "channel")))))),
            Option(List(DruidFilter("equals", "dialcode_channel", Option("012315809814749184151")))), None, None,None, None,None, None, Option("count"))
        val druidQuery = DruidDataFetcher.getDruidQuery(query)

        val json = """[{"date":"2020-03-13","count":9,"dialcode_slug":"Andaman & Nicobar Islands"}]"""
        val doc: Json = parse(json).getOrElse(Json.Null);
        val results = List(DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc));
        val druidResponse = DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc)

        implicit val mockFc = mock[FrameworkContext];
        implicit val druidConfig = mock[DruidConfig];
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.actorSystem _).expects().returning(ActorSystem("TestQuery")).anyNumberOfTimes()
        (mockDruidClient.doQueryAsStream(_:DruidQuery)(_:DruidConfig)).expects(druidQuery, *).returns(Source(List(druidResponse))).anyNumberOfTimes()
        (mockFc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes();

        val druidResult = DruidDataFetcher.getDruidData(query,true).collect()

        druidResult.size should be (1)
        druidResult.head should be ("""{"date":"2020-03-13","count":9.0,"dialcode_slug":"Andaman & Nicobar Islands"}""")
    }

    it should "test scan query with stream" in {

        val query = DruidQueryModel("scan", "summary-rollup-syncts", "2020-03-12T00:00:00+00:00/2020-03-13T00:00:00+00:00", Option("all"),
            None, None, None, None, None,Option(List("derived_loc_state","derived_loc_district")), None, None)
        val druidQuery = DruidDataFetcher.getDruidQuery(query)

        val json = """{"__time":1583971200000,"derived_loc_state":"unknown","derived_loc_district":"unknown","date":"2019-03-12"}"""
        val json1 = """{"__time":1583971200000,"derived_loc_state":"ka","derived_loc_district":"unknown","date":"2019-03-12"}"""
        val json2 = """{"__time":1583971200000,"derived_loc_state":"apekx","derived_loc_district":"Vizag","date":"2019-03-12"}"""
        val doc: Json = parse(json).getOrElse(Json.Null);
        val doc1: Json = parse(json1).getOrElse(Json.Null)
        val doc2: Json = parse(json2).getOrElse(Json.Null)
        val druidResponse = DruidScanResult.apply(doc)
        val druidResponse1 = DruidScanResult.apply(doc1)
        val druidResponse2 = DruidScanResult.apply(doc2)
        implicit val mockFc = mock[FrameworkContext];
        implicit val druidConfig = mock[DruidConfig];
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.actorSystem _).expects().returning(ActorSystem("TestQuery")).anyNumberOfTimes()
        (mockDruidClient.doQueryAsStream(_:DruidQuery)(_:DruidConfig)).expects(druidQuery, *).returns(Source(List(druidResponse,druidResponse1,druidResponse2))).anyNumberOfTimes()
        (mockFc.getDruidRollUpClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes();

        val druidResult = DruidDataFetcher.getDruidData(query,true).collect()

        druidResult.size should be (3)
        druidResult.head should be ("""{"__time":"1583971200000.0","derived_loc_state":"unknown","derived_loc_district":"unknown","date":"2020-03-12"}""")

    }

    it should "test scan query without stream" in {

        val query = DruidQueryModel("scan", "summary-events", "2020-03-12T00:00:00+00:00/2020-03-13T00:00:00+00:00", Option("all"),
            None, None, Option(List(DruidFilter("in", "eid", None, Option(List("START", "END"))))), None, None,None, None, None)
        val druidQuery = DruidDataFetcher.getDruidQuery(query)
        val json = """{"__time":1583971200000,"derived_loc_state":"unknown","derived_loc_district":"unknown","date":"2019-03-12","created_for": null,"active":true}"""
        val doc: Json = parse(json).getOrElse(Json.Null)
        val results = List(DruidScanResult.apply(doc));
        val scanresults = DruidScanResults.apply("122",List("derived_loc_state","derived_loc_district","active"),results)
        val druidResponse = DruidScanResponse.apply(List(scanresults))
        implicit val mockFc = mock[FrameworkContext]
        implicit val druidConfig = mock[DruidConfig]
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.actorSystem _).expects().returning(ActorSystem("TestQuery")).anyNumberOfTimes()
        (mockDruidClient.doQuery[DruidResponse](_:DruidNativeQuery)(_:DruidConfig)).expects(druidQuery, *).returns(Future(druidResponse)).anyNumberOfTimes()
        (mockFc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes();

        val druidResult = DruidDataFetcher.getDruidData(query).collect()

        druidResult.size should be (1)
        druidResult.head should be (
            """{"created_for":"unknown","derived_loc_state":"unknown","__time":"1583971200000.0","date":"2020-03-12","derived_loc_district":"unknown","active":true}""".stripMargin)

    }

    it should "test query with stream with empty results" in {
        val query = DruidQueryModel("groupBy", "telemetry-events", "2019-11-01/2019-11-02", Option("all"), Option(List(Aggregation(Option("count"), "count", ""),Aggregation(Option("total_duration"), "doubleSum", "edata_duration"))), Option(List(DruidDimension("context_pdata_id", Option("producer_id")), DruidDimension("context_pdata_pid", Option("producer_pid")))), Option(List(DruidFilter("in", "eid", None, Option(List("START", "END"))))), Option(DruidHavingFilter("lessThan", "doubleSum", 20.asInstanceOf[AnyRef])), Option(List(PostAggregation("arithmetic", "Addition", PostAggregationFields("field", ""), "+"))))
        val druidQuery = DruidDataFetcher.getDruidQuery(query)
        druidQuery.toString() should be ("GroupByQuery(List(CountAggregation(count), DoubleSumAggregation(total_duration,edata_duration)),List(2019-11-01/2019-11-02),Some(AndFilter(List(InFilter(eid,List(START, END),None)))),List(DefaultDimension(context_pdata_id,Some(producer_id),None), DefaultDimension(context_pdata_pid,Some(producer_pid),None)),All,Some(LessThanHaving(doubleSum,20.0)),None,List(ArithmeticPostAggregation(Addition,PLUS,List(FieldAccessPostAggregation(field,None), FieldAccessPostAggregation(,None)),Some(FloatingPoint))),Map())")

        val json: String = """
          {
          }
        """
        val doc: Json = parse(json).getOrElse(Json.Null);
        implicit val mockFc = mock[FrameworkContext];
        implicit val druidConfig = mock[DruidConfig];
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.actorSystem _).expects().returning(ActorSystem("TestQuery")).anyNumberOfTimes()
        (mockDruidClient.doQueryAsStream(_:DruidQuery)(_:DruidConfig)).expects(druidQuery, *).returns(Source(List())).anyNumberOfTimes()
        (mockFc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes();

        val druidResult = DruidDataFetcher.getDruidData(query,true).collect()

        druidResult.size should be (0)
    }

    it should "test sql query " in {

        val sqlQuery = DruidQueryModel("scan", "summary-rollup-syncts", "2020-08-23T00:00:00+00:00/2020-08-24T00:00:00+00:00", Option("all"),
            None, None, None, None, None, None, Option(List(DruidSQLDimension("state",Option("LOOKUP(derived_loc_state, 'stateSlugLookup')")),
                DruidSQLDimension("dimensions_pdata_id",None))),None)


        implicit val mockFc = mock[FrameworkContext];
        implicit val druidConfig = mock[DruidConfig];


        val mockAKkaUtil = mock[AkkaHttpClient]
        val url = String.format("%s://%s:%s%s%s", "http",AppConf.getConfig("druid.rollup.host"),
            AppConf.getConfig("druid.rollup.port"),AppConf.getConfig("druid.url"),"sql")
        val request = HttpRequest(method = HttpMethods.POST,
            uri = url,
            entity = HttpEntity(ContentTypes.`application/json`, JSONUtils.serialize(DruidDataFetcher.getSQLDruidQuery(sqlQuery))))
        val stripString =
            """{"dimensions_pdata_id":"", "state":10}
              {"dimensions_pdata_id":null, "state":5}
              |{"dimensions_pdata_id":"dev.portal", "state":5}""".stripMargin
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.actorSystem _).expects().returning(ActorSystem("TestQuery")).anyNumberOfTimes()
        (mockFc.getDruidRollUpClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes();
        (mockAKkaUtil.sendRequest(_: HttpRequest)(_: ActorSystem))
          .expects(request,mockDruidClient.actorSystem)
          .returns(Future.successful(HttpResponse(entity = HttpEntity(ByteString(stripString))))).anyNumberOfTimes();
        val response = DruidDataFetcher.executeSQLQuery(sqlQuery, mockAKkaUtil)
        response.count() should be (3)
    }

    "DruidDataFetcher" should "verify DruidOutput operations"  in {
        val json: String =
            """
          {
              "total_sessions" : 2000,
              "total_ts" : 5,
              "district" : "Nellore",
              "state" : "Andhra Pradesh"
          }
        """

        val output = new DruidOutput(JSONUtils.deserialize[Map[String,AnyRef]](json))
        output.size should be(4)
        val output2 =output + ("count" -> 1)
        output2.size should be(5)
        val output3 = output - ("count")
        output3.size should be(4)
        output3.get("total_ts").get should be(5)
    }


    it should "test the latest_index granularity" in {
        EmbeddedPostgresqlService.execute("INSERT INTO druid_segments (id,datasource,start,\"end\",used) VALUES('segment1','content-model-snapshot','2020-10-27T00:00:00.000Z','2020-10-28T00:00:00.000Z','t')")
        val query = DruidQueryModel("groupBy", "content-model-snapshot", "LastDay",
            Option("latest_index"), Option(List(Aggregation(Option("count"), "count", ""))),
            Option(List(DruidDimension("status", Option("status")))),
            None,None,None)
        val druidQuery = DruidDataFetcher.getDruidQuery(query)

        druidQuery.toDebugString.contains("2020-10-27T00:00:00.000Z") should be (true)
        druidQuery.toString() should be ("GroupByQuery(List(CountAggregation(count)),List(2020-10-27T00:00:00.000Z/2020-10-28T00:00:00.000Z),None,List(DefaultDimension(status,Some(status),None)),All,None,None,List(),Map())")
        val query1 = DruidQueryModel("groupBy", "content-snapshot", "2019-11-01/2019-11-02",
            Option("latest_index"), Option(List(Aggregation(Option("count"), "count", ""))),
            Option(List(DruidDimension("status", Option("status")))),
            None,None,None)
        val druidQuery1 = DruidDataFetcher.getDruidQuery(query1)
        druidQuery1.toDebugString.contains("2019-11-01") should be (true)

    }

    it should "execute sql query string" in {

        val sqlQueryModelStr = "{\"queryType\":\"sql\",\"dataSource\":\"summary-rollup-syncts\",\"intervals\":\"2021-01-01T00:00:00+00:00/2021-01-02T00:00:00+00:00\",\"granularity\":\"all\",\"sqlQueryStr\":\"WITH\\\"course_data\\\"AS(SELECT\\\"actor_id\\\",\\\"object_rollup_l1\\\",\\\"derived_loc_state\\\",\\\"derived_loc_district\\\"FROM\\\"audit-rollup-syncts\\\"WHERE\\\"__time\\\"BETWEEN TIMESTAMP '%s' AND TIMESTAMP '%s' AND\\\"edata_type\\\"='enrol-complete')SELECT\\\"c1\\\".\\\"derived_loc_state\\\",\\\"c1\\\".\\\"derived_loc_district\\\",COUNT(DISTINCT\\\"c1\\\".\\\"actor_id\\\")AS\\\"completed_count\\\"FROM(SELECT\\\"derived_loc_state\\\",\\\"derived_loc_district\\\",\\\"actor_id\\\"FROMcourse_dataWHERE\\\"object_rollup_l1\\\"='do_31319773241548800012753')c1INNERJOIN(SELECT\\\"derived_loc_state\\\",\\\"derived_loc_district\\\",\\\"actor_id\\\"FROMcourse_dataWHERE\\\"object_rollup_l1\\\"='do_31319972203266048013575')AS\\\"c2\\\"ON\\\"c1\\\".\\\"actor_id\\\"=\\\"c2\\\".\\\"actor_id\\\"INNERJOIN(SELECT\\\"derived_loc_state\\\",\\\"derived_loc_district\\\",\\\"actor_id\\\"FROMcourse_dataWHERE\\\"object_rollup_l1\\\"='do_31314758133181644811384')AS\\\"c3\\\"ON\\\"c2\\\".\\\"actor_id\\\"=\\\"c3\\\".\\\"actor_id\\\"GROUPBY\\\"c1\\\".\\\"derived_loc_state\\\",\\\"c1\\\".\\\"derived_loc_district\\\"\",\"descending\":\"false\",\"intervalSlider\":0}"
        val sqlQueryModel = JSONUtils.deserialize[DruidQueryModel](sqlQueryModelStr)

        implicit val mockFc = mock[FrameworkContext];
        implicit val druidConfig = mock[DruidConfig];

        val mockAKkaUtil = mock[AkkaHttpClient]
        val url = String.format("%s://%s:%s%s%s", "http",AppConf.getConfig("druid.rollup.host"),
            AppConf.getConfig("druid.rollup.port"),AppConf.getConfig("druid.url"),"sql")
        val request = HttpRequest(method = HttpMethods.POST,
            uri = url,
            entity = HttpEntity(ContentTypes.`application/json`, JSONUtils.serialize(getSQLDruidQuery(sqlQueryModel))))
        val stripString =
            """{"derived_loc_state":"Karnataka","derived_loc_district":"Mysore","completed_count":4}""".stripMargin
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.actorSystem _).expects().returning(ActorSystem("TestQuery")).anyNumberOfTimes()
        (mockFc.getDruidRollUpClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes();
        (mockAKkaUtil.sendRequest(_: HttpRequest)(_: ActorSystem))
          .expects(request,mockDruidClient.actorSystem)
          .returns(Future.successful(HttpResponse(entity = HttpEntity(ByteString(stripString))))).anyNumberOfTimes();
        val response = DruidDataFetcher.executeSQLQuery(sqlQueryModel, mockAKkaUtil)
        response.count() should be (1)
    }

    ignore should "test sql join query without time interval" in {

        val sqlQueryModelStr = "{\"queryType\":\"sql\",\"dataSource\":\"summary-rollup-syncts\",\"intervals\":\"2021-01-01T00:00:00+00:00/2021-01-02T00:00:00+00:00\",\"granularity\":\"all\",\"sqlQueryStr\":\"WITH\\\"course_data\\\"AS(SELECT\\\"actor_id\\\",\\\"object_rollup_l1\\\",\\\"derived_loc_state\\\",\\\"derived_loc_district\\\"FROM\\\"audit-rollup-syncts\\\"WHERE\\\"__time\\\"BETWEENTIMESTAMP'2021-01-0100:00:00'ANDTIMESTAMP'2021-07-1523:00:00'AND\\\"edata_type\\\"='enrol-complete')SELECT\\\"c1\\\".\\\"derived_loc_state\\\",\\\"c1\\\".\\\"derived_loc_district\\\",COUNT(DISTINCT\\\"c1\\\".\\\"actor_id\\\")AS\\\"completed_count\\\"FROM(SELECT\\\"derived_loc_state\\\",\\\"derived_loc_district\\\",\\\"actor_id\\\"FROMcourse_dataWHERE\\\"object_rollup_l1\\\"='do_31319773241548800012753')c1INNERJOIN(SELECT\\\"derived_loc_state\\\",\\\"derived_loc_district\\\",\\\"actor_id\\\"FROMcourse_dataWHERE\\\"object_rollup_l1\\\"='do_31319972203266048013575')AS\\\"c2\\\"ON\\\"c1\\\".\\\"actor_id\\\"=\\\"c2\\\".\\\"actor_id\\\"INNERJOIN(SELECT\\\"derived_loc_state\\\",\\\"derived_loc_district\\\",\\\"actor_id\\\"FROMcourse_dataWHERE\\\"object_rollup_l1\\\"='do_31314758133181644811384')AS\\\"c3\\\"ON\\\"c2\\\".\\\"actor_id\\\"=\\\"c3\\\".\\\"actor_id\\\"GROUPBY\\\"c1\\\".\\\"derived_loc_state\\\",\\\"c1\\\".\\\"derived_loc_district\\\"\",\"descending\":\"false\",\"intervalSlider\":0}"
        val sqlQueryModel = JSONUtils.deserialize[DruidQueryModel](sqlQueryModelStr)

        implicit val mockFc = mock[FrameworkContext];
        implicit val druidConfig = mock[DruidConfig];

        val mockAKkaUtil = mock[AkkaHttpClient]
        val url = String.format("%s://%s:%s%s%s", "http",AppConf.getConfig("druid.rollup.host"),
          AppConf.getConfig("druid.rollup.port"),AppConf.getConfig("druid.url"),"sql")
        val request = HttpRequest(method = HttpMethods.POST,
          uri = url,
          entity = HttpEntity(ContentTypes.`application/json`, JSONUtils.serialize(DruidSQLQuery(sqlQueryModel.sqlQueryStr.get))))
        val stripString =
          """{"derived_loc_state":"Karnataka","derived_loc_district":"Mysore","completed_count":4}""".stripMargin
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.actorSystem _).expects().returning(ActorSystem("TestQuery")).anyNumberOfTimes()
        (mockFc.getDruidRollUpClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes();
        (mockAKkaUtil.sendRequest(_: HttpRequest)(_: ActorSystem))
          .expects(request,mockDruidClient.actorSystem)
          .returns(Future.successful(HttpResponse(entity = HttpEntity(ByteString(stripString))))).anyNumberOfTimes();

        a[Exception] should be thrownBy {
          DruidDataFetcher.executeSQLQuery(sqlQueryModel, mockAKkaUtil)
        }
    }

    it should "test for scientific notation format for BigDecimal values" in {

      val query = DruidQueryModel("groupBy", "telemetry-rollup-events", "2019-11-01/2019-11-02", Option("all"), Option(List(Aggregation(Option("count"), "count", ""),Aggregation(Option("total_duration"), "doubleSum", "edata_duration"))), Option(List(DruidDimension("context_pdata_id", Option("producer_id")), DruidDimension("context_pdata_pid", Option("producer_pid")))), Option(List(DruidFilter("in", "eid", None, Option(List("START", "END"))))), Option(DruidHavingFilter("lessThan", "doubleSum", 20.asInstanceOf[AnyRef])), Option(List(PostAggregation("arithmetic", "Addition", PostAggregationFields("field", ""), "+"))))
      val druidQuery = DruidDataFetcher.getDruidQuery(query)
      druidQuery.toString() should be ("GroupByQuery(List(CountAggregation(count), DoubleSumAggregation(total_duration,edata_duration)),List(2019-11-01/2019-11-02),Some(AndFilter(List(InFilter(eid,List(START, END),None)))),List(DefaultDimension(context_pdata_id,Some(producer_id),None), DefaultDimension(context_pdata_pid,Some(producer_pid),None)),All,Some(LessThanHaving(doubleSum,20.0)),None,List(ArithmeticPostAggregation(Addition,PLUS,List(FieldAccessPostAggregation(field,None), FieldAccessPostAggregation(,None)),Some(FloatingPoint))),Map())")

      val json: String = """
            {
                "total_scans" : 1.20905875E+08,
                "total_count" : 1209058,
                "producer_id" : "dev.sunbird.learning.platform"
            }
          """
      val doc: Json = parse(json).getOrElse(Json.Null);
      val results = List(DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc));
      val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)

      implicit val mockFc = mock[FrameworkContext];
      implicit val druidConfig = mock[DruidConfig];
      val mockDruidClient = mock[DruidClient]
      (mockDruidClient.doQuery[DruidResponse](_:DruidQuery)(_:DruidConfig)).expects(druidQuery, *).returns(Future(druidResponse))
      (mockFc.getDruidRollUpClient: () => DruidClient).expects().returns(mockDruidClient);

      val druidResult = DruidDataFetcher.getDruidData(query).collect()

      druidResult.size should be (1)
      // total_scans will be converted to string as it has more than 8 digits
      // total_count will be numeric value
      druidResult.head should be ("""{"total_scans":"120905875.0","total_count":1209058.0,"producer_id":"dev.sunbird.learning.platform","date":"2019-11-28"}""")
    }


}
