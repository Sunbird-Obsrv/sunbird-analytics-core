package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{HTTPClient, JSONUtils}
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util.AppConfig
import org.joda.time.DateTime
import org.scalamock.scalatest.MockFactory

class TestUpdateContentRating extends SparkSpec(null) with MockFactory {

  implicit val fc = new FrameworkContext();
  "UpdateContentRating" should "get content list which are rated in given time" in {
    val startDate = new DateTime().minusDays(1).toString("yyyy-MM-dd")
    val endDate = new DateTime().toString("yyyy-MM-dd")
    val mockRestUtil = mock[HTTPClient]
    (mockRestUtil.post[List[Map[String, AnyRef]]](_: String, _: String, _: Option[Map[String, String]])(_: Manifest[List[Map[String, AnyRef]]]))
      .expects("http://localhost:8082/druid/v2/sql/", AppConf.getConfig("druid.unique.content.query").format(new DateTime(startDate).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss"), new DateTime(endDate).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss")), None, manifest[List[Map[String, AnyRef]]])
      .returns(List(Map("ContentId" -> "test-1"), Map("ContentId" -> "test-2")))

    val contentIds = UpdateContentRating.getRatedContents(Map("startDate" -> startDate.asInstanceOf[AnyRef], "endDate" -> endDate.asInstanceOf[AnyRef]), mockRestUtil)
    contentIds.size should be(2)

    UpdateContentRating.execute(sc.emptyRDD, None);
    val metrics1 = sc.parallelize(List(ContentMetrics("test-1",Some(100), Some(20.0), None, None, Some(32432), None, Some(53), Some(552))))
    UpdateContentRating.postProcess(metrics1, Map())

    // check for empty/null content id
    val metrics2 = sc.parallelize(List(ContentMetrics("",Some(100), Some(20.0), None, None, Some(32432), None, Some(53), Some(552))))
    UpdateContentRating.postProcess(metrics2, Map())
  }

  it should "alter end date for replay scenario" in {

    val startDate = new DateTime().minusDays(1).toString("yyyy-MM-dd")
    val endDate = new DateTime().toString("yyyy-MM-dd")
    val mockRestUtil = mock[HTTPClient]
    (mockRestUtil.post[List[Map[String, AnyRef]]](_: String, _: String, _: Option[Map[String, String]])(_: Manifest[List[Map[String, AnyRef]]]))
      .expects("http://localhost:8082/druid/v2/sql/", AppConf.getConfig("druid.unique.content.query").format(new DateTime(startDate).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss"), new DateTime(endDate).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss")), None, manifest[List[Map[String, AnyRef]]])
      .returns(List(Map("ContentId" -> "test-1"), Map("ContentId" -> "test-2")))

    val contentIds = UpdateContentRating.getRatedContents(Map("startDate" -> startDate.asInstanceOf[AnyRef], "endDate" -> startDate.asInstanceOf[AnyRef]), mockRestUtil)
    contentIds.size should be(2)
  }

  it should "get all content ratings" in {
    val mockRestUtil = mock[HTTPClient]
    (mockRestUtil.post[List[Map[String, AnyRef]]](_: String, _: String, _: Option[Map[String, String]])(_: Manifest[List[Map[String, AnyRef]]]))
      .expects("http://localhost:8082/druid/v2/sql/", AppConfig.getConfig("druid.content.rating.query"), None, manifest[List[Map[String, AnyRef]]])
      .returns(List(Map("contentId" -> "test-1", "totalRatingsCount" -> 25.asInstanceOf[AnyRef], "Number of Ratings" -> 5.asInstanceOf[AnyRef], "averageRating" -> 5.asInstanceOf[AnyRef]), Map("contentId" -> "test-2", "averageRating" -> 3.asInstanceOf[AnyRef])))


    val contentRatings = UpdateContentRating.getContentMetrics(mockRestUtil, AppConfig.getConfig("druid.content.rating.query"))
    contentRatings.count() should be(2)
    contentRatings.take(1).map(x => {
      x.contentId should be("test-1")
      x.totalRatingsCount.get should be(25)
      x.averageRating.get should be(5.0)
    })
  }

  it should "get all content consuption metrics" in {
    val mockRestUtil = mock[HTTPClient]
    val response = "[{\"play_sessions_count\":1,\"total_time_spent\":152.34,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"KP_FT_1576559515069\"},{\"play_sessions_count\":2,\"total_time_spent\":1.43,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"KP_FT_1576559537277\"},{\"play_sessions_count\":2,\"total_time_spent\":11.879999999999999,\"dimensions_pdata_id\":\"local.sunbird.desktop\",\"contentId\":\"KP_FT_1576560445791\"},{\"play_sessions_count\":1,\"total_time_spent\":1.6,\"dimensions_pdata_id\":\"local.sunbird.desktop\",\"contentId\":\"KP_FT_1576560467255\"},{\"play_sessions_count\":1,\"total_time_spent\":203.72,\"dimensions_pdata_id\":\"local.sunbird.desktop\",\"contentId\":\"KP_FT_1576560489699\"},{\"play_sessions_count\":1,\"total_time_spent\":0.01,\"dimensions_pdata_id\":\"local.sunbird.desktop\",\"contentId\":\"do_112777871922569216119\"},{\"play_sessions_count\":1,\"total_time_spent\":7.84,\"dimensions_pdata_id\":\"prod.sunbird.desktop\",\"contentId\":\"do_112777871922569216119\"},{\"play_sessions_count\":1,\"total_time_spent\":716.02,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_112835336960000000152\"},{\"play_sessions_count\":1,\"total_time_spent\":5.02,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_11285800480340377611027\"},{\"play_sessions_count\":1,\"total_time_spent\":0.38,\"dimensions_pdata_id\":\"prod.sunbird.desktop\",\"contentId\":\"do_20045067\"},{\"play_sessions_count\":1,\"total_time_spent\":0.2,\"dimensions_pdata_id\":\"prod.sunbird.desktop\",\"contentId\":\"do_20045289\"},{\"play_sessions_count\":2,\"total_time_spent\":191.94,\"dimensions_pdata_id\":\"prod.sunbird.desktop\",\"contentId\":\"do_2122161222933299201262\"},{\"play_sessions_count\":2,\"total_time_spent\":3.75,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2122432175175761921186\"},{\"play_sessions_count\":1,\"total_time_spent\":38.66,\"dimensions_pdata_id\":\"staging.sunbirda.app\",\"contentId\":\"do_212292951494197248197\"},{\"play_sessions_count\":1,\"total_time_spent\":1.05,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2122951778195865601246\"},{\"play_sessions_count\":3,\"total_time_spent\":5.65,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2122952599231856641325\"},{\"play_sessions_count\":4,\"total_time_spent\":6.0600000000000005,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_212295902628782080167\"},{\"play_sessions_count\":7,\"total_time_spent\":20.980000000000004,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_212295908607836160179\"},{\"play_sessions_count\":12,\"total_time_spent\":384.71000000000004,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_212295919143411712182\"},{\"play_sessions_count\":1,\"total_time_spent\":3.85,\"dimensions_pdata_id\":\"staging.sunbirda.portal\",\"contentId\":\"do_212295919143411712182\"},{\"play_sessions_count\":1,\"total_time_spent\":272.8,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_212297326962860032139\"},{\"play_sessions_count\":1,\"total_time_spent\":610.92,\"dimensions_pdata_id\":\"staging.sunbirda.portal\",\"contentId\":\"do_212297326962860032139\"},{\"play_sessions_count\":1,\"total_time_spent\":32.17,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_212305836974661632137\"},{\"play_sessions_count\":1,\"total_time_spent\":41.91,\"dimensions_pdata_id\":\"staging.sunbirda.portal\",\"contentId\":\"do_212305836974661632137\"},{\"play_sessions_count\":1,\"total_time_spent\":103.72,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2123164671596052481388\"},{\"play_sessions_count\":4,\"total_time_spent\":14.96,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2123208084795310081772\"},{\"play_sessions_count\":1,\"total_time_spent\":1.11,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2123208084832829441775\"},{\"play_sessions_count\":2,\"total_time_spent\":4.1,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2123208084868300801779\"},{\"play_sessions_count\":1,\"total_time_spent\":2.63,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2123229899264573441612\"},{\"play_sessions_count\":1,\"total_time_spent\":1.41,\"dimensions_pdata_id\":\"staging.sunbirda.portal\",\"contentId\":\"do_2123229899264573441612\"},{\"play_sessions_count\":1,\"total_time_spent\":2.47,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2123229899293982721615\"},{\"play_sessions_count\":1,\"total_time_spent\":3.28,\"dimensions_pdata_id\":\"staging.sunbirda.portal\",\"contentId\":\"do_2123277680390389761198\"},{\"play_sessions_count\":1,\"total_time_spent\":545.62,\"dimensions_pdata_id\":\"staging.sunbirda.portal\",\"contentId\":\"do_2123278066849628161264\"},{\"play_sessions_count\":2,\"total_time_spent\":519.41,\"dimensions_pdata_id\":\"prod.sunbird.desktop\",\"contentId\":\"do_2123278267024343041278\"},{\"play_sessions_count\":2,\"total_time_spent\":8.67,\"dimensions_pdata_id\":\"local.sunbird.desktop\",\"contentId\":\"do_2123320947346391041175\"},{\"play_sessions_count\":5,\"total_time_spent\":26.720000000000002,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_212343330163007488137\"},{\"play_sessions_count\":4,\"total_time_spent\":897.3600000000001,\"dimensions_pdata_id\":\"prod.sunbird.desktop\",\"contentId\":\"do_212343332329766912138\"},{\"play_sessions_count\":8,\"total_time_spent\":388.76,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_212343332329766912138\"},{\"play_sessions_count\":2,\"total_time_spent\":3.66,\"dimensions_pdata_id\":\"local.sunbird.desktop\",\"contentId\":\"do_2123476114861506561132\"},{\"play_sessions_count\":1,\"total_time_spent\":4.8,\"dimensions_pdata_id\":\"local.sunbird.desktop\",\"contentId\":\"do_212351887955853312146\"},{\"play_sessions_count\":1,\"total_time_spent\":0.24,\"dimensions_pdata_id\":\"prod.sunbird.desktop\",\"contentId\":\"do_212361708511485952140\"},{\"play_sessions_count\":4,\"total_time_spent\":49.3,\"dimensions_pdata_id\":\"local.sunbird.desktop\",\"contentId\":\"do_2123618302937989121631\"},{\"play_sessions_count\":1,\"total_time_spent\":53.71,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2123618302937989121631\"},{\"play_sessions_count\":1,\"total_time_spent\":1.18,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_212371257097060352128\"},{\"play_sessions_count\":1,\"total_time_spent\":238.77,\"dimensions_pdata_id\":\"staging.sunbirda.app\",\"contentId\":\"do_2123795092172636161757\"},{\"play_sessions_count\":2,\"total_time_spent\":24.770000000000003,\"dimensions_pdata_id\":\"local.sunbird.desktop\",\"contentId\":\"do_212380089244606464116\"},{\"play_sessions_count\":1,\"total_time_spent\":1.11,\"dimensions_pdata_id\":\"staging.sunbirda.portal\",\"contentId\":\"do_2124121620945059841219\"},{\"play_sessions_count\":3,\"total_time_spent\":60.510000000000005,\"dimensions_pdata_id\":\"staging.sunbirda.portal\",\"contentId\":\"do_2124339505053450241746\"},{\"play_sessions_count\":2,\"total_time_spent\":120.53,\"dimensions_pdata_id\":\"staging.sunbirda.portal\",\"contentId\":\"do_2124347141266391041106\"},{\"play_sessions_count\":1,\"total_time_spent\":2.92,\"dimensions_pdata_id\":\"prod.sunbird.desktop\",\"contentId\":\"do_21244095469408256011169\"},{\"play_sessions_count\":1,\"total_time_spent\":8.13,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_212459391424782336145\"},{\"play_sessions_count\":1,\"total_time_spent\":113.38,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2124700100688035841261\"},{\"play_sessions_count\":1,\"total_time_spent\":315.97,\"dimensions_pdata_id\":\"staging.sunbirda.app\",\"contentId\":\"do_2124708442401751041124\"},{\"play_sessions_count\":1,\"total_time_spent\":4.34,\"dimensions_pdata_id\":\"staging.sunbirda.portal\",\"contentId\":\"do_2124708443414036481125\"},{\"play_sessions_count\":1,\"total_time_spent\":2.56,\"dimensions_pdata_id\":\"staging.sunbirda.portal\",\"contentId\":\"do_2124742691037429761195\"},{\"play_sessions_count\":2,\"total_time_spent\":2.5500000000000003,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2124841348376985601751\"},{\"play_sessions_count\":2,\"total_time_spent\":2.34,\"dimensions_pdata_id\":\"prod.sunbird.desktop\",\"contentId\":\"do_2124905130588651521376\"},{\"play_sessions_count\":1,\"total_time_spent\":6.01,\"dimensions_pdata_id\":\"staging.sunbirda.app\",\"contentId\":\"do_21249344488375091213752\"},{\"play_sessions_count\":1,\"total_time_spent\":1.39,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2124948145232855041166\"},{\"play_sessions_count\":2,\"total_time_spent\":58.879999999999995,\"dimensions_pdata_id\":\"staging.sunbirda.app\",\"contentId\":\"do_21249542373921587211269\"},{\"play_sessions_count\":1,\"total_time_spent\":18.64,\"dimensions_pdata_id\":\"staging.sunbirda.app\",\"contentId\":\"do_21249558183120896011342\"},{\"play_sessions_count\":1,\"total_time_spent\":1.27,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_21249981114087014412566\"}]"

    (mockRestUtil.post[List[Map[String, AnyRef]]](_: String, _: String, _: Option[Map[String, String]])(_: Manifest[List[Map[String, AnyRef]]]))
      .expects("http://localhost:8082/druid/v2/sql/", AppConfig.getConfig("druid.content.consumption.query"), None, manifest[List[Map[String, AnyRef]]])
      .returns(JSONUtils.deserialize[List[Map[String, AnyRef]]](response))

    val contentConsumption = UpdateContentRating.getContentMetrics(mockRestUtil, AppConfig.getConfig("druid.content.consumption.query"))
    contentConsumption.count() should be(62)
    contentConsumption.take(1).map(x => {
      x.contentId should be("KP_FT_1576559515069")
      x.totalTimeSpentInDeskTop.get should be(152)
      x.totalPlaySessionCountInDeskTop.get should be(1)
    })
  }

  it should "check for system update API call" in {

    val mockRestUtil = mock[HTTPClient]
    val mockResponse =
      s"""
         |{
         |    "id": "ekstep.learning.system.content.update",
         |    "ver": "1.0",
         |    "ts": "2019-05-02T12:20:17ZZ",
         |    "params": {
         |        "resmsgid": "622ade80-e22a-4cc1-8683-d002babe9ae6",
         |        "msgid": null,
         |        "err": null,
         |        "status": "successful",
         |        "errmsg": null
         |    },
         |    "responseCode": "OK",
         |    "result": {
         |        "node_id": "test-1",
         |        "versionKey": "1554515533414"
         |    }
         |}
           """.stripMargin

    (mockRestUtil.patch[String](_: String, _: String, _: Option[Map[String, String]])(_: Manifest[String]))
      .expects("http://localhost:8080/learning-service/system/v3/content/update/test-1/test-1", "{\"request\":{\"content\":{\"me_totalRatingsCount\":100,\"me_averageRating\":20.0,\"me_totalTimeSpentInSec\":{\"desktop\":32432},\"me_totalPlaySessionCount\":{\"portal\":53,\"desktop\":552}}}}", None, manifest[String])
      .returns(mockResponse)
    val response = UpdateContentRating.publishMetricsToContentModel(
      ContentMetrics("test-1",
        Some(100), Some(20.0), None, None, Some(32432), None, Some(53), Some(552)
      ), "http://localhost:8080/learning-service/system/v3/content/update/test-1", mockRestUtil)
    response.params.status.getOrElse("") should be("successful")
    response.result.getOrElse("node_id", "") should be("test-1")

  }

  it should "check for system update API call with empty maps for timespent and play sessions" in {

    val mockRestUtil = mock[HTTPClient]
    val mockResponse =
      s"""
         |{
         |    "id": "ekstep.learning.system.content.update",
         |    "ver": "1.0",
         |    "ts": "2019-05-02T12:20:17ZZ",
         |    "params": {
         |        "resmsgid": "622ade80-e22a-4cc1-8683-d002babe9ae6",
         |        "msgid": null,
         |        "err": null,
         |        "status": "successful",
         |        "errmsg": null
         |    },
         |    "responseCode": "OK",
         |    "result": {
         |        "node_id": "test-1",
         |        "versionKey": "1554515533414"
         |    }
         |}
           """.stripMargin

    (mockRestUtil.patch[String](_: String, _: String, _: Option[Map[String, String]])(_: Manifest[String]))
      .expects("http://localhost:8080/learning-service/system/v3/content/update/test-1/test-1", "{\"request\":{\"content\":{\"me_totalRatingsCount\":100,\"me_averageRating\":20.0}}}", None, manifest[String])
      .returns(mockResponse)
    val response = UpdateContentRating.publishMetricsToContentModel(
      ContentMetrics("test-1",
        Some(100), Some(20.0), None, None, None, None, None, None
      ), "http://localhost:8080/learning-service/system/v3/content/update/test-1", mockRestUtil)
    response.params.status.getOrElse("") should be("successful")
    response.result.getOrElse("node_id", "") should be("test-1")

  }

  it should "check for system update API call failure" in {
    val mockRestUtil = mock[HTTPClient]
    val mockResponse =
      s"""
         |{
         |    "id": "ekstep.learning.system.content.update",
         |    "ver": "1.0",
         |    "ts": "2019-05-14T14:43:36ZZ",
         |    "params": {
         |        "resmsgid": "c5369a14-eb6e-4eb0-9288-d4cb272d59b9",
         |        "msgid": null,
         |        "err": "ERR_GRAPH_UPDATE_NODE_VALIDATION_FAILED",
         |        "status": "failed",
         |        "errmsg": "Node Metadata validation failed"
         |    },
         |    "responseCode": "CLIENT_ERROR",
         |    "result": {
         |        "messages": [
         |            "Please provide framework.",
         |            "Metadata contentType should be one of: [Resource, Collection, TextBook, LessonPlan, Course, Template, Asset, Plugin, LessonPlanUnit, CourseUnit, TextBookUnit, TeachingMethod, PedagogyFlow]",
         |            "Metadata resourceType should be one of: [Read, Learn, Teach, Play, Test, Practice, Experiment, Collection, Book, Lesson Plan, Course, Theory, Worksheet, Practical]"
         |        ],
         |        "node_id": "org.ekstep.jun16.story.test05"
         |    }
         |}
           """.stripMargin


    (mockRestUtil.patch[String](_: String, _: String, _: Option[Map[String, String]])(_: Manifest[String]))
      .expects("http://localhost:8080/learning-service/system/v3/content/update/test-1", "{\"request\":{\"content\":{\"me_totalRatingsCount\":100,\"me_averageRating\":20.0,\"me_totalTimeSpentInSec\":{\"app\":122,\"portal\":432,\"desktop\":32432},\"me_totalPlaySessionCount\":{\"app\":33,\"portal\":53,\"desktop\":552}}}}", None, manifest[String])
      .returns(mockResponse)

    val response = UpdateContentRating.publishMetricsToContentModel(ContentMetrics("test-1", Some(100), Some(20), Some(122), Some(432), Some(32432), Some(33), Some(53), Some(552)), "http://localhost:8080/learning-service/system/v3/content/update", mockRestUtil)
    response.params.status.getOrElse("") should be("failed")
    response.result.getOrElse("node_id", "") should be("org.ekstep.jun16.story.test05")
  }

  it should "get the content consumption metrics" in {
    val mockRestUtil = mock[HTTPClient]
    val startDate = new DateTime().minusDays(1).toString("yyyy-MM-dd")
    val endDate = new DateTime().toString("yyyy-MM-dd")
    (mockRestUtil.post[List[Map[String, AnyRef]]](_: String, _: String, _: Option[Map[String, String]])(_: Manifest[List[Map[String, AnyRef]]]))
      .expects("http://localhost:8082/druid/v2/sql/", AppConfig.getConfig("druid.content.rating.query"), None, manifest[List[Map[String, AnyRef]]])
      .returns(List(Map("contentId" -> "test-1", "totalRatingsCount" -> 25.asInstanceOf[AnyRef], "Number of Ratings" -> 5.asInstanceOf[AnyRef], "averageRating" -> 5.asInstanceOf[AnyRef]), Map("contentId" -> "test-2", "averageRating" -> 3.asInstanceOf[AnyRef])))

    (mockRestUtil.post[List[Map[String, AnyRef]]](_: String, _: String, _: Option[Map[String, String]])(_: Manifest[List[Map[String, AnyRef]]]))
      .expects("http://localhost:8082/druid/v2/sql/", AppConf.getConfig("druid.unique.content.query").format(new DateTime(startDate).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss"), new DateTime(endDate).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss")), None, manifest[List[Map[String, AnyRef]]])
      .returns(List(Map("Id" -> "test-1"), Map("Id" -> "test-2")))


    (mockRestUtil.post[List[Map[String, AnyRef]]](_: String, _: String, _: Option[Map[String, String]])(_: Manifest[List[Map[String, AnyRef]]]))
      .expects("http://localhost:8082/druid/v2/sql/", AppConfig.getConfig("druid.content.consumption.query"), None, manifest[List[Map[String, AnyRef]]])
      .returns(JSONUtils.deserialize[List[Map[String, AnyRef]]]("[{\"play_sessions_count\":1,\"total_time_spent\":152.34,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"test-1\"},{\"play_sessions_count\":2,\"total_time_spent\":1.43,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"KP_FT_1576559537277\"},{\"play_sessions_count\":2,\"total_time_spent\":11.879999999999999,\"dimensions_pdata_id\":\"local.sunbird.desktop\",\"contentId\":\"KP_FT_1576560445791\"},{\"play_sessions_count\":1,\"total_time_spent\":1.6,\"dimensions_pdata_id\":\"local.sunbird.desktop\",\"contentId\":\"KP_FT_1576560467255\"},{\"play_sessions_count\":1,\"total_time_spent\":203.72,\"dimensions_pdata_id\":\"local.sunbird.desktop\",\"contentId\":\"KP_FT_1576560489699\"},{\"play_sessions_count\":1,\"total_time_spent\":0.01,\"dimensions_pdata_id\":\"local.sunbird.desktop\",\"contentId\":\"do_112777871922569216119\"},{\"play_sessions_count\":1,\"total_time_spent\":7.84,\"dimensions_pdata_id\":\"prod.sunbird.desktop\",\"contentId\":\"do_112777871922569216119\"},{\"play_sessions_count\":1,\"total_time_spent\":716.02,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_112835336960000000152\"},{\"play_sessions_count\":1,\"total_time_spent\":5.02,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_11285800480340377611027\"},{\"play_sessions_count\":1,\"total_time_spent\":0.38,\"dimensions_pdata_id\":\"prod.sunbird.desktop\",\"contentId\":\"do_20045067\"},{\"play_sessions_count\":1,\"total_time_spent\":0.2,\"dimensions_pdata_id\":\"prod.sunbird.desktop\",\"contentId\":\"do_20045289\"},{\"play_sessions_count\":2,\"total_time_spent\":191.94,\"dimensions_pdata_id\":\"prod.sunbird.desktop\",\"contentId\":\"do_2122161222933299201262\"},{\"play_sessions_count\":2,\"total_time_spent\":3.75,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2122432175175761921186\"},{\"play_sessions_count\":1,\"total_time_spent\":38.66,\"dimensions_pdata_id\":\"staging.sunbirda.app\",\"contentId\":\"do_212292951494197248197\"},{\"play_sessions_count\":1,\"total_time_spent\":1.05,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2122951778195865601246\"},{\"play_sessions_count\":3,\"total_time_spent\":5.65,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2122952599231856641325\"},{\"play_sessions_count\":4,\"total_time_spent\":6.0600000000000005,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_212295902628782080167\"},{\"play_sessions_count\":7,\"total_time_spent\":20.980000000000004,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_212295908607836160179\"},{\"play_sessions_count\":12,\"total_time_spent\":384.71000000000004,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_212295919143411712182\"},{\"play_sessions_count\":1,\"total_time_spent\":3.85,\"dimensions_pdata_id\":\"staging.sunbirda.portal\",\"contentId\":\"do_212295919143411712182\"},{\"play_sessions_count\":1,\"total_time_spent\":272.8,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_212297326962860032139\"},{\"play_sessions_count\":1,\"total_time_spent\":610.92,\"dimensions_pdata_id\":\"staging.sunbirda.portal\",\"contentId\":\"do_212297326962860032139\"},{\"play_sessions_count\":1,\"total_time_spent\":32.17,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_212305836974661632137\"},{\"play_sessions_count\":1,\"total_time_spent\":41.91,\"dimensions_pdata_id\":\"staging.sunbirda.portal\",\"contentId\":\"do_212305836974661632137\"},{\"play_sessions_count\":1,\"total_time_spent\":103.72,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2123164671596052481388\"},{\"play_sessions_count\":4,\"total_time_spent\":14.96,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2123208084795310081772\"},{\"play_sessions_count\":1,\"total_time_spent\":1.11,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2123208084832829441775\"},{\"play_sessions_count\":2,\"total_time_spent\":4.1,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2123208084868300801779\"},{\"play_sessions_count\":1,\"total_time_spent\":2.63,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2123229899264573441612\"},{\"play_sessions_count\":1,\"total_time_spent\":1.41,\"dimensions_pdata_id\":\"staging.sunbirda.portal\",\"contentId\":\"do_2123229899264573441612\"},{\"play_sessions_count\":1,\"total_time_spent\":2.47,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2123229899293982721615\"},{\"play_sessions_count\":1,\"total_time_spent\":3.28,\"dimensions_pdata_id\":\"staging.sunbirda.portal\",\"contentId\":\"do_2123277680390389761198\"},{\"play_sessions_count\":1,\"total_time_spent\":545.62,\"dimensions_pdata_id\":\"staging.sunbirda.portal\",\"contentId\":\"do_2123278066849628161264\"},{\"play_sessions_count\":2,\"total_time_spent\":519.41,\"dimensions_pdata_id\":\"prod.sunbird.desktop\",\"contentId\":\"do_2123278267024343041278\"},{\"play_sessions_count\":2,\"total_time_spent\":8.67,\"dimensions_pdata_id\":\"local.sunbird.desktop\",\"contentId\":\"do_2123320947346391041175\"},{\"play_sessions_count\":5,\"total_time_spent\":26.720000000000002,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_212343330163007488137\"},{\"play_sessions_count\":4,\"total_time_spent\":897.3600000000001,\"dimensions_pdata_id\":\"prod.sunbird.desktop\",\"contentId\":\"do_212343332329766912138\"},{\"play_sessions_count\":8,\"total_time_spent\":388.76,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_212343332329766912138\"},{\"play_sessions_count\":2,\"total_time_spent\":3.66,\"dimensions_pdata_id\":\"local.sunbird.desktop\",\"contentId\":\"do_2123476114861506561132\"},{\"play_sessions_count\":1,\"total_time_spent\":4.8,\"dimensions_pdata_id\":\"local.sunbird.desktop\",\"contentId\":\"do_212351887955853312146\"},{\"play_sessions_count\":1,\"total_time_spent\":0.24,\"dimensions_pdata_id\":\"prod.sunbird.desktop\",\"contentId\":\"do_212361708511485952140\"},{\"play_sessions_count\":4,\"total_time_spent\":49.3,\"dimensions_pdata_id\":\"local.sunbird.desktop\",\"contentId\":\"do_2123618302937989121631\"},{\"play_sessions_count\":1,\"total_time_spent\":53.71,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2123618302937989121631\"},{\"play_sessions_count\":1,\"total_time_spent\":1.18,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_212371257097060352128\"},{\"play_sessions_count\":1,\"total_time_spent\":238.77,\"dimensions_pdata_id\":\"staging.sunbirda.app\",\"contentId\":\"do_2123795092172636161757\"},{\"play_sessions_count\":2,\"total_time_spent\":24.770000000000003,\"dimensions_pdata_id\":\"local.sunbird.desktop\",\"contentId\":\"do_212380089244606464116\"},{\"play_sessions_count\":1,\"total_time_spent\":1.11,\"dimensions_pdata_id\":\"staging.sunbirda.portal\",\"contentId\":\"do_2124121620945059841219\"},{\"play_sessions_count\":3,\"total_time_spent\":60.510000000000005,\"dimensions_pdata_id\":\"staging.sunbirda.portal\",\"contentId\":\"do_2124339505053450241746\"},{\"play_sessions_count\":2,\"total_time_spent\":120.53,\"dimensions_pdata_id\":\"staging.sunbirda.portal\",\"contentId\":\"do_2124347141266391041106\"},{\"play_sessions_count\":1,\"total_time_spent\":2.92,\"dimensions_pdata_id\":\"prod.sunbird.desktop\",\"contentId\":\"do_21244095469408256011169\"},{\"play_sessions_count\":1,\"total_time_spent\":8.13,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_212459391424782336145\"},{\"play_sessions_count\":1,\"total_time_spent\":113.38,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2124700100688035841261\"},{\"play_sessions_count\":1,\"total_time_spent\":315.97,\"dimensions_pdata_id\":\"staging.sunbirda.app\",\"contentId\":\"do_2124708442401751041124\"},{\"play_sessions_count\":1,\"total_time_spent\":4.34,\"dimensions_pdata_id\":\"staging.sunbirda.portal\",\"contentId\":\"do_2124708443414036481125\"},{\"play_sessions_count\":1,\"total_time_spent\":2.56,\"dimensions_pdata_id\":\"staging.sunbirda.portal\",\"contentId\":\"do_2124742691037429761195\"},{\"play_sessions_count\":2,\"total_time_spent\":2.5500000000000003,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2124841348376985601751\"},{\"play_sessions_count\":2,\"total_time_spent\":2.34,\"dimensions_pdata_id\":\"prod.sunbird.desktop\",\"contentId\":\"do_2124905130588651521376\"},{\"play_sessions_count\":1,\"total_time_spent\":6.01,\"dimensions_pdata_id\":\"staging.sunbirda.app\",\"contentId\":\"do_21249344488375091213752\"},{\"play_sessions_count\":1,\"total_time_spent\":1.39,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_2124948145232855041166\"},{\"play_sessions_count\":2,\"total_time_spent\":58.879999999999995,\"dimensions_pdata_id\":\"staging.sunbirda.app\",\"contentId\":\"do_21249542373921587211269\"},{\"play_sessions_count\":1,\"total_time_spent\":18.64,\"dimensions_pdata_id\":\"staging.sunbirda.app\",\"contentId\":\"do_21249558183120896011342\"},{\"play_sessions_count\":1,\"total_time_spent\":1.27,\"dimensions_pdata_id\":\"staging.sunbirda.desktop\",\"contentId\":\"do_21249981114087014412566\"}]"))
    val jobConfig = JSONUtils.deserialize[Map[String, AnyRef]]("{\"search\":{\"type\":\"none\"},\"model\":\"org.ekstep.analytics.updater.UpdateContentRating\",\"modelParams\":{\"startDate\":\"'$endDate'\",\"endDate\":\"'$endDate'\"},\"output\":[{\"to\":\"console\",\"params\":{\"printEvent\":false}}],\"parallelization\":8,\"appName\":\"Content Rating Updater\",\"deviceMapping\":false}")

    val data = UpdateContentRating.getContentConsumptionMetrics(jobConfig, mockRestUtil)
    data.count() should be(2)
  }
}



