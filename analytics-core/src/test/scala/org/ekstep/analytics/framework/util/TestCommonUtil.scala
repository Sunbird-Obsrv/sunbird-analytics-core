package org.ekstep.analytics.framework.util

import org.ekstep.analytics.framework._
import org.joda.time.LocalDate
import java.io.File

import org.joda.time.DateTime
import java.util.Date
import java.text.SimpleDateFormat

import scala.collection.mutable.ListBuffer
import org.joda.time.format.DateTimeFormat
import org.ekstep.analytics.framework.Period._

class TestCommonUtil extends BaseSpec {

    it should "pass test case of all methods in CommonUtil" in {
        try {
            //datesBetween
            val from = new LocalDate("2016-01-01");
            val to = new LocalDate("2016-01-04");
            CommonUtil.datesBetween(from, to).toArray should be(Array(new LocalDate("2016-01-01"), new LocalDate("2016-01-02"), new LocalDate("2016-01-03"), new LocalDate("2016-01-04")))

            //deleteDirectory
            val path = "delete-this";
            val dir = new File(path)
            val dirCreated = dir.mkdir;
            dirCreated should be(true);
            val fp = "delete-this/delete-this.txt";
            val f = new File(fp);
            f.createNewFile();
            CommonUtil.deleteDirectory(path)
            dir.isDirectory() should be(false);
            f.isFile() should be(false);

            //deleteFile
            val filePath = "delete-this.txt";
            val noFile = "no-file.txt"
            val file = new File(filePath);
            val created = file.createNewFile();
            created should be(true);
            CommonUtil.deleteFile(filePath)
            CommonUtil.deleteFile(noFile)
            file.isFile() should be(false);

            //getAge
            val dateformat = new SimpleDateFormat("dd/MM/yyyy");
            val dob = dateformat.parse("04/07/1990");
            CommonUtil.getAge(dob) should be > (25)

            //getDatesBetween
            CommonUtil.getDatesBetween("2016-01-01", Option("2016-01-04")) should be(Array("2016-01-01", "2016-01-02", "2016-01-03", "2016-01-04"))
            CommonUtil.getDatesBetween("2016-01-01", None) should not be null;

            //getEvent
            val line = "{\"eid\":\"OE_START\",\"ts\":\"2016-01-01T12:13:20+05:30\",\"@timestamp\":\"2016-01-02T00:59:22.924Z\",\"ver\":\"1.0\",\"gdata\":{\"id\":\"org.ekstep.aser.lite\",\"ver\":\"5.7\"},\"sid\":\"a6e4b3e2-5c40-4d5c-b2bd-44f1d5c7dd7f\",\"uid\":\"2ac2ebf4-89bb-4d5d-badd-ba402ee70182\",\"did\":\"828bd4d6c37c300473fb2c10c2d28868bb88fee6\",\"edata\":{\"eks\":{\"loc\":null,\"mc\":null,\"mmc\":null,\"pass\":null,\"qid\":null,\"qtype\":null,\"qlevel\":null,\"score\":0,\"maxscore\":0,\"res\":null,\"exres\":null,\"length\":null,\"exlength\":0.0,\"atmpts\":0,\"failedatmpts\":0,\"category\":null,\"current\":null,\"max\":null,\"type\":null,\"extype\":null,\"id\":null,\"gid\":null}}}";
            val event = JSONUtils.deserialize[Event](line);
            val line2 = "{\"eid\":\"OE_START\",\"ts\":\"01-01-2016\",\"@timestamp\":\"2016-01-02\",\"ver\":\"1.0\",\"sid\":\"a6e4b3e2-5c40-4d5c-b2bd-44f1d5c7dd7f\",\"uid\":\"2ac2ebf4-89bb-4d5d-badd-ba402ee70182\",\"did\":\"828bd4d6c37c300473fb2c10c2d28868bb88fee6\",\"edata\":{\"eks\":{\"loc\":null,\"mc\":null,\"mmc\":null,\"pass\":null,\"qid\":null,\"qtype\":null,\"qlevel\":null,\"score\":0,\"maxscore\":0,\"res\":null,\"exres\":null,\"length\":null,\"exlength\":0.0,\"atmpts\":0,\"failedatmpts\":0,\"category\":null,\"current\":null,\"max\":null,\"type\":null,\"extype\":null,\"id\":null,\"gid\":null}}}";
            val event2 = JSONUtils.deserialize[Event](line2);
            val line3 = "{\"eid\":\"OE_START\",\"ts\":\"01-01-2016\",\"@timestamp\":\"2016-01-02T00:59:22+05:30\",\"ver\":\"1.0\",\"sid\":\"a6e4b3e2-5c40-4d5c-b2bd-44f1d5c7dd7f\",\"uid\":\"2ac2ebf4-89bb-4d5d-badd-ba402ee70182\",\"did\":\"828bd4d6c37c300473fb2c10c2d28868bb88fee6\",\"edata\":{\"eks\":{\"loc\":null,\"mc\":null,\"mmc\":null,\"pass\":null,\"qid\":null,\"qtype\":null,\"qlevel\":null,\"score\":0,\"maxscore\":0,\"res\":null,\"exres\":null,\"length\":null,\"exlength\":0.0,\"atmpts\":0,\"failedatmpts\":0,\"category\":null,\"current\":null,\"max\":null,\"type\":null,\"extype\":null,\"id\":null,\"gid\":null}}}";
            val event3 = JSONUtils.deserialize[Event](line3);
            val line4 = "{\"eid\":\"OE_START\",\"ts\":\"01-01-2016\",\"@timestamp\":\"2016-01-02T00:59:22P:ST\",\"ver\":\"1.0\",\"sid\":\"a6e4b3e2-5c40-4d5c-b2bd-44f1d5c7dd7f\",\"uid\":\"2ac2ebf4-89bb-4d5d-badd-ba402ee70182\",\"did\":\"828bd4d6c37c300473fb2c10c2d28868bb88fee6\",\"edata\":{\"eks\":{\"loc\":null,\"mc\":null,\"mmc\":null,\"pass\":null,\"qid\":null,\"qtype\":null,\"qlevel\":null,\"score\":0,\"maxscore\":0,\"res\":null,\"exres\":null,\"length\":null,\"exlength\":0.0,\"atmpts\":0,\"failedatmpts\":0,\"category\":null,\"current\":null,\"max\":null,\"type\":null,\"extype\":null,\"id\":null,\"gid\":null}}}";
            val event4 = JSONUtils.deserialize[Event](line4);
            val line5 = "{\"eid\":\"OE_START\",\"ets\":1451630600000,\"@timestamp\":\"2016-01-02T00:59:22.924Z\",\"ver\":\"1.0\",\"gdata\":{\"id\":\"org.ekstep.aser.lite\",\"ver\":\"5.7\"},\"sid\":\"a6e4b3e2-5c40-4d5c-b2bd-44f1d5c7dd7f\",\"uid\":\"2ac2ebf4-89bb-4d5d-badd-ba402ee70182\",\"did\":\"828bd4d6c37c300473fb2c10c2d28868bb88fee6\",\"edata\":{\"eks\":{\"loc\":null,\"mc\":null,\"mmc\":null,\"pass\":null,\"qid\":null,\"qtype\":null,\"qlevel\":null,\"score\":0,\"maxscore\":0,\"res\":null,\"exres\":null,\"length\":null,\"exlength\":0.0,\"atmpts\":0,\"failedatmpts\":0,\"category\":null,\"current\":null,\"max\":null,\"type\":null,\"extype\":null,\"id\":null,\"gid\":null}}}";
            val event5 = JSONUtils.deserialize[Event](line5);

            //getEventDate yyyy-MM-dd'T'HH:mm:ssZZ
            val evDate = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZZ").parseLocalDate("2016-01-01T12:13:20+05:30").toDate;
            CommonUtil.getEventDate(event) should be(evDate)

            //getEventTs
            CommonUtil.getEventTS(event) should be(1451630600000L)
            CommonUtil.getEventTS(event5) should be(1451630600000L)
            CommonUtil.getEventSyncTS(event) should be(1451696362924L)
            CommonUtil.getEventSyncTS(event2) should be(0L)
            CommonUtil.getEventSyncTS(event3) should be(1451676562000L)
            CommonUtil.getEventSyncTS(event4) should be(1451696362000L)

            CommonUtil.getEventTS(event2) should be(0)

            CommonUtil.getEventDate(event2) should be(null)

            //getGameId
            CommonUtil.getGameId(event) should be("org.ekstep.aser.lite")
            CommonUtil.getGameId(event2) should be(null)

            //getGameVersion
            CommonUtil.getGameVersion(event) should be("5.7")
            CommonUtil.getGameVersion(event2) should be(null)

            //getHourOfDay
            CommonUtil.getHourOfDay(1447154514000L, 1447158114000L) should be(ListBuffer(11, 12))
            CommonUtil.getHourOfDay(1447154514000L, 1447000L) should be(ListBuffer(11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 0))

            //getParallelization
            val config = new JobConfig(null, None, None, null, None, None, Option(10), Option("testApp"), Option(false));
            CommonUtil.getParallelization(config) should be(10)

            val config2 = new JobConfig(null, None, None, null, None, None, None, Option("testApp"), Option(false));
            CommonUtil.getParallelization(config) should be(10)

            //getParallelization
            val con = Option(Map("search" -> null, "filters" -> null, "sort" -> null, "model" -> null, "modelParams" -> null, "output" -> null, "parallelization" -> "10", "appName" -> "testApp", "deviceMapping" -> null))
            CommonUtil.getParallelization(con) should be(10)

            //getStartDate
            CommonUtil.getStartDate(Option("2016-01-08"), 7) should be(Option("2016-01-01"))
            CommonUtil.getStartDate(None, 0) should be(Option(LocalDate.fromDateFields(new Date).toString()))

            //getTimeDiff
            CommonUtil.getTimeDiff(1451650400000L, 1451650410000L) should be(Option(10d))
            CommonUtil.getTimeDiff(1451650400000L, 1451650410000L) should be(Option(10d))

            CommonUtil.getTimeDiff(event, event) should be(Option(0d))
            CommonUtil.getTimeDiff(event, event2) should be(Option(0d))

            //getTimeSpent
            CommonUtil.getTimeSpent("10") should be(Option(10d))
            CommonUtil.getTimeSpent(10d.asInstanceOf[AnyRef]) should be(Option(10d))
            CommonUtil.getTimeSpent(10.asInstanceOf[AnyRef]) should be(Option(10d))
            CommonUtil.getTimeSpent(null) should be(Option(0d))
            CommonUtil.getTimeSpent(true.asInstanceOf[AnyRef]) should be(Option(0d))

            CommonUtil.getTimestamp("2016-01-02T00:59:22+P:ST") should be(1451696362000L);

            CommonUtil.roundDouble(12.7345, 2) should be(12.73);

            //gzip
            val testPath = "src/test/resources/sample_telemetry.log";
            CommonUtil.gzip(testPath)
            new File("src/test/resources/sample_telemetry.log.gz").isFile() should be(true)
            CommonUtil.deleteFile("src/test/resources/sample_telemetry.log.gz");

            a[Exception] should be thrownBy {
                CommonUtil.gzip("src/test/resources/sample_telemetry.txt")
            }

            CommonUtil.getParallelization(None) should be(10);

            CommonUtil.getMessageId("ME_TEST", "123", "MONTH", DtRange(1451650400000L, 1451650400000L)) should be("1D99B2F1C6637AE21081CD981AFFB56F");
            CommonUtil.getMessageId("ME_TEST", "123", "MONTH", DtRange(1451650400000L, 1451650400000L), "org.ekstep.aser.lite") should be("6D5DCB288B1A9BC3036D04C37FF08EDF");

            CommonUtil.getMessageId("ME_TEST", "123", "MONTH", 1451650400000L, None, None) should be("D0BF57F856E3B7FAD5E47CCD4B31DE57");

            val res = CommonUtil.time({

                CommonUtil.getWeeksBetween(1451650400000L, 1454650400000L) should be(5)
                CommonUtil.getPeriod(1451650400000L, DAY) should be(20160101)
                CommonUtil.getPeriod(1451650400000L, WEEK) should be(2015753)
                CommonUtil.getPeriod(1452250748000L, WEEK) should be(2016701)
                CommonUtil.getPeriod(1451650400000L, MONTH) should be(201601)
                CommonUtil.getPeriod(1451650400000L, CUMULATIVE) should be(0)
                CommonUtil.getPeriod(1451650400000L, LAST7) should be(7)
                CommonUtil.getPeriod(1451650400000L, LAST30) should be(30)
                CommonUtil.getPeriod(1451650400000L, LAST90) should be(90)
                CommonUtil.getPeriod(new DateTime("2016-01-01"), DAY) should be(20160101)

            })
            res._1 should be > (0L)

            //getTags
            val metaData1 = Map("tags" -> List("test", "QA"), "activation_keys" -> "ptm007")
            val tags1 = CommonUtil.getTags(metaData1).get
            tags1.length should be(2)

            val metaData2 = Map("activation_keys" -> "ptm007", "tags" -> null)
            val tags2 = CommonUtil.getTags(metaData2).get
            tags2.length should be(0)

            val metaData3 = Map("activation_keys" -> "ptm007")
            val tags3 = CommonUtil.getTags(metaData3).get
            tags3.length should be(0)

            CommonUtil.daysBetween(new DateTime(1451650400000L).toLocalDate(), new DateTime(1454650400000L).toLocalDate()) should be(35);
        } catch {
            case ex: Exception => ex.printStackTrace();
        }

        CommonUtil.getPathFromURL("https://ekstep-public.s3-ap-southeast-1.amazonaws.com/ecar_files/domain_38527_1460631037666.ecar") should be("/ecar_files/domain_38527_1460631037666.ecar")

        // getPeriods
        val daysArray = CommonUtil.getPeriods(DAY, 5)
        daysArray.length should be(5)

        val weeksArray = CommonUtil.getPeriods(WEEK, 5)
        weeksArray.length should be(5)

        val monthsArray = CommonUtil.getPeriods(MONTH, 5)
        monthsArray.length should be(5)

        val cumArray = CommonUtil.getPeriods(CUMULATIVE, 5)
        cumArray.length should be(1)

        CommonUtil.getPeriods("DAY", 5)
        CommonUtil.getPeriods("WEEK", 5)
        CommonUtil.getPeriods("MONTH", 5)
        CommonUtil.getPeriods("CUMULATIVE", 5)

        //getValidTags
        val dEvent1 = "{\"eid\":\"ME_SESSION_SUMMARY\",\"ets\":1474439187443,\"syncts\":1474351045034,\"ver\":\"1.0\",\"mid\":\"0C2CE73054050FE7D0E03B5A71A35829\",\"uid\":\"3b81dc76-917c-4a67-9f08-1d84b201820c\",\"context\":{\"pdata\":{\"id\":\"AnalyticsDataPipeline\",\"model\":\"LearnerSessionSummary\",\"ver\":\"1.0\"},\"granularity\":\"SESSION\",\"date_range\":{\"from\":1474350532673,\"to\":1474350628540}},\"dimensions\":{\"did\":\"38e8baf2f7d2fa48fd73dac95dec23348992a907\",\"gdata\":{\"id\":\"do_30043182\",\"ver\":\"10\"},\"loc\":\"\",\"group_user\":false,\"anonymous_user\":true},\"edata\":{\"eks\":{\"levels\":[],\"noOfAttempts\":1,\"timeSpent\":95.87,\"interruptTime\":8.38,\"timeDiff\":95.87,\"start_time\":1474350532673,\"end_time\":1474350628540,\"currentLevel\":{},\"noOfLevelTransitions\":-1,\"interactEventsPerMin\":107.65,\"completionStatus\":false,\"screenSummary\":[{\"id\":\"reading_word_stage\",\"timeSpent\":5.33},{\"id\":\"homeScreen\",\"timeSpent\":6.37},{\"id\":\"writing_assess_stage\",\"timeSpent\":17.09},{\"id\":\"assessment_stage_three\",\"timeSpent\":7.72},{\"id\":\"splash\",\"timeSpent\":6.79},{\"id\":\"assessment_stage_one\",\"timeSpent\":10.92},{\"id\":\"writing_stage\",\"timeSpent\":4.36},{\"id\":\"assessment_stage_two\",\"timeSpent\":4.35},{\"id\":\"endScreen\",\"timeSpent\":4.31},{\"id\":\"reading_stage\",\"timeSpent\":20.27}],\"noOfInteractEvents\":172,\"eventsSummary\":[{\"id\":\"OE_ITEM_RESPONSE\",\"count\":1},{\"id\":\"OE_START\",\"count\":1},{\"id\":\"OE_NAVIGATE\",\"count\":13},{\"id\":\"OE_INTERACT\",\"count\":171},{\"id\":\"OE_INTERRUPT\",\"count\":2},{\"id\":\"OE_ASSESS\",\"count\":23},{\"id\":\"OE_END\",\"count\":1}],\"syncDate\":1474351045034,\"contentType\":\"Story\",\"mimeType\":\"application/vnd.ekstep.ecml-archive\",\"itemResponses\":[{\"time_stamp\":1474350574049,\"score\":1,\"timeSpent\":5.0,\"mmc\":[],\"res\":[\"5:D\"],\"resValues\":[{\"5\":\"D\"}],\"itemId\":\"esl.l3q28\",\"mc\":[]},{\"time_stamp\":1474350575965,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q29\",\"mc\":[]},{\"time_stamp\":1474350577358,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q30\",\"mc\":[]},{\"time_stamp\":1474350578565,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q31\",\"mc\":[]},{\"time_stamp\":1474350579836,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q32\",\"mc\":[]},{\"time_stamp\":1474350581019,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q33\",\"mc\":[]},{\"time_stamp\":1474350582208,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q34\",\"mc\":[]},{\"time_stamp\":1474350583517,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q35\",\"mc\":[]},{\"time_stamp\":1474350584901,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q36\",\"mc\":[]},{\"time_stamp\":1474350586118,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q37\",\"mc\":[]},{\"time_stamp\":1474350601438,\"score\":0,\"timeSpent\":9.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q38\",\"mc\":[]},{\"time_stamp\":1474350602642,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q39\",\"mc\":[]},{\"time_stamp\":1474350603195,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q40\",\"mc\":[]},{\"time_stamp\":1474350603602,\"score\":0,\"timeSpent\":0.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q41\",\"mc\":[]},{\"time_stamp\":1474350606416,\"score\":0,\"timeSpent\":3.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q42\",\"mc\":[]},{\"time_stamp\":1474350607301,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q43\",\"mc\":[]},{\"time_stamp\":1474350608010,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q44\",\"mc\":[]},{\"time_stamp\":1474350610031,\"score\":0,\"timeSpent\":2.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q45\",\"mc\":[]},{\"time_stamp\":1474350611213,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q46\",\"mc\":[]},{\"time_stamp\":1474350621749,\"score\":0,\"timeSpent\":11.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q47\",\"mc\":[]},{\"time_stamp\":1474350622758,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q48\",\"mc\":[]},{\"time_stamp\":1474350623511,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q49\",\"mc\":[]},{\"time_stamp\":1474350624180,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q50\",\"mc\":[]}]}},\"etags\":{\"app\":[]}}"
        val derivedEvent1 = JSONUtils.deserialize[DerivedEvent](dEvent1);
        CommonUtil.getValidTags(derivedEvent1, Array("test"))

        val dEvent2 = "{\"eid\":\"ME_SESSION_SUMMARY\",\"ets\":1474439187443,\"syncts\":1474351045034,\"ver\":\"1.0\",\"mid\":\"0C2CE73054050FE7D0E03B5A71A35829\",\"uid\":\"3b81dc76-917c-4a67-9f08-1d84b201820c\",\"context\":{\"pdata\":{\"id\":\"AnalyticsDataPipeline\",\"model\":\"LearnerSessionSummary\",\"ver\":\"1.0\"},\"granularity\":\"SESSION\",\"date_range\":{\"from\":1474350532673,\"to\":1474350628540}},\"dimensions\":{\"did\":\"38e8baf2f7d2fa48fd73dac95dec23348992a907\",\"gdata\":{\"id\":\"do_30043182\",\"ver\":\"10\"},\"loc\":\"\",\"group_user\":false,\"anonymous_user\":true},\"edata\":{\"eks\":{\"levels\":[],\"noOfAttempts\":1,\"timeSpent\":95.87,\"interruptTime\":8.38,\"timeDiff\":95.87,\"start_time\":1474350532673,\"end_time\":1474350628540,\"currentLevel\":{},\"noOfLevelTransitions\":-1,\"interactEventsPerMin\":107.65,\"completionStatus\":false,\"screenSummary\":[{\"id\":\"reading_word_stage\",\"timeSpent\":5.33},{\"id\":\"homeScreen\",\"timeSpent\":6.37},{\"id\":\"writing_assess_stage\",\"timeSpent\":17.09},{\"id\":\"assessment_stage_three\",\"timeSpent\":7.72},{\"id\":\"splash\",\"timeSpent\":6.79},{\"id\":\"assessment_stage_one\",\"timeSpent\":10.92},{\"id\":\"writing_stage\",\"timeSpent\":4.36},{\"id\":\"assessment_stage_two\",\"timeSpent\":4.35},{\"id\":\"endScreen\",\"timeSpent\":4.31},{\"id\":\"reading_stage\",\"timeSpent\":20.27}],\"noOfInteractEvents\":172,\"eventsSummary\":[{\"id\":\"OE_ITEM_RESPONSE\",\"count\":1},{\"id\":\"OE_START\",\"count\":1},{\"id\":\"OE_NAVIGATE\",\"count\":13},{\"id\":\"OE_INTERACT\",\"count\":171},{\"id\":\"OE_INTERRUPT\",\"count\":2},{\"id\":\"OE_ASSESS\",\"count\":23},{\"id\":\"OE_END\",\"count\":1}],\"syncDate\":1474351045034,\"contentType\":\"Story\",\"mimeType\":\"application/vnd.ekstep.ecml-archive\",\"itemResponses\":[{\"time_stamp\":1474350574049,\"score\":1,\"timeSpent\":5.0,\"mmc\":[],\"res\":[\"5:D\"],\"resValues\":[{\"5\":\"D\"}],\"itemId\":\"esl.l3q28\",\"mc\":[]},{\"time_stamp\":1474350575965,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q29\",\"mc\":[]},{\"time_stamp\":1474350577358,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q30\",\"mc\":[]},{\"time_stamp\":1474350578565,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q31\",\"mc\":[]},{\"time_stamp\":1474350579836,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q32\",\"mc\":[]},{\"time_stamp\":1474350581019,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q33\",\"mc\":[]},{\"time_stamp\":1474350582208,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q34\",\"mc\":[]},{\"time_stamp\":1474350583517,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q35\",\"mc\":[]},{\"time_stamp\":1474350584901,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q36\",\"mc\":[]},{\"time_stamp\":1474350586118,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q37\",\"mc\":[]},{\"time_stamp\":1474350601438,\"score\":0,\"timeSpent\":9.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q38\",\"mc\":[]},{\"time_stamp\":1474350602642,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q39\",\"mc\":[]},{\"time_stamp\":1474350603195,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q40\",\"mc\":[]},{\"time_stamp\":1474350603602,\"score\":0,\"timeSpent\":0.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q41\",\"mc\":[]},{\"time_stamp\":1474350606416,\"score\":0,\"timeSpent\":3.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q42\",\"mc\":[]},{\"time_stamp\":1474350607301,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q43\",\"mc\":[]},{\"time_stamp\":1474350608010,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q44\",\"mc\":[]},{\"time_stamp\":1474350610031,\"score\":0,\"timeSpent\":2.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q45\",\"mc\":[]},{\"time_stamp\":1474350611213,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q46\",\"mc\":[]},{\"time_stamp\":1474350621749,\"score\":0,\"timeSpent\":11.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q47\",\"mc\":[]},{\"time_stamp\":1474350622758,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q48\",\"mc\":[]},{\"time_stamp\":1474350623511,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q49\",\"mc\":[]},{\"time_stamp\":1474350624180,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q50\",\"mc\":[]}]}},\"etags\":{}}"
        val derivedEvent2 = JSONUtils.deserialize[DerivedEvent](dEvent2);
        CommonUtil.getValidTags(derivedEvent2, Array("test"))

        val dEvent3 = "{\"eid\":\"ME_SESSION_SUMMARY\",\"ets\":1474439187443,\"syncts\":1474351045034,\"ver\":\"1.0\",\"mid\":\"0C2CE73054050FE7D0E03B5A71A35829\",\"uid\":\"3b81dc76-917c-4a67-9f08-1d84b201820c\",\"context\":{\"pdata\":{\"id\":\"AnalyticsDataPipeline\",\"model\":\"LearnerSessionSummary\",\"ver\":\"1.0\"},\"granularity\":\"SESSION\",\"date_range\":{\"from\":1474350532673,\"to\":1474350628540}},\"dimensions\":{\"did\":\"38e8baf2f7d2fa48fd73dac95dec23348992a907\",\"gdata\":{\"id\":\"do_30043182\",\"ver\":\"10\"},\"loc\":\"\",\"group_user\":false,\"anonymous_user\":true},\"edata\":{\"eks\":{\"levels\":[],\"noOfAttempts\":1,\"timeSpent\":95.87,\"interruptTime\":8.38,\"timeDiff\":95.87,\"start_time\":1474350532673,\"end_time\":1474350628540,\"currentLevel\":{},\"noOfLevelTransitions\":-1,\"interactEventsPerMin\":107.65,\"completionStatus\":false,\"screenSummary\":[{\"id\":\"reading_word_stage\",\"timeSpent\":5.33},{\"id\":\"homeScreen\",\"timeSpent\":6.37},{\"id\":\"writing_assess_stage\",\"timeSpent\":17.09},{\"id\":\"assessment_stage_three\",\"timeSpent\":7.72},{\"id\":\"splash\",\"timeSpent\":6.79},{\"id\":\"assessment_stage_one\",\"timeSpent\":10.92},{\"id\":\"writing_stage\",\"timeSpent\":4.36},{\"id\":\"assessment_stage_two\",\"timeSpent\":4.35},{\"id\":\"endScreen\",\"timeSpent\":4.31},{\"id\":\"reading_stage\",\"timeSpent\":20.27}],\"noOfInteractEvents\":172,\"eventsSummary\":[{\"id\":\"OE_ITEM_RESPONSE\",\"count\":1},{\"id\":\"OE_START\",\"count\":1},{\"id\":\"OE_NAVIGATE\",\"count\":13},{\"id\":\"OE_INTERACT\",\"count\":171},{\"id\":\"OE_INTERRUPT\",\"count\":2},{\"id\":\"OE_ASSESS\",\"count\":23},{\"id\":\"OE_END\",\"count\":1}],\"syncDate\":1474351045034,\"contentType\":\"Story\",\"mimeType\":\"application/vnd.ekstep.ecml-archive\",\"itemResponses\":[{\"time_stamp\":1474350574049,\"score\":1,\"timeSpent\":5.0,\"mmc\":[],\"res\":[\"5:D\"],\"resValues\":[{\"5\":\"D\"}],\"itemId\":\"esl.l3q28\",\"mc\":[]},{\"time_stamp\":1474350575965,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q29\",\"mc\":[]},{\"time_stamp\":1474350577358,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q30\",\"mc\":[]},{\"time_stamp\":1474350578565,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q31\",\"mc\":[]},{\"time_stamp\":1474350579836,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q32\",\"mc\":[]},{\"time_stamp\":1474350581019,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q33\",\"mc\":[]},{\"time_stamp\":1474350582208,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q34\",\"mc\":[]},{\"time_stamp\":1474350583517,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q35\",\"mc\":[]},{\"time_stamp\":1474350584901,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q36\",\"mc\":[]},{\"time_stamp\":1474350586118,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q37\",\"mc\":[]},{\"time_stamp\":1474350601438,\"score\":0,\"timeSpent\":9.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q38\",\"mc\":[]},{\"time_stamp\":1474350602642,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q39\",\"mc\":[]},{\"time_stamp\":1474350603195,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q40\",\"mc\":[]},{\"time_stamp\":1474350603602,\"score\":0,\"timeSpent\":0.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q41\",\"mc\":[]},{\"time_stamp\":1474350606416,\"score\":0,\"timeSpent\":3.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q42\",\"mc\":[]},{\"time_stamp\":1474350607301,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q43\",\"mc\":[]},{\"time_stamp\":1474350608010,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q44\",\"mc\":[]},{\"time_stamp\":1474350610031,\"score\":0,\"timeSpent\":2.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q45\",\"mc\":[]},{\"time_stamp\":1474350611213,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q46\",\"mc\":[]},{\"time_stamp\":1474350621749,\"score\":0,\"timeSpent\":11.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q47\",\"mc\":[]},{\"time_stamp\":1474350622758,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q48\",\"mc\":[]},{\"time_stamp\":1474350623511,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q49\",\"mc\":[]},{\"time_stamp\":1474350624180,\"score\":0,\"timeSpent\":1.0,\"mmc\":[],\"res\":[],\"resValues\":[],\"itemId\":\"esl.l3q50\",\"mc\":[]}]}},\"etags\":{\"app\":[\"test\", \"QA\"]}}"
        val derivedEvent3 = JSONUtils.deserialize[DerivedEvent](dEvent3);
        val out = CommonUtil.getValidTags(derivedEvent3, Array("test"))
        out.length should be(1)

        //zip
        CommonUtil.zip("src/test/resources/test.zip", List("src/test/resources/sample_telemetry.log", "src/test/resources/sample_telemetry_2.log"))
        new File("src/test/resources/test.zip").isFile() should be(true)
        CommonUtil.deleteFile("src/test/resources/test.zip");
        //zip folder
        //CommonUtil.zipFolder("src/test/resources/zipFolderTest.zip", "src/test/resources/1234/OE_INTERACT")
        //new File("src/test/resources/zipFolderTest.zip").isFile() should be(true)
        //CommonUtil.deleteFile("src/test/resources/zipFolderTest.zip");

        //ccToMap
        val x = CommonUtil.caseClassToMap(DerivedEvent)

        //zip dir
        CommonUtil.zipDir("src/test/resources/test.zip", "src/test/resources/1234")
        new File("src/test/resources/test.zip").isFile() should be(true)
        CommonUtil.deleteFile("src/test/resources/test.zip");

        //getChanneId
        val event = "{\"eid\":\"OE_INTERACT\", \"channel\": \"in.ekstep\", \"ts\":\"2016-05-05T11:13:04.305+0530\",\"ets\":1462426984305,\"ver\":\"2.0\",\"gdata\":{\"id\":\"org.ekstep.story.en.haircut\",\"ver\":\"1\"},\"sid\":\"2b927be8-6a74-460b-aa20-0c991bcf57f6\",\"uid\":\"40550853-c88c-4f6b-8d33-88d0f47c32f4\",\"did\":\"d601e461a64b06f8828886e2f740e1688491a0a8\",\"edata\":{\"eks\":{\"score\":0,\"atmpts\":0,\"failedatmpts\":0,\"type\":\"LISTEN\",\"extype\":\"\",\"id\":\"splash:cover_sound\",\"stageid\":\"splash\",\"uri\":\"\",\"subtype\":\"PLAY\",\"pos\":[],\"values\":[],\"tid\":\"\",\"rating\":0.0}},\"tags\":[{\"genie\":[\"becb887fe82f24c644482eb30041da6d88bd8150\"]}],\"metadata\":{\"sync_timestamp\":\"2016-11-19T23:12:28+00:00\",\"public\":\"true\"},\"@timestamp\":\"2016-11-09T08:16:35.699Z\"}"
        val channelId = CommonUtil.getChannelId(JSONUtils.deserialize[Event](event))
        channelId should be("in.ekstep")

        val drivedEvent = "{\"eid\":\"ME_CE_SESSION_SUMMARY\",\"ets\":1495515314134,\"syncts\":1495456436116,\"ver\":\"1.0\",\"mid\":\"37E9E91997249D12F06C1D4869E286DE\",\"uid\":\"562\",\"content_id\":\"do_2122315986551685121193\",\"context\":{\"pdata\":{\"id\":\"AnalyticsDataPipeline\",\"model\":\"ContentEditorSessionSummary\",\"ver\":\"1.0\"},\"granularity\":\"SESSION\",\"date_range\":{\"from\":1495456435738,\"to\":1495456436116}},\"dimensions\":{\"sid\":\"5edg6dsos4bun8q8utp0k9gqa0\"},\"edata\":{\"eks\":{\"interact_events_per_min\":0.0,\"start_time\":1495456435738,\"plugin_summary\":{\"loaded_count\":0,\"plugins_added\":0,\"plugins_removed\":0,\"plugins_modified\":0,\"per_plugin_summary\":[]},\"menu_events_count\":0,\"interact_events_count\":0,\"end_time\":1495456436116,\"events_summary\":[{\"id\":\"CE_API_CALL\",\"count\":3}],\"sidebar_events_count\":0,\"time_diff\":0.38,\"api_calls_count\":3,\"stage_summary\":{\"stages_added\":0,\"stages_removed\":0,\"stages_modified\":0},\"load_time\":0.0,\"save_summary\":{\"total_count\":0,\"success_count\":0,\"failed_count\":0},\"time_spent\":0.38}}}"
        val channelId1 = CommonUtil.getChannelId(JSONUtils.deserialize[DerivedEvent](drivedEvent))
        channelId1 should be("in.ekstep")

        val profileEvent = "{\"eid\":\"ME_SESSION_SUMMARY\",\"ets\":1453207660735,\"syncts\":1453207660735,\"ver\":\"1.0\",\"uid\":\"8b4f3775-6f65-4abf-9afa-b15b8f82a24b\",\"context\":{\"pdata\":{\"id\":\"AnalyticsDataPipeline\",\"model\":\"GenericSessionSummarizer\",\"ver\":\"1.1\"},\"granularity\":\"SESSION\",\"dt_range\":{\"from\":1450079174000,\"to\":1450079337000}},\"dimensions\":{\"gdata\":{\"id\":\"org.ekstep.aser\",\"ver\":\"5.6.1\"},\"loc\":\"22.6370684,77.5506687\"},\"edata\":{\"eks\":{\"startTime\":1450079174000,\"noOfLevelTransitions\":1,\"levels\":[{\"choices\":[],\"domain\":\"\",\"noOfAttempts\":1,\"level\":\"Can do subtraction\"},{\"choices\":[\"q_4_s_hindi\",\"q_sub_q1127\",\"q_sub_q1126\"],\"domain\":\"\",\"noOfAttempts\":1,\"level\":\"Can read story\"}],\"activitySummary\":{\"TOUCH\":{\"count\":21,\"timeSpent\":161.0}},\"noOfAttempts\":1,\"timeSpent\":6206.0,\"interactEventsPerMin\":0.2,\"endTime\":1450079337000,\"eventsSummary\":{\"OE_START\":1,\"OE_INTERACT\":21,\"OE_ASSESS\":3,\"OE_END\":1,\"OE_LEVEL_SET\":2},\"currentLevel\":{\"numeracy\":\"Can do subtraction\",\"literacy\":\"Can read story\"},\"noOfInteractEvents\":21,\"interruptTime\":0.0,\"itemResponses\":[{\"itemId\":\"q_4_s_hindi\",\"itype\":\"recognition\",\"ilevel\":\"MEDIUM\",\"timeSpent\":29.0,\"res\":[\"अत्चा\"],\"mc\":[],\"score\":1,\"timeStamp\":1450079266000,\"maxScore\":1,\"domain\":\"literacy\"},{\"itemId\":\"q_sub_q1127\",\"itype\":\"ftb\",\"ilevel\":\"MEDIUM\",\"timeSpent\":33.0,\"res\":[\"49\"],\"mc\":[],\"score\":1,\"timeStamp\":1450079299000,\"maxScore\":1,\"domain\":\"numeracy\"},{\"itemId\":\"q_sub_q1126\",\"itype\":\"ftb\",\"ilevel\":\"MEDIUM\",\"timeSpent\":27.0,\"res\":[\"17\"],\"mc\":[],\"score\":1,\"timeStamp\":1450079322000,\"maxScore\":1,\"domain\":\"numeracy\"}]}}}"
        val channelId2 = CommonUtil.getChannelId(JSONUtils.deserialize[ProfileEvent](profileEvent))
        channelId2 should be("in.ekstep")
        
        // getAppDetails
        val event1 = "{\"eid\":\"OE_INTERACT\", \"channel\": \"in.ekstep\", \"ts\":\"2016-05-05T11:13:04.305+0530\",\"ets\":1462426984305,\"ver\":\"2.0\",\"gdata\":{\"id\":\"org.ekstep.story.en.haircut\",\"ver\":\"1\"},\"sid\":\"2b927be8-6a74-460b-aa20-0c991bcf57f6\",\"uid\":\"40550853-c88c-4f6b-8d33-88d0f47c32f4\",\"did\":\"d601e461a64b06f8828886e2f740e1688491a0a8\",\"edata\":{\"eks\":{\"score\":0,\"atmpts\":0,\"failedatmpts\":0,\"type\":\"LISTEN\",\"extype\":\"\",\"id\":\"splash:cover_sound\",\"stageid\":\"splash\",\"uri\":\"\",\"subtype\":\"PLAY\",\"pos\":[],\"values\":[],\"tid\":\"\",\"rating\":0.0}},\"tags\":[{\"genie\":[\"becb887fe82f24c644482eb30041da6d88bd8150\"]}],\"metadata\":{\"sync_timestamp\":\"2016-11-19T23:12:28+00:00\",\"public\":\"true\"},\"@timestamp\":\"2016-11-09T08:16:35.699Z\"}"
        val appId = CommonUtil.getAppDetails(JSONUtils.deserialize[Event](event))
        appId.id should be("genie")

        val drivedEvent1 = "{\"eid\":\"ME_CE_SESSION_SUMMARY\",\"ets\":1495515314134,\"syncts\":1495456436116,\"ver\":\"1.0\",\"mid\":\"37E9E91997249D12F06C1D4869E286DE\",\"uid\":\"562\",\"content_id\":\"do_2122315986551685121193\",\"context\":{\"pdata\":{\"id\":\"AnalyticsDataPipeline\",\"model\":\"ContentEditorSessionSummary\",\"ver\":\"1.0\"},\"granularity\":\"SESSION\",\"date_range\":{\"from\":1495456435738,\"to\":1495456436116}},\"dimensions\":{\"sid\":\"5edg6dsos4bun8q8utp0k9gqa0\"},\"edata\":{\"eks\":{\"interact_events_per_min\":0.0,\"start_time\":1495456435738,\"plugin_summary\":{\"loaded_count\":0,\"plugins_added\":0,\"plugins_removed\":0,\"plugins_modified\":0,\"per_plugin_summary\":[]},\"menu_events_count\":0,\"interact_events_count\":0,\"end_time\":1495456436116,\"events_summary\":[{\"id\":\"CE_API_CALL\",\"count\":3}],\"sidebar_events_count\":0,\"time_diff\":0.38,\"api_calls_count\":3,\"stage_summary\":{\"stages_added\":0,\"stages_removed\":0,\"stages_modified\":0},\"load_time\":0.0,\"save_summary\":{\"total_count\":0,\"success_count\":0,\"failed_count\":0},\"time_spent\":0.38}}}"
        val appId1 = CommonUtil.getAppDetails(JSONUtils.deserialize[DerivedEvent](drivedEvent))
        appId1.id should be("genie")

        val profileEvent1 = "{\"eid\":\"ME_SESSION_SUMMARY\",\"ets\":1453207660735,\"syncts\":1453207660735,\"ver\":\"1.0\",\"uid\":\"8b4f3775-6f65-4abf-9afa-b15b8f82a24b\",\"context\":{\"pdata\":{\"id\":\"AnalyticsDataPipeline\",\"model\":\"GenericSessionSummarizer\",\"ver\":\"1.1\"},\"granularity\":\"SESSION\",\"dt_range\":{\"from\":1450079174000,\"to\":1450079337000}},\"dimensions\":{\"gdata\":{\"id\":\"org.ekstep.aser\",\"ver\":\"5.6.1\"},\"loc\":\"22.6370684,77.5506687\"},\"edata\":{\"eks\":{\"startTime\":1450079174000,\"noOfLevelTransitions\":1,\"levels\":[{\"choices\":[],\"domain\":\"\",\"noOfAttempts\":1,\"level\":\"Can do subtraction\"},{\"choices\":[\"q_4_s_hindi\",\"q_sub_q1127\",\"q_sub_q1126\"],\"domain\":\"\",\"noOfAttempts\":1,\"level\":\"Can read story\"}],\"activitySummary\":{\"TOUCH\":{\"count\":21,\"timeSpent\":161.0}},\"noOfAttempts\":1,\"timeSpent\":6206.0,\"interactEventsPerMin\":0.2,\"endTime\":1450079337000,\"eventsSummary\":{\"OE_START\":1,\"OE_INTERACT\":21,\"OE_ASSESS\":3,\"OE_END\":1,\"OE_LEVEL_SET\":2},\"currentLevel\":{\"numeracy\":\"Can do subtraction\",\"literacy\":\"Can read story\"},\"noOfInteractEvents\":21,\"interruptTime\":0.0,\"itemResponses\":[{\"itemId\":\"q_4_s_hindi\",\"itype\":\"recognition\",\"ilevel\":\"MEDIUM\",\"timeSpent\":29.0,\"res\":[\"अत्चा\"],\"mc\":[],\"score\":1,\"timeStamp\":1450079266000,\"maxScore\":1,\"domain\":\"literacy\"},{\"itemId\":\"q_sub_q1127\",\"itype\":\"ftb\",\"ilevel\":\"MEDIUM\",\"timeSpent\":33.0,\"res\":[\"49\"],\"mc\":[],\"score\":1,\"timeStamp\":1450079299000,\"maxScore\":1,\"domain\":\"numeracy\"},{\"itemId\":\"q_sub_q1126\",\"itype\":\"ftb\",\"ilevel\":\"MEDIUM\",\"timeSpent\":27.0,\"res\":[\"17\"],\"mc\":[],\"score\":1,\"timeStamp\":1450079322000,\"maxScore\":1,\"domain\":\"numeracy\"}]}}}"
        val appId2 = CommonUtil.getAppDetails(JSONUtils.deserialize[ProfileEvent](profileEvent1))
        appId2.id should be("genie")
        
        //getEndTimestampOfDay
        val time = CommonUtil.getEndTimestampOfDay("2016-01-02")
        time.toString() should be ("1451759399000")
        
        // dayPeriodToLong
        val dayPeriodToLong = CommonUtil.dayPeriodToLong(20170713)
        dayPeriodToLong.toString should be("1499904000000")
        
        // getWeeksBetween
        val getWeeksBetween = CommonUtil.getWeeksBetween(1499904L, 1451759399L)
        getWeeksBetween should be(2)

        // getMetricEvent
        val metricEvent = CommonUtil.getMetricEvent(Map("system" -> "DataProduct", "subsystem" -> "test", "metrics" -> List(V3MetricEdata("count", "100".asInstanceOf[AnyRef]))), "pipeline-monitoring", "dataproduct-metric")
        metricEvent.context.pdata.get.id should be("pipeline-monitoring")
        metricEvent.context.pdata.get.pid.get should be("dataproduct-metric")

        val epochToTimestamp = CommonUtil.getTimestampFromEpoch(1537550355883L)
        epochToTimestamp.toString should be("2018-09-21 17:19:15.883")

        val connectionProperties = CommonUtil.getPostgresConnectionProps()
        connectionProperties.getProperty("user") should be("postgres")
        connectionProperties.getProperty("password") should be("postgres")
        connectionProperties.getProperty("driver") should be("org.postgresql.Driver")
    }
}