package org.ekstep.analytics.util

import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.{SparkSpec, WFSInputEvent}

class TestSummary extends SparkSpec {

    it should "create summary without uid, pdata and edata.type" in {
        val eventStr = "{\"eid\":\"START\",\"ets\":1534595611976,\"ver\":\"3.0\",\"mid\":\"817e25d0-33f5-48a2-8239-db286aaf3bd8\",\"actor\":{\"type\":\"User\"},\"context\":{\"channel\":\"01235953109336064029450\",\"env\":\"home\",\"sid\":\"8f32dbc4-c0d0-4630-9ff6-8c3bce3d15bb\",\"did\":\"a49c706d0402d6db3bb7cb3105cc9e7cf9b2ed7e\",\"cdata\":[]},\"object\":{\"id\":\"do_31250841732493312026783\",\"type\":\"TextBookUnit\",\"rollup\":{\"l1\":\"do_31250841732058316826675\",\"l2\":\"do_31250841732491673626778\",\"l3\":\"do_31250841732493312026783\"}},\"edata\":{\"mode\":\"play\",\"duration\":0,\"pageid\":\"collection-detail\",\"score\":0,\"rating\":0.0,\"index\":0,\"size\":0},\"tags\":[],\"@timestamp\":\"2018-08-19T04:08:41.195Z\"}"
        val event = JSONUtils.deserialize[WFSInputEvent](eventStr)
        val summary = new Summary(event)

        summary.uid should be("")
        summary.`type` should be("app")
        val me = summary.getSummaryEvent(Map())
    }

    it should "create summary and add ASSESS event" in {

        val eventStr = "{\"eid\":\"START\",\"ets\":1534651548447,\"ver\":\"3.0\",\"mid\":\"b154b70f-17fb-4c16-80f6-130f5ad479a1\",\"actor\":{\"id\":\"79498cf0-cdfa-41fb-b504-f257a8b40955\",\"type\":\"User\"},\"context\":{\"channel\":\"01235953109336064029450\",\"pdata\":{\"id\":\"prod.sunbird.app\",\"ver\":\"2.0.102\",\"pid\":\"sunbird.app.contentplayer\"},\"env\":\"contentplayer\",\"sid\":\"8f32dbc4-c0d0-4630-9ff6-8c3bce3d15bb\",\"did\":\"a49c706d0402d6db3bb7cb3105cc9e7cf9b2ed7e\",\"cdata\":[{\"id\":\"7f37a89e68bceff05ac34fb1b507ad71\",\"type\":\"ContentSession\"}],\"rollup\":{}},\"object\":{\"id\":\"do_31251316520745369623572\",\"type\":\"Content\",\"ver\":\"1.0\"},\"edata\":{\"type\":\"content\",\"mode\":\"play\",\"duration\":1534651548445,\"pageid\":\"\",\"score\":0,\"rating\":0.0,\"index\":0,\"size\":0},\"tags\":[],\"@timestamp\":\"2018-08-19T04:09:13.259Z\"}"
        val event = JSONUtils.deserialize[WFSInputEvent](eventStr)
        val summary = new Summary(event)

        summary.uid should be("79498cf0-cdfa-41fb-b504-f257a8b40955")
        summary.`type` should be("content")
        summary.mode.get should be("play")

        val assessStr = "{\"actor\":{\"id\":\"580\",\"type\":\"User\"},\"context\":{\"cdata\":[{\"id\":\"de2115eb13f5113ffce9444cfecd3ab7\",\"type\":\"ContentSession\"}],\"channel\":\"in.sunbird\",\"did\":\"b027147870670bc57de790535311fbe5\",\"env\":\"ContentPlayer\",\"pdata\":{\"id\":\"in.sunbird.dev\",\"pid\":\"sunbird.app.contentplayer\",\"ver\":\"2.0.93\"},\"rollup\":{},\"sid\":\"7op5o46hpi2abkmp8ckihjeq72\"},\"edata\":{\"duration\":25,\"index\":8,\"item\":{\"desc\":\"\",\"exlength\":0,\"id\":\"do_31249422333176217625083\",\"maxscore\":1,\"mc\":[],\"mmc\":[],\"params\":[],\"title\":\"3ZMSUQQ9\",\"uri\":\"\"},\"pass\":\"Yes\",\"score\":1},\"eid\":\"ASSESS\",\"ets\":1515497370223,\"mid\":\"01951ddb-cf6e-4a4a-b740-b5e4fd0e483d\",\"object\":{\"id\":\"do_1122852550749306881159\",\"type\":\"Content\",\"ver\":\"1.0\"},\"tags\":[],\"ver\":\"3.0\",\"@timestamp\":\"2018-06-04T04:59:45.328Z\",\"ts\":\"2018-06-04T04:59:33.379+0000\"}"
        val assessEvent = JSONUtils.deserialize[WFSInputEvent](assessStr)

        summary.add(assessEvent, 600)

        val endStr = "{\"eid\":\"END\",\"ets\":1536574379460,\"ver\":\"3.0\",\"mid\":\"9200c869-d16f-47af-b220-cf97845f1501\",\"actor\":{\"id\":\"a72482ab-320b-43dc-9821-5e6d1c9bf44a\",\"type\":\"User\"},\"context\":{\"channel\":\"01235953109336064029450\",\"pdata\":{\"id\":\"prod.sunbird.app\",\"ver\":\"2.0.106\",\"pid\":\"sunbird.app.contentplayer\"},\"env\":\"contentplayer\",\"sid\":\"b8bc1f1d-22a6-4aa6-aa5e-de3654e80f96\",\"did\":\"1b21a2906e7de0dd66235e7cf9373adb4aaaa104\",\"cdata\":[{\"id\":\"324168fd6623977b924f8552697cfd09\",\"type\":\"ContentSession\"}],\"rollup\":{\"l1\":\"do_31250890252435456027272\",\"l2\":\"do_31250890925065011217170\",\"l3\":\"do_31250890925065830417174\",\"l4\":\"do_31250890925069107217196\"}},\"object\":{\"id\":\"do_31251478823299481625615\",\"type\":\"Content\",\"ver\":\"1.0\"},\"edata\":{\"type\":\"resource\",\"mode\":\"play\",\"duration\":121972,\"pageid\":\"sunbird-player-Endpage\",\"score\":0,\"rating\":0.0,\"summary\":[{\"progress\":100.0}],\"index\":0,\"size\":0},\"tags\":[],\"@timestamp\":\"2018-09-11T05:08:04.311Z\"}"
        val endEvent = JSONUtils.deserialize[WFSInputEvent](endStr)

        summary.checkEnd(endEvent, 600, Map())
        summary.getSimilarEndSummary(endEvent)

    }
}
