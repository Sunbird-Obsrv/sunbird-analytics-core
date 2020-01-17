package org.ekstep.analytics.framework.dispatcher

import org.ekstep.analytics.framework.SparkSpec
import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.FrameworkContext

class TestSlackDispatcher extends SparkSpec {

    implicit val fc = new FrameworkContext();
    
    "SlackDispatcher" should "send output to slack" in {

        SlackDispatcher.dispatch(Map("channel" -> "testing", "userName" -> "test"), sc.parallelize(List("test")));

        val out = SlackDispatcher.dispatch(Array("test"), Map("channel" -> "testing", "userName" -> "test"));
        println(out)
        out.length should be(1)
    }

    it should "give DispatcherException if slack config is missing" in {


        the[DispatcherException] thrownBy {
            SlackDispatcher.dispatch(Map("channel" -> "testing"), sc.parallelize(List("test")));
        } should have message "'channel' & 'userName' parameters are required to send output to slack"

    }
}
