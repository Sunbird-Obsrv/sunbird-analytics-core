package org.ekstep.analytics.framework.factory

import org.ekstep.analytics.framework.dispatcher.S3Dispatcher
import org.ekstep.analytics.framework.{BaseSpec, Dispatcher}

class TestDispatcherFactory extends BaseSpec {

    it should "return a dispatcher for known dispatcher types" in {

        val dispatcherList = List(Dispatcher("s3", Map()), Dispatcher("kafka", Map()),
            Dispatcher("console", Map()), Dispatcher("file", Map()))
        val dispatchers = dispatcherList.map { f => DispatcherFactory.getDispatcher(f) }

        dispatchers(0) should be(S3Dispatcher)

    }
}
