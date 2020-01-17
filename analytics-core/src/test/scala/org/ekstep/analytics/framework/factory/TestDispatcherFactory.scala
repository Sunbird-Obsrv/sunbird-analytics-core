package org.ekstep.analytics.framework.factory

import org.ekstep.analytics.framework.dispatcher.S3Dispatcher
import org.ekstep.analytics.framework.{BaseSpec, Dispatcher}

class TestDispatcherFactory extends BaseSpec {

    it should "return a Model class for a model code" in {

        val dispatcherList = List(Dispatcher("s3file", Map()), Dispatcher("s3", Map()), Dispatcher("kafka", Map()), Dispatcher("script", Map()),
            Dispatcher("console", Map()), Dispatcher("file", Map()), Dispatcher("azure", Map()), Dispatcher("slack", Map()), Dispatcher("elasticsearch", Map()))
        val dispatchers = dispatcherList.map { f => DispatcherFactory.getDispatcher(f) }

        dispatchers(1) should be(S3Dispatcher)

    }
}
