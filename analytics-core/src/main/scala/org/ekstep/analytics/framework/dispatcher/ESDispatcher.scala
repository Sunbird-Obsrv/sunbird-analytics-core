package org.ekstep.analytics.framework.dispatcher

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.exception.DispatcherException
import org.elasticsearch.spark._
import org.ekstep.analytics.framework.FrameworkContext

object ESDispatcher extends IDispatcher {

    implicit val className = "org.ekstep.analytics.framework.dispatcher.ESDispatcher"

    @throws(classOf[DispatcherException])
    def dispatch(config: Map[String, AnyRef], events: RDD[String])(implicit sc: SparkContext, fc: FrameworkContext) = {

        val index = config.getOrElse("index", null).asInstanceOf[String];
        if (null == index) {
            throw new DispatcherException("'index' parameter is required to send output to elasticsearch")
        }
        events.saveToEs(s"$index/_doc", Map("es.input.json" -> "true"))
    }

}
