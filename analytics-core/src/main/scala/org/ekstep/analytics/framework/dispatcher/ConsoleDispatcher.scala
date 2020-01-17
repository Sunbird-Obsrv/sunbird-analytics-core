package org.ekstep.analytics.framework.dispatcher

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.FrameworkContext

/**
 * @author Santhosh
 */
object ConsoleDispatcher extends IDispatcher {

    def dispatch(events: Array[String], config: Map[String, AnyRef])(implicit fc: FrameworkContext): Array[String] = {
        if (config.getOrElse("printEvent", true).asInstanceOf[Boolean]) {
            for (event <- events) {
                println("Event", event);
            }
        }
        println("Total Events Size", events.length);
        events;
    }
    
    def dispatch(config: Map[String, AnyRef], events: RDD[String])(implicit sc: SparkContext, fc: FrameworkContext) = {
        if (config.getOrElse("printEvent", true).asInstanceOf[Boolean]) {
            for (event <- events) {
                println("Event", event);
            }
        }
        println("Total Events Size", events.count);
    }
}