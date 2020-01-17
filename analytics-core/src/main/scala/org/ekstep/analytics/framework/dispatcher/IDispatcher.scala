package org.ekstep.analytics.framework.dispatcher

import org.ekstep.analytics.framework.exception.DispatcherException
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.FrameworkContext

/**
 * @author Santhosh
 */
trait IDispatcher {

    @throws(classOf[DispatcherException])
    def dispatch(events: Array[String], config: Map[String, AnyRef])(implicit fc: FrameworkContext) : Array[String];
    
    @throws(classOf[DispatcherException])
    def dispatch(config: Map[String, AnyRef], events: RDD[String])(implicit sc: SparkContext, fc: FrameworkContext);
    
}