package org.ekstep.analytics.framework

import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.dispatcher.S3Dispatcher
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.analytics.framework.dispatcher.ScriptDispatcher
import org.ekstep.analytics.framework.factory.DispatcherFactory
import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.dispatcher.FileDispatcher
import org.apache.spark.SparkContext

/**
 * @author Santhosh
 */
object OutputDispatcher {

    implicit val className = "org.ekstep.analytics.framework.OutputDispatcher";

    @throws(classOf[DispatcherException])
    private def _dispatch(dispatcher: Dispatcher, events: RDD[String])(implicit sc: SparkContext, fc: FrameworkContext) = {
        DispatcherFactory.getDispatcher(dispatcher).dispatch(dispatcher.params, events);
    }

    @throws(classOf[DispatcherException])
    def dispatch[T](outputs: Option[Array[Dispatcher]], events: RDD[T])(implicit sc: SparkContext, fc: FrameworkContext): Long = {

        if (outputs.isEmpty) {
            throw new DispatcherException("No output configurations found");
        }
        val eventArr = stringify(events);
        outputs.get.foreach { dispatcher =>
            JobLogger.log("Dispatching output", Option(dispatcher.to));
            DispatcherFactory.getDispatcher(dispatcher).dispatch(dispatcher.params, eventArr);
        }
        events.count;

    }

    @throws(classOf[DispatcherException])
    def dispatch[T](dispatcher: Dispatcher, events: RDD[T])(implicit sc: SparkContext, fc: FrameworkContext): Long = {

        if (null == dispatcher) {
            throw new DispatcherException("No output configurations found");
        }
        val eventArr = stringify(events);
        DispatcherFactory.getDispatcher(dispatcher).dispatch(dispatcher.params, eventArr);
        events.count;
    }

    @throws(classOf[DispatcherException])
    def dispatch[T](dispatcher: Dispatcher, events: Array[String])(implicit fc: FrameworkContext) = {

        if (null == dispatcher) {
            throw new DispatcherException("No output configurations found");
        }
        if (events.length != 0) {
            JobLogger.log("Dispatching output", Option(dispatcher.to));
            DispatcherFactory.getDispatcher(dispatcher).dispatch(events, dispatcher.params);
        } else {
            JobLogger.log("No events produced");
            null;
        }
    }

    def stringify[T](events: RDD[T]): RDD[String] = {
        events.map { x =>
            if (x.isInstanceOf[String])
                x.asInstanceOf[String]
            else
                JSONUtils.serialize(x.asInstanceOf[AnyRef])
        }
    }
}