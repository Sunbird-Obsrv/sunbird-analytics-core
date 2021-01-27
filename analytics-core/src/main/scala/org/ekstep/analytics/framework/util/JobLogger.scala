package org.ekstep.analytics.framework.util

import org.ekstep.analytics.framework.{Level => _, _}
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.core.LoggerContext
import org.ekstep.analytics.framework.Level._
import org.ekstep.analytics.framework.conf.AppConf
import org.apache.commons.lang3.StringUtils
import org.apache.logging.log4j.core.appender.mom.kafka.KafkaAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import java.nio.charset.Charset

import org.apache.logging.log4j.core.config.AppenderRef
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import org.joda.time.DateTime

object JobLogger {

    implicit val fc = new FrameworkContext();

    def init(jobName: String) = {
        System.setProperty("logFilename", jobName.toLowerCase());
        val ctx = LogManager.getContext(false).asInstanceOf[LoggerContext];
        ctx.reconfigure();
        JobContext.jobName = jobName;
    }

    private def logger(name: String): Logger = {
        LogManager.getLogger(name+".logger");
    }

    private def info(msg: String, data: Option[AnyRef] = None, name: String = "org.ekstep.analytics", pdata_id: String = "AnalyticsDataPipeline", pdata_pid: String = JobContext.jobName)(implicit className: String) {
        val event = JSONUtils.serialize(getV3JobEvent("JOB_LOG", "INFO", msg, data, None, pdata_id, pdata_pid))
        logEvent(event, name, INFO)
    }

    private def debug(msg: String, data: Option[AnyRef] = None, name: String = "org.ekstep.analytics", pdata_id: String = "AnalyticsDataPipeline", pdata_pid: String = JobContext.jobName)(implicit className: String) {
        val event = JSONUtils.serialize(getV3JobEvent("JOB_LOG", "DEBUG", msg, data, None, pdata_id, pdata_pid))
        logger(name).debug(event);
    }

    private def error(msg: String, data: Option[AnyRef] = None, name: String = "org.ekstep.analytics", pdata_id: String = "AnalyticsDataPipeline", pdata_pid: String = JobContext.jobName)(implicit className: String) {
        val event = JSONUtils.serialize(getV3JobEvent("JOB_LOG", "ERROR", msg, data, None, pdata_id, pdata_pid))
        logEvent(event, name, ERROR)
    }

    private def warn(msg: String, data: Option[AnyRef] = None, name: String = "org.ekstep.analytics", pdata_id: String = "AnalyticsDataPipeline", pdata_pid: String = JobContext.jobName)(implicit className: String) {
        val event = JSONUtils.serialize(getV3JobEvent("JOB_LOG", "WARN", msg, data, None, pdata_id, pdata_pid))
        logger(name).debug(event);
    }

    def start(msg: String, data: Option[AnyRef] = None, name: String = "org.ekstep.analytics", pdata_id: String = "AnalyticsDataPipeline", pdata_pid: String = JobContext.jobName)(implicit className: String) = {
        val event = JSONUtils.serialize(getV3JobEvent("JOB_START", "INFO", msg, data, None, pdata_id, pdata_pid));
        EventBusUtil.dipatchEvent(event);
        logEvent(event, name, INFO)
    }

    def end(msg: String, status: String, data: Option[AnyRef] = None, name: String = "org.ekstep.analytics", pdata_id: String = "AnalyticsDataPipeline", pdata_pid: String = JobContext.jobName)(implicit className: String) = {
        val event = JSONUtils.serialize(getV3JobEvent("JOB_END", "INFO", msg, data, Option(status), pdata_id, pdata_pid));
        EventBusUtil.dipatchEvent(event);
        logEvent(event, name, INFO)
    }

    def log(msg: String, data: Option[AnyRef] = None, logLevel: Level = DEBUG, name: String = "org.ekstep.analytics")(implicit className: String) = {
        logLevel match {
            case INFO =>
                info(msg, data, name)
            case DEBUG =>
                debug(msg, data, name)
            case WARN =>
                warn(msg, data, name)
            case ERROR =>
                error(msg, data, name)
        }
    }

    def logEvent(event: String, name: String = "org.ekstep.analytics", logLevel: Level = DEBUG) = {
        if (StringUtils.equalsIgnoreCase(AppConf.getConfig("log.appender.kafka.enable"), "true")) {
            val brokerList = AppConf.getConfig("log.appender.kafka.broker_host")
            val topic = AppConf.getConfig("log.appender.kafka.topic")
            KafkaDispatcher.dispatch(Array(event), Map("brokerList" -> brokerList, "topic" -> topic))
        }
        else {
            logLevel match {
                case INFO =>
                    logger(name).info(event);
                case DEBUG =>
                    logger(name).debug(event);
                case WARN =>
                    logger(name).debug(event);
                case ERROR =>
                    logger(name).error(event);
            }
        }
    }

    private def getV3JobEvent(eid: String, level: String, msg: String, data: Option[AnyRef], status: Option[String] = None, pdata_id: String = "AnalyticsDataPipeline", pdata_pid: String = JobContext.jobName)(implicit className: String): V3DerivedEvent = {
        val measures = Map(
            "class" -> className,
            "level" -> level,
            "message" -> msg,
            "status" -> status,
            "data" -> data);
        val ts = new DateTime().getMillis
        val mid = CommonUtil.getMessageId(eid, level, ts, None, None);
        val context = V3Context("in.ekstep", Option(V3PData(pdata_id, Option("1.0"), Option(pdata_pid))), "analytics", None, None, None, None)
        V3DerivedEvent(eid, System.currentTimeMillis(), new DateTime().toString(CommonUtil.df3), "3.0", mid, Actor("", "System"), context, None, measures)
    }




}