package org.ekstep.analytics.framework.dispatcher

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.Output
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.util.{JSONUtils, RestUtil}
import org.ekstep.analytics.framework.FrameworkContext


case class SlackMessage(channel: String, username: String, text: Option[String] = None, attachments: Option[Array[Attachments]] = None, icon_emoji:String = ":ghost:")
case class Fields(title: String, value: String, short: Boolean)
case class Attachments(mrkdwn_in: String, fallback: String, pretext: String, title: String, title_link: String, text: String, color: String, fields: Array[Fields]) extends Output
/**
 * @author Santhosh
 */
object SlackDispatcher extends IDispatcher {

    @throws(classOf[DispatcherException])
    def dispatch(events: Array[String], config: Map[String, AnyRef])(implicit fc: FrameworkContext): Array[String] = {
        
        val channel = config.getOrElse("channel", null).asInstanceOf[String]
        val userName = config.getOrElse("userName", null).asInstanceOf[String]
        val hasAttachments = config.getOrElse("attachments", "false").asInstanceOf[String]

        if (null == channel || null == userName) {
            throw new DispatcherException("'channel' & 'userName' parameters are required to send output to slack")
        }

        val webhookUrl = AppConf.getConfig("monitor.notification.webhook_url")
        val message =  if (hasAttachments.equalsIgnoreCase("true")) {
            SlackMessage(channel, userName, attachments = Some(events.map(JSONUtils.deserialize[Attachments](_))))
        } else SlackMessage(channel, userName, text = Some(events.mkString(",")))
        val resp = RestUtil.post[String](webhookUrl, JSONUtils.serialize(message))
        events
    }
    
    def dispatch(config: Map[String, AnyRef], events: RDD[String])(implicit sc: SparkContext, fc: FrameworkContext) = {
        dispatch(events.collect(), config)
    }
    
}