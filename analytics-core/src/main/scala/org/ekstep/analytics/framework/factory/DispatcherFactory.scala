package org.ekstep.analytics.framework.factory

import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.dispatcher._
import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.Level._
import org.ekstep.analytics.framework.StorageConfig

/**
 * @author Santhosh
 */
object DispatcherFactory {

    @throws(classOf[DispatcherException])
    def getDispatcher(disp: Dispatcher): IDispatcher = {
        disp.to.toLowerCase() match {
            case "s3" =>
                S3Dispatcher;
            case "kafka" =>
                KafkaDispatcher;
            case "script" =>
                ScriptDispatcher;
            case "console" =>
                ConsoleDispatcher;
            case "file" =>
                FileDispatcher;
            case "slack" =>
                SlackDispatcher;
            case "azure" =>
                AzureDispatcher;
            case "elasticsearch" =>
                ESDispatcher;
            case "gcloud" =>
                GcloudDispatcher;
            case _ =>
                throw new DispatcherException("Unknown output dispatcher destination found");
        }
    }
    
    @throws(classOf[DispatcherException])
    def getDispatcher(config: StorageConfig): IDispatcher = {
        config.store.toLowerCase() match {
            case "s3" =>
                S3Dispatcher;
            case "local" =>
                FileDispatcher;
            case "azure" =>
                AzureDispatcher;
            case "gcloud" =>
                GcloudDispatcher;
            case _ =>
                throw new DispatcherException("Unknown output dispatcher destination found");
        }
    }
}