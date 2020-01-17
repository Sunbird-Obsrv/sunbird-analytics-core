package org.ekstep.analytics.framework

import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.driver.BatchJobDriver
import org.ekstep.analytics.framework.driver.StreamingJobDriver
import org.ekstep.analytics.framework.util.JSONUtils
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonMappingException
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.log4j.Logger
import org.json4s.JsonUtil
import org.apache.logging.log4j.LogManager
import org.ekstep.analytics.framework.Level._

/**
 * @author Santhosh
 */
object JobDriver {

    implicit val className = "org.ekstep.analytics.framework.JobDriver"
    
    @throws(classOf[Exception])
    def run[T, R](t: String, config: String, model: IBatchModel[T, R])(implicit mf: Manifest[T], mfr: Manifest[R], sc: Option[SparkContext], fc: Option[FrameworkContext]) {
        JobLogger.init(model.getClass.getName.split("\\$").last);
        try {
            val jobConfig = JSONUtils.deserialize[JobConfig](config);
            t match {
                case "batch" =>
                    BatchJobDriver.process[T, R](jobConfig, model);
                case "streaming" =>
                    StreamingJobDriver.process(jobConfig);
                case _ =>
                    throw new Exception("Unknown job type")
            }
        } catch {
            case e: JsonMappingException =>
                JobLogger.log(e.getMessage, None, ERROR)
                throw e;
            case e: Exception =>
                JobLogger.log(e.getMessage, None, ERROR)
                throw e;
        }
    }
    
    def run[T, R](t: String, config: String, models: List[IBatchModel[T, R]], jobName: String)(implicit mf: Manifest[T], mfr: Manifest[R], sc: Option[SparkContext], fc: Option[FrameworkContext]) {
        JobLogger.init(jobName);
        try {
            val jobConfig = JSONUtils.deserialize[JobConfig](config);
            t match {
                case "batch" =>
                    BatchJobDriver.process[T, R](jobConfig, models);
                case "streaming" =>
                    StreamingJobDriver.process(jobConfig);
                case _ =>
                    throw new Exception("Unknown job type")
            }
        } catch {
            case e: JsonMappingException =>
                JobLogger.log(e.getMessage, None, ERROR)
                throw e;
            case e: Exception =>
                JobLogger.log(e.getMessage, None, ERROR)
                throw e;
        }
    }

}