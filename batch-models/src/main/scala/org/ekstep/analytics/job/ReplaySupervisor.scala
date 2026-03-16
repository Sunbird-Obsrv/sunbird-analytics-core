package org.ekstep.analytics.job

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, JobContext}
import org.ekstep.analytics.framework.Level._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.exception.{DataFetcherException, JobNotFoundException}
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}

object ReplaySupervisor extends optional.Application {

    implicit val className = "org.ekstep.analytics.job.ReplaySupervisor"

    def main(model: String, fromDate: String, toDate: String, config: String) {

        JobLogger.start("Started executing ReplaySupervisor", Option(Map("config" -> config, "model" -> model, "fromDate" -> fromDate, "toDate" -> toDate)))
        val con = JSONUtils.deserialize[JobConfig](config)
        val sparkCassandraConnectionHost = con.modelParams.getOrElse(Map()).get("sparkCassandraConnectionHost")
        val sc = CommonUtil.getSparkContext(JobContext.parallelization, con.appName.getOrElse(con.model), sparkCassandraConnectionHost);

        val storageKey = con.modelParams.getOrElse(Map()).getOrElse("storageKeyConfig", "azure_storage_key").asInstanceOf[String]
        val storageSecret = con.modelParams.getOrElse(Map()).getOrElse("storageSecretConfig", "azure_storage_secret").asInstanceOf[String]
        val fc = CommonUtil.getFrameworkContext(Option(Array((AppConf.getConfig("cloud_storage_type"), storageKey, storageSecret))));

        try {
            val result = CommonUtil.time({
                execute(model, fromDate, toDate, config)(sc, fc);
            })
            JobLogger.end("Replay Supervisor completed", "SUCCESS", Option(Map("timeTaken" -> result._1)));
        } catch {
            case ex: Exception =>
                JobLogger.log(ex.getMessage, None, ERROR);
                JobLogger.end("Replay Supervisor failed", "FAILED")
                throw ex
        } finally {
            CommonUtil.closeSparkContext()(sc);
        }
    }

    def execute(model: String, fromDate: String, toDate: String, config: String)(implicit sc: SparkContext, fc: FrameworkContext) {
        val dateRange = CommonUtil.getDatesBetween(fromDate, Option(toDate))
        for (date <- dateRange) {
            try {
                val jobConfig = config.replace("__endDate__", date)
                val job = JobFactory.getJob(model);
                JobLogger.log("### Executing replay for the date - " + date + " ###")
                job.main(jobConfig)(Option(sc), Option(fc))
            } catch {
                case ex: DataFetcherException => {
                    JobLogger.log(ex.getMessage, Option(Map("model_code" -> model, "date" -> date)), ERROR)
                }
                case ex: JobNotFoundException => {
                    JobLogger.log(ex.getMessage, Option(Map("model_code" -> model, "date" -> date)), ERROR)
                    throw ex;
                }
                case ex: Exception => {
                    JobLogger.log(ex.getMessage, Option(Map("model_code" -> model, "date" -> date)), ERROR)
                    ex.printStackTrace()
                }
            }
        }
    }
}