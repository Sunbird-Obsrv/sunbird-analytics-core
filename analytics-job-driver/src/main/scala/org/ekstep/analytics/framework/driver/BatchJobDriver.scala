package org.ekstep.analytics.framework.driver

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.Level._
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.joda.time.DateTime

/**
 * @author Santhosh
 */
object BatchJobDriver {

    implicit val className = "org.ekstep.analytics.framework.driver.BatchJobDriver"
    implicit val fc = new FrameworkContext();
    
    case class Context(spark: SparkContext, fc: FrameworkContext, autocloseSpark: Boolean, autocloseFC: Boolean )
    
    def process[T, R](config: JobConfig, model: IBatchModel[T, R])(implicit mf: Manifest[T], mfr: Manifest[R], sc: Option[SparkContext], fc: Option[FrameworkContext]) {
        process(config, List(model));
    }

    def process[T, R](config: JobConfig, models: List[IBatchModel[T, R]])(implicit mf: Manifest[T], mfr: Manifest[R], sc: Option[SparkContext], fc: Option[FrameworkContext]) {
        JobContext.parallelization = CommonUtil.getParallelization(config);
        
        val context: Context = {
            val spark = if (sc.isEmpty) {
                val sparkCassandraConnectionHost = config.modelParams.getOrElse(Map()).get("sparkCassandraConnectionHost")
                val sparkElasticsearchConnectionHost = config.modelParams.getOrElse(Map()).get("sparkElasticsearchConnectionHost")
                CommonUtil.getSparkContext(JobContext.parallelization, config.appName.getOrElse(config.model), sparkCassandraConnectionHost,sparkElasticsearchConnectionHost)
            } else {
                sc.get
            }
            val autocloseSC = if (sc.isEmpty) true else false;
            val frameworkContext = if (fc.isEmpty) {
                CommonUtil.getFrameworkContext(Option(Array((AppConf.getConfig("cloud_storage_type"), AppConf.getConfig("cloud_storage_type"), AppConf.getConfig("cloud_storage_type")))));
            } else {
                fc.get
            }
            val autocloseFC = if (fc.isEmpty) true else false;
            Context(spark, frameworkContext, autocloseSC, autocloseFC)
        }
        try {
            implicit val sparkContext = context.spark;
            implicit val fContext = context.fc;
            _process(config, models);
        } finally {
            if(context.autocloseSpark) CommonUtil.closeSparkContext()(context.spark);
            if(context.autocloseFC) context.fc.closeContext();
            // Clearing previous job persisting rdd, in case of the job got failed
            if (JobContext.rddList.nonEmpty) JobContext.rddList.clear()
        }
    }

    private def _process[T, R](config: JobConfig, models: List[IBatchModel[T, R]])(implicit mf: Manifest[T], mfr: Manifest[R], sc: SparkContext, fc: FrameworkContext) {
        
        val rdd = DataFetcher.fetchBatchData[T](config.search).cache();
        val count = rdd.count;
        val data = DataFilter.filterAndSort[T](rdd, config.filters, config.sort);
        models.foreach { model =>
            JobContext.jobName = model.name
            // TODO: It is not necessary that the end date always exists. The below log statement might throw exceptions
            val endDate = config.search.queries.getOrElse(Array(Query())).last.endDate
            JobLogger.start("Started processing of " + model.name, Option(Map("config" -> config, "model" -> model.name, "date" -> endDate)));
            try {
                val result = _processModel(config, data, model);

                // generate metric event and push it to kafka topic
                val date = if (endDate.isEmpty) new DateTime().toString(CommonUtil.dateFormat) else endDate.get
                val metrics = List(V3MetricEdata("date", date.asInstanceOf[AnyRef]), V3MetricEdata("inputEvents", count.asInstanceOf[AnyRef]),
                    V3MetricEdata("outputEvents", result._2.asInstanceOf[AnyRef]), V3MetricEdata("timeTakenSecs", Double.box(result._1 / 1000).asInstanceOf[AnyRef]))
                val metricEvent = CommonUtil.getMetricEvent(Map("system" -> "DataProduct", "subsystem" -> model.name, "metrics" -> metrics), AppConf.getConfig("metric.producer.id"), AppConf.getConfig("metric.producer.pid"))
                if (AppConf.getConfig("push.metrics.kafka").toBoolean)
                    KafkaDispatcher.dispatch(Array(JSONUtils.serialize(metricEvent)), Map("topic" -> AppConf.getConfig("metric.kafka.topic"), "brokerList" -> AppConf.getConfig("metric.kafka.broker")))

                JobLogger.end(model.name + " processing complete", "SUCCESS", Option(Map("model" -> model.name, "date" -> endDate, "inputEvents" -> count, "outputEvents" -> result._2, "timeTaken" -> Double.box(result._1 / 1000))));
            } catch {
                case ex: Exception =>
                    JobLogger.log(ex.getMessage, None, ERROR);
                    JobLogger.end(model.name + " processing failed", "FAILED", Option(Map("model" -> model.name, "date" -> endDate, "inputEvents" -> count, "statusMsg" -> ex.getMessage)));
                    ex.printStackTrace();
            } finally {
                rdd.unpersist()
            }
        }
    }

    private def _processModel[T, R](config: JobConfig, data: RDD[T], model: IBatchModel[T, R])(implicit mf: Manifest[T], mfr: Manifest[R], sc: SparkContext, fc: FrameworkContext): (Long, Long) = {

        CommonUtil.time({
            val output = model.execute(data, config.modelParams);
            // JobContext.recordRDD(output);
            val count = OutputDispatcher.dispatch(config.output, output);
            // JobContext.cleanUpRDDs();
            count;
        })

    }
}