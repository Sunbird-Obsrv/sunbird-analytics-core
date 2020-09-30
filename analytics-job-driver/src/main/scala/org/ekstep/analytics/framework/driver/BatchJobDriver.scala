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
                val storageKey = config.modelParams.getOrElse(Map()).getOrElse("storageKeyConfig", "azure_storage_key").asInstanceOf[String]
                val storageSecret = config.modelParams.getOrElse(Map()).getOrElse("storageSecretConfig", "azure_storage_secret").asInstanceOf[String]
                CommonUtil.getFrameworkContext(Option(Array((AppConf.getConfig("cloud_storage_type"), storageKey, storageSecret))));
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
        
        fc.inputEventsCount = sc.longAccumulator("InputEventsCount");
        fc.outputEventsCount = sc.longAccumulator("OutputEventsCount");
        val rdd = DataFetcher.fetchBatchData[T](config.search);
        val data = DataFilter.filterAndSort[T](rdd, config.filters, config.sort);
        models.foreach { model =>
            // TODO: It is not necessary that the end date always exists. The below log statement might throw exceptions
            // $COVERAGE-OFF$
            fc.outputEventsCount.reset();
            val endDate = config.search.queries.getOrElse(Array(Query())).last.endDate
            // $COVERAGE-ON$
            val modelName = if(config.modelParams.nonEmpty && config.modelParams.get.get("modelName").nonEmpty)
                config.modelParams.get.get("modelName").get.asInstanceOf[String]
            else model.name
            JobContext.jobName = modelName
            JobLogger.start("Started processing of " + modelName, Option(Map("config" -> config, "model" -> model.name, "date" -> endDate)));
            try {
                val result = _processModel(config, data, model);

                // generate metric event and push it to kafka topic
                val metrics = List(Map("id" -> "input-events", "value" -> fc.inputEventsCount.value.asInstanceOf[AnyRef]), Map("id" -> "output-events", "value" -> result._2.asInstanceOf[AnyRef]), Map("id" -> "time-taken-secs", "value" -> Double.box(result._1 / 1000).asInstanceOf[AnyRef]))
                val metricEvent = getMetricJson(model.name, endDate, "SUCCESS", metrics)
                // $COVERAGE-OFF$
                if (AppConf.getConfig("push.metrics.kafka").toBoolean)
                    KafkaDispatcher.dispatch(Array(metricEvent), Map("topic" -> AppConf.getConfig("metric.kafka.topic"), "brokerList" -> AppConf.getConfig("metric.kafka.broker")))
                // $COVERAGE-ON$

                JobLogger.end(modelName + " processing complete", "SUCCESS", Option(Map("model" -> model.name, "date" -> endDate, "inputEvents" -> fc.inputEventsCount.value, "outputEvents" -> result._2, "timeTaken" -> Double.box(result._1 / 1000))));
            } catch {
                case ex: Exception =>
                    JobLogger.log(ex.getMessage, None, ERROR);
                    JobLogger.end(modelName + " processing failed", "FAILED", Option(Map("model" -> model.name, "date" -> endDate, "statusMsg" -> ex.getMessage)));
                    val metricEvent = getMetricJson(model.name, endDate, "FAILED", List(Map("id" -> "input-events", "value" -> fc.inputEventsCount.value.asInstanceOf[AnyRef])))
                    // $COVERAGE-OFF$
                    if (AppConf.getConfig("push.metrics.kafka").toBoolean)
                        KafkaDispatcher.dispatch(Array(metricEvent), Map("topic" -> AppConf.getConfig("metric.kafka.topic"), "brokerList" -> AppConf.getConfig("metric.kafka.broker")))
                    // $COVERAGE-ON$
                    ex.printStackTrace();
            }
        }
    }

    private def _processModel[T, R](config: JobConfig, data: RDD[T], model: IBatchModel[T, R])(implicit mf: Manifest[T], mfr: Manifest[R], sc: SparkContext, fc: FrameworkContext): (Long, Long) = {

        CommonUtil.time({
            val output = model.execute(data, config.modelParams);
            val count = OutputDispatcher.dispatch(config.output, output);
            fc.outputEventsCount.value
        })

    }

    def getMetricJson(subsystem: String, endDate: Option[String], status: String, metrics: List[Map[String, AnyRef]]): String = {
        // $COVERAGE-OFF$
        val date = if (endDate.isEmpty) new DateTime().toString(CommonUtil.dateFormat) else endDate.get
        // $COVERAGE-ON$
        val dims = List(Map("id" -> "report-date", "value" -> date), Map("id" -> "status", "value" -> status))
        val metricEvent = Map("metricts" -> System.currentTimeMillis(), "system" -> "DataProduct", "subsystem" -> subsystem, "metrics" -> metrics, "dimensions" -> dims)
        JSONUtils.serialize(metricEvent)
    }
}