package org.ekstep.analytics.framework

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger

/**
 * @author Santhosh
 */
object JobRunner {

    implicit val className = "org.ekstep.analytics.framework.JobRunner"

    def executeBatch[T](jobClass: String, sc: SparkContext, events: RDD[T], jobParams: Option[Map[String, AnyRef]])(implicit mf: Manifest[T]): RDD[String] = {
        JobLogger.log("Executing the job", Option(Map("jobClass" -> jobClass)));
        val model = Class.forName(jobClass).newInstance.asInstanceOf[{ def execute(sc: SparkContext, events: RDD[T], jobParams: Option[Map[String, AnyRef]]): RDD[String] }]; ;
        model.execute(sc, events, jobParams);
    }
}