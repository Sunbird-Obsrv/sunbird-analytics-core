package org.ekstep.analytics.framework

import org.apache.spark.sql.{Dataset, SparkSession}



trait ReportOnDemandModel[T] {

    def execute(reportParams: Option[Map[String, AnyRef]])(implicit spark: SparkSession, fc: FrameworkContext) : Unit

    def getJobRequest(jobId: String) (implicit  sparkSession: SparkSession, fc: FrameworkContext): Dataset[T]

    def updateJobRequest(reportBlobs :  Dataset[T]) (implicit  sparkSession: SparkSession, fc: FrameworkContext): Unit

    def name() : String = "OnDemandReportModel";


}