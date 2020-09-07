package org.ekstep.analytics.framework

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}



trait ReportOnDemandModel[T,R] {

    def execute(reportParams: Option[Map[String, AnyRef]])(implicit spark: SparkSession, fc: FrameworkContext) : DataFrame

    def getReportConfigs(datasetId: String) (implicit  sparkSession: SparkSession, fc: FrameworkContext): Dataset[T]

    def saveReportLocations(reportBlobs :  Dataset[R]) (implicit  sparkSession: SparkSession, fc: FrameworkContext): DataFrame

    def name() : String = "OnDemandReportModel";


}