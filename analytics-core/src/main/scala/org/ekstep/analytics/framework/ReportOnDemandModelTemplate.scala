package org.ekstep.analytics.framework

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


trait ReportOnDemandModelTemplate[  A <: AnyRef, B <: AnyRef, R <: AnyRef] extends ReportOnDemandModel[ReportConfigs,R] {

    /**
     * Override and implement the data product execute method,
     * 1. filterReports
     * 2. generateReports
     * 3. saveReports
     */


    override def execute(reportParams: Option[Map[String, AnyRef]])(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = {

        val config = reportParams.getOrElse(Map[String, AnyRef]())

        val reportConfigList = getReportConfigs(config.getOrElse("jobId","").asInstanceOf[String])

        val filteredReports= filterReports(reportConfigList,config)

        val generatedReports = generateReports(filteredReports,config)

        val savedReportsList = saveReports(generatedReports,config)




        saveReportLocations(savedReportsList)

    }

    /**
      * Method will get the list the active on demand reports from table
      * @param jobId
      * @return
      */
    override def getReportConfigs(jobId: String) (implicit spark: SparkSession, fc: FrameworkContext): Dataset[ReportConfigs] = {
        val config =s"""{"batchFilters":["TPD","NCFCOPY"],"contentFilters":{"request":{"filters":{"identifier":["do_11305960936384921612216","do_1130934466492252161819"],"prevState":"Draft"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel","prevState"]}},"reportPath":"course-progress-v2/"}"""

        import spark.implicits._

        spark.createDataset(List(ReportConfigs("1234",List(config))))
    }





    /**
      * Method will save the list of report blob paths for each request id
 *
      * @param savedReports - List of saved Reports with
      * @return
      */
    override def saveReportLocations(requestedReports: Dataset[R])(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = {
        null
    }

    /**
     * filter Reports steps before generating Report. Few pre-process steps are
     * 1. Combine or filter the report configs an
     * 2. Join or fetch Data from Tables
     */
    def filterReports(reportConfigs: Dataset[ReportConfigs], config: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext): Dataset[A]

    /**
     * Method which will generate report
      * Input : List of Filtered Ids to generate Report
      * Output : List of Files to be saved per request
     */
    def generateReports(filteredReports: Dataset[A], config: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext): Dataset[B]

    /**
     * .
     * 1. Saving Reports to Blob
     * 2. Generate Metrics
     * 3. Return Map list of blobs to RequestIds as per the request
     */
    def saveReports(generatedreports: Dataset[B], config: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext): Dataset[R]

}