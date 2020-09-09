package org.ekstep.analytics.framework

import java.sql.DriverManager
import java.util.{Date, Properties}

import org.apache.spark.sql._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.CommonUtil


trait ReportOnDemandModelTemplate[A <: AnyRef, B <: AnyRef] extends ReportOnDemandModel[OnDemandJobRequest] {

    val connProperties: Properties = CommonUtil.getPostgresConnectionProps()
    val db: String = AppConf.getConfig("postgres.db")
    val url: String = AppConf.getConfig("postgres.url") + s"$db"
    val report_config_table: String = AppConf.getConfig("postgres.table.job_request")

    /**
      * Override and implement the data product execute method,
      * 1. filterReports
      * 2. generateReports
      * 3. saveReports
      */
    override def execute(reportParams: Option[Map[String, AnyRef]])(implicit spark: SparkSession, fc: FrameworkContext) = {

        val config = reportParams.getOrElse(Map[String, AnyRef]())

        val reportConfigList = getJobRequest(config.getOrElse("jobId", "").asInstanceOf[String])

        val filteredReports = filterReports(reportConfigList, config)

        val generatedReports = generateReports(filteredReports, config)

        val savedReportsList = saveReports(generatedReports, config)

        updateJobRequest(savedReportsList)

    }

    /**
      * Method will get the list of active on demand reports from table
      *
      * @param jobId job id of the report
      * @param spark sparkSession implicit
      * @param fc    framework context
      * @return
      */
    override def getJobRequest(jobId: String)(implicit spark: SparkSession, fc: FrameworkContext): Dataset[OnDemandJobRequest] = {

        val encoder = Encoders.product[OnDemandJobRequest]
        import org.apache.spark.sql.functions.col
        val reportConfigsDf = spark.sqlContext.sparkSession.read.jdbc(url, report_config_table, connProperties)
          .where(col("job_id") === jobId).where(col("status") === "SUBMITTED")
          .select("request_id", "request_data","download_urls","status")
        reportConfigsDf.as[OnDemandJobRequest](encoder)
    }


    /**
      * Method will save the list of report location urls for each request id
      *
      * @param reportLocationsDf Dataset with list of report paths per request id
      * @param spark             sparkSession implict
      * @param fc                frameWorkContext implicit
      * @return
      */
    override def updateJobRequest(reportLocationsDf: Dataset[OnDemandJobRequest])(implicit spark: SparkSession, fc: FrameworkContext) = {
        val connProperties: Properties = CommonUtil.getPostgresConnectionProps()
        val db: String = AppConf.getConfig("postgres.db")
        val url: String = AppConf.getConfig("postgres.url") + s"$db"
        val report_config_table: String = AppConf.getConfig("postgres.table.job_request")
        val user = connProperties.getProperty("user")
        val pass = connProperties.getProperty("password")
        reportLocationsDf.rdd.foreachPartition { rddPartition: Iterator[OnDemandJobRequest] =>
            val connection = DriverManager.getConnection(url, user, pass)
            val statement = connection.createStatement()
            rddPartition.foreach { report =>
                val url = report.download_urls.mkString(",")
                val row =
                    s""" UPDATE ${report_config_table} SET download_urls = '{${url}}',
                       |dt_job_completed = '${new Date()}',status = 'COMPLETED' where request_id='${report.request_id}' """.stripMargin
                statement.addBatch(row)
            }
            statement.executeBatch()
            statement.close()
            connection.close()
        }

    }

    /**
      * filter Reports steps before generating Report. Few pre-process steps are
      * 1. Combine or filter the report configs an
      * 2. Join or fetch Data from Tables
      */
    def filterReports(reportConfigs: Dataset[OnDemandJobRequest], config: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext): Dataset[A]

    /**
      * Method which will generate report
      * Input : List of Filtered Ids to generate Report
      * Output : List of Files to be saved per request
      */
    def generateReports(reports: Dataset[A], config: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext): Dataset[B]

    /**
      * .
      * 1. Saving Reports to Blob
      * 2. Generate Metrics
      * 3. Return Map list of blobs to RequestIds as per the request
      */
    def saveReports(reports: Dataset[B], config: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext): Dataset[OnDemandJobRequest]

}