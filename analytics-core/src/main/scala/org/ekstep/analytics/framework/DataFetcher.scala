package org.ekstep.analytics.framework

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.exception.DataFetcherException
import org.ekstep.analytics.framework.fetcher.{AzureDataFetcher, DruidDataFetcher, S3DataFetcher}
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger}

/**
 * @author Santhosh
 */
object DataFetcher {

    implicit val className = "org.ekstep.analytics.framework.DataFetcher"
    @throws(classOf[DataFetcherException])
    def fetchBatchData[T](search: Fetcher)(implicit mf: Manifest[T], sc: SparkContext, fc: FrameworkContext): RDD[T] = {

        JobLogger.log("Fetching data", Option(Map("query" -> search)))
        if (search.queries.isEmpty && search.druidQuery.isEmpty) {
            if (search.`type`.equals("none")) return sc.emptyRDD[T]
            throw new DataFetcherException("Data fetch configuration not found")
        }
        //val date = search.queries.get.last.endDate
        val keys = search.`type`.toLowerCase() match {
            case "s3" =>
                JobLogger.log("Fetching the batch data from S3")
                S3DataFetcher.getObjectKeys(search.queries.get);
            case "azure" =>
                JobLogger.log("Fetching the batch data from AZURE")
                AzureDataFetcher.getObjectKeys(search.queries.get);
            case "local" =>
                JobLogger.log("Fetching the batch data from Local file")
                search.queries.get.map { x => x.file.getOrElse("") }.filterNot { x => x == null };
            case "druid" =>
                JobLogger.log("Fetching the batch data from Druid")
                val data = DruidDataFetcher.getDruidData(search.druidQuery.get)
                val druidDataList = data.map(f => JSONUtils.deserialize[T](f))
                return sc.parallelize(druidDataList);
            case _ =>
                throw new DataFetcherException("Unknown fetcher type found");
        }
        if (null == keys || keys.length == 0) {
            return sc.parallelize(Seq[T](), JobContext.parallelization);
        }
        JobLogger.log("Deserializing Input Data", None, INFO);
        val isString = mf.runtimeClass.getName.equals("java.lang.String");
        sc.textFile(keys.mkString(","), JobContext.parallelization).map { line => {
            try {
                if (isString) line.asInstanceOf[T] else JSONUtils.deserialize[T](line);
            } catch {
                case ex: Exception =>
                    JobLogger.log(ex.getMessage, None, INFO);
                    null.asInstanceOf[T]
                }
            }
        }.filter { x => x != null };
    }

    /**
     * API to fetch the streaming data given an array of query objects
     */
    def fetchStreamData[T](sc: StreamingContext, search: Fetcher)(implicit mf: Manifest[T]): DStream[T] = {
        null;
    }

}