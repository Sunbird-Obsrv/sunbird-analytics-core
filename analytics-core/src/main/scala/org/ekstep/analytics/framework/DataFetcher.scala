package org.ekstep.analytics.framework

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.exception.DataFetcherException
import org.ekstep.analytics.framework.fetcher.{AzureDataFetcher, DruidDataFetcher, S3DataFetcher, CephS3DataFetcher, GcloudDataFetcher}
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}

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
            case "cephs3" =>
                JobLogger.log("Fetching the batch data from S3-like stores")
                CephS3DataFetcher.getObjectKeys(search.queries.get);
            case "s3" =>
                JobLogger.log("Fetching the batch data from S3")
                S3DataFetcher.getObjectKeys(search.queries.get);
            case "azure" =>
                JobLogger.log("Fetching the batch data from AZURE")
                AzureDataFetcher.getObjectKeys(search.queries.get);
            case "gcloud" =>
                JobLogger.log("Fetching the batch data from Google Cloud")
                GcloudDataFetcher.getObjectKeys(search.queries.get);
            case "local" =>
                JobLogger.log("Fetching the batch data from Local file")
                search.queries.get.map { x => x.file.getOrElse(null) }.filterNot { x => x == null };
            case "druid" =>
                JobLogger.log("Fetching the batch data from Druid")
                val data = DruidDataFetcher.getDruidData(search.druidQuery.get)
                // $COVERAGE-OFF$
                // Disabling scoverage as the below code cannot be covered as DruidDataFetcher is not mockable being an object and embedded druid is not available yet
                val druidDataList = data.map(f => JSONUtils.deserialize[T](f))
                return druidDataList
            // $COVERAGE-ON$
            case _ =>
                throw new DataFetcherException("Unknown fetcher type found");
        }

        if (null == keys || keys.length == 0) {
            return sc.parallelize(Seq[T](), JobContext.parallelization);
        }
        JobLogger.log("Deserializing Input Data", None, INFO);
        val filteredKeys = search.queries.get.map{q =>
            getFilteredKeys(q, keys, q.partitions)
        }.flatMap(f => f)

        val isString = mf.runtimeClass.getName.equals("java.lang.String");
        val inputEventsCount = fc.inputEventsCount;
        sc.textFile(filteredKeys.mkString(","), JobContext.parallelization).map { line => {
            try {
                inputEventsCount.add(1);
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

    def getFilteredKeys(query: Query, keys: Array[String], partitions: Option[List[Int]]): Array[String] = {
        if (partitions.nonEmpty) {
            val finalKeys = keys.map{f =>
                partitions.get.map{p =>
                    val reg = raw"(\d{4})-(\d{2})-(\d{2})-$p-".r.findFirstIn(f)
                    if(reg.nonEmpty && f.contains(reg.get)) f else ""
                }
            }.flatMap(f => f)
            finalKeys.filter(f => f.nonEmpty)
        }
        else keys
    }
}