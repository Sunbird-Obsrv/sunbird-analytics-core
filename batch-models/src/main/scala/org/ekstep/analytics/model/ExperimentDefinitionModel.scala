package org.ekstep.analytics.model

import java.text.SimpleDateFormat
import java.util.Date
import java.sql.{BatchUpdateException, DriverManager, Timestamp}

import com.datastax.spark.connector._
import com.datastax.spark.connector.types.TimestampFormatter
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, SQLContext, SaveMode}
import org.ekstep.analytics.framework.Level.ERROR
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{IBatchModelTemplate, _}
import org.ekstep.analytics.util.Constants
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer


case class ExperimentDefinitionOutput(userId: Option[String] = None, deviceId: Option[String] = None, key: String, url: Option[String] = None, id: String
                                   , name: String, platform: String, userIdMod: Int = 0, deviceIdMod: Int = 0, expType: String, startDate: String,
                                   endDate: String, lastUpdatedOn: String) extends AlgoOutput with Output

case class UserList(count: Int, content: List[Map[String, AnyRef]])

case class UserResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: Map[String, UserList])

case class CriteriaModel(`type`: String, filters: AnyRef, modulus: Option[Int])

case class ExperimentDefinition(exp_id: String, exp_name: String, criteria: String, exp_data: String, status: String) extends AlgoInput with Input

case class ExperimentData(startDate: String, endDate: String, key: String, client: String, modulus: Option[Int])

case class ExperimentDefinitionMetadata(exp_id: String, status: String, status_message: String, updated_on: Timestamp, stats: String, updated_by: String)

object ExperimentDefinitionModel extends IBatchModelTemplate[Empty, ExperimentDefinition, ExperimentDefinitionOutput, ExperimentDefinitionOutput] with Serializable {

    implicit val className: String = "org.ekstep.analytics.model.ExperimentDefinitionModel"

    override def name: String = "ExperimentDefinitionModel"
    implicit val utils: ExperimentDataUtils = new ExperimentDataUtils

    override def preProcess(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[ExperimentDefinition] = {

        // Get experiments from postgres
        val experiments = utils.getExprimentData(Constants.EXPERIMENT_DEFINITION_TABLE)
          .filter(metadata => metadata.status.equalsIgnoreCase("SUBMITTED"))
        experiments
    }

    override def algorithm(experiments: RDD[ExperimentDefinition], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[ExperimentDefinitionOutput] = {

        val metadata: ListBuffer[ExperimentDefinitionMetadata] = ListBuffer()
        val result = algorithmProcess(experiments, metadata)
        // save to postgres
        utils.saveExperimentMetaData(Constants.EXPERIMENT_DEFINITION_TABLE, sc.parallelize(metadata))
        result.fold(sc.emptyRDD[ExperimentDefinitionOutput])(_ ++ _)

    }


    override def postProcess(data: RDD[ExperimentDefinitionOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[ExperimentDefinitionOutput] = {
        data
    }

    def algorithmProcess(experiments: RDD[ExperimentDefinition], metadata: ListBuffer[ExperimentDefinitionMetadata])
                        (implicit sc: SparkContext, util: ExperimentDataUtils): Array[RDD[ExperimentDefinitionOutput]] = {
        val experimentList = experiments.collect()
        val deviceProfile = util.getDeviceProfile(Constants.DEVICE_PROFILE_TABLE)

        val result = experimentList.map(exp => {
            val criteria = JSONUtils.deserialize[CriteriaModel](exp.criteria)
            val filterType = criteria.`type`

            try {
                filterType match {
                    case "user" | "user_mod" =>
                        val userResponse = util.getUserDetails(JSONUtils.serialize(criteria.filters))
                        if (null != userResponse && !userResponse.responseCode.isEmpty && userResponse.responseCode.equalsIgnoreCase("OK")) {
                            userResponse.result.get("response").map { userResult =>
                                metadata ++= Seq(populateExperimentMetadata(exp, userResult.content.size, filterType,
                                    "ACTIVE", "Experiment Mapped Sucessfully"))
                                sc.parallelize(userResult.content.map(user =>
                                    populateExperimentMapping(user.get("id").asInstanceOf[Option[String]], exp, filterType)))
                            }.get
                        } else {
                            metadata ++= Seq(populateExperimentMetadata(exp, 0, filterType, "FAILED",
                                "Experiment Failed, Please Check the criteria"))
                            sc.emptyRDD[ExperimentDefinitionOutput]
                        }

                    case "device" | "device_mod" =>
                        val filters = criteria.filters.asInstanceOf[List[Map[String, AnyRef]]].
                          map(f => Filter(f("name").asInstanceOf[String], f("operator").asInstanceOf[String], f.get("value")))
                        val filteredProfile = DataFilter.filter(deviceProfile, filters.toArray)
                        val deviceRDD = filteredProfile.map(z => populateExperimentMapping(z.device_id, exp, filterType))
                        metadata ++= Seq(populateExperimentMetadata(exp, deviceRDD.count(), filterType, "ACTIVE", "Experiment Mapped Sucessfully"))
                        deviceRDD
                }
            } catch {
                case ex: Exception =>
                    JobLogger.log(ex.getMessage, None, ERROR)
                    metadata ++= Seq(populateExperimentMetadata(exp, 0, filterType, "FAILED", "Experiment Failed : " + ex.getMessage))
                    ex.printStackTrace()
                    sc.emptyRDD[ExperimentDefinitionOutput]
            }
        })
        result
    }

    private def populateExperimentMapping(mappingId: Option[String], exp: ExperimentDefinition, expType: String): ExperimentDefinitionOutput = {
        val expData = JSONUtils.deserialize[ExperimentData](exp.exp_data)
        val modulus = expData.modulus.getOrElse(0)
        val outputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
        val inputFormat = new SimpleDateFormat("yyyy-MM-dd")
        if (expType.contains("user"))
            ExperimentDefinitionOutput(userId = mappingId,
                id = exp.exp_id, name = exp.exp_name, platform = expData.client, key = expData.key, expType = expType,
                startDate = outputFormat.format(inputFormat.parse(expData.startDate)),
                endDate = outputFormat.format(inputFormat.parse(expData.endDate)),
                lastUpdatedOn = outputFormat.format(inputFormat.parse(TimestampFormatter.format(new Date))),
                userIdMod = modulus)
        else
            ExperimentDefinitionOutput(deviceId = mappingId,
                id = exp.exp_id, name = exp.exp_name, platform = expData.client, key = expData.key, expType = expType,
                startDate = outputFormat.format(inputFormat.parse(expData.startDate)),
                endDate = outputFormat.format(inputFormat.parse(expData.endDate)),
                lastUpdatedOn = outputFormat.format(inputFormat.parse(TimestampFormatter.format(new Date))),
                deviceIdMod = modulus)
    }

    private def populateExperimentMetadata(exp: ExperimentDefinition, mappedCount: Long, expType: String, status: String, status_msg: String): ExperimentDefinitionMetadata = {
        val stats = Map(expType + "Matched" -> mappedCount)
        ExperimentDefinitionMetadata(exp.exp_id, status, status_msg, new Timestamp(System.currentTimeMillis()), JSONUtils.serialize(stats), "ExperimentDataProduct")
    }
}

class ExperimentDataUtils {

    def getUserDetails[T](request_filter: String)(implicit mf: Manifest[T]): UserResponse = {
        val user_search_limit = AppConf.getConfig("user.search.limit")
        val request =
            s"""
               |{
               |  "request": {
               |    "filters": $request_filter,
               |    "limit": $user_search_limit
               |  }
               |}
               |""".stripMargin

        val userResponse = RestUtil.post[UserResponse](Constants.USER_SEARCH_URL, request)
        userResponse
    }

    def getDeviceProfile(table: String)(implicit sc: SparkContext): RDD[DeviceProfileModel] = {
        val db = AppConf.getConfig("postgres.db")
        val url = AppConf.getConfig("postgres.url") + s"$db"
        implicit val sqlContext = new SQLContext(sc)
        val encoder = Encoders.product[DeviceProfileModel]
        sqlContext.sparkSession.read.jdbc(url, table, CommonUtil.getPostgresConnectionProps()).as[DeviceProfileModel](encoder).rdd
    }

    def saveExperimentMetaData(table: String, data: RDD[ExperimentDefinitionMetadata])(implicit sc: SparkContext): Unit = {
        val queries = data.map{f =>
            val dataMap = JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(f))
            val filteredDataMap = dataMap.--(List("updated_on"))
            val finalDataMap = filteredDataMap ++ Map("updated_on" -> f.updated_on)
            val columns = finalDataMap.keySet.mkString(",")
            val values = finalDataMap.values.mkString("','")
            s"""INSERT INTO $table ($columns) VALUES ('$values') ON CONFLICT (exp_id) DO UPDATE SET ($columns) = ('$values')"""
        }
        dispatchEventsToPostgres(queries);
    }

    def dispatchEventsToPostgres(queries: RDD[String]): Unit = {
        val connProperties = CommonUtil.getPostgresConnectionProps
        val user = connProperties.getProperty("user")
        val pass = connProperties.getProperty("password")
        val db = AppConf.getConfig("postgres.db")
        val url = AppConf.getConfig("postgres.url") + s"$db"

        queries
          .foreachPartition { (rddpartition: Iterator[String]) =>
              val connection = DriverManager.getConnection(url, user, pass)
              var statement = connection.createStatement()
              rddpartition.foreach { (row: String) =>
                  statement.addBatch(row)
              }
              statement.executeBatch()
              statement.close()
              connection.close()
          }
    }

    def getExprimentData(table: String)(implicit sc: SparkContext): RDD[ExperimentDefinition] = {
        val db = AppConf.getConfig("postgres.db")
        val url = AppConf.getConfig("postgres.url") + s"$db"
        implicit val sqlContext = new SQLContext(sc)
        val encoder = Encoders.product[ExperimentDefinition]
        sqlContext.sparkSession.read.jdbc(url, table, CommonUtil.getPostgresConnectionProps()).as[ExperimentDefinition](encoder).rdd
    }
}