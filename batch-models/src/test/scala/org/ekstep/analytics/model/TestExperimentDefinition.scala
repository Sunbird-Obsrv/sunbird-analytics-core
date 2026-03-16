package org.ekstep.analytics.model

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{DeviceProfileModel, JobConfig, OutputDispatcher}
import org.scalamock.scalatest.MockFactory
import org.ekstep.analytics.util.Constants
import scala.collection.mutable.ListBuffer
import scala.io.Source
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.util.{EmbeddedCassandra, EmbeddedPostgresql}


class TestExperimentDefinition  extends SparkSpec(null) with MockFactory {

  var schema: ListBuffer[ExperimentDefinitionMetadata] = ListBuffer()

  implicit var util = mock[ExperimentDataUtils]
  implicit val fc = new FrameworkContext();

  override def beforeAll(){
    super.beforeAll()
    EmbeddedPostgresql.start()
    EmbeddedPostgresql.createDeviceProfileTable()
    EmbeddedPostgresql.createExperimentTable()
    val insertQuery = s"""INSERT INTO ${Constants.EXPERIMENT_DEFINITION_TABLE}(exp_id, exp_name, criteria, exp_data, status) VALUES ('U1234', 'USER_ORG', '{"type":"user","filters":{"organisations.orgName":["sunbird"]}}', '{"startDate":"2019-08-06","endDate":"2019-08-09", "key":"/org/profile","client":"portal"}', 'SUBMITTED')"""
    EmbeddedPostgresql.execute(insertQuery)
  }

  override def afterAll(): Unit ={
    super.afterAll()
    EmbeddedPostgresql.close
  }

  it should " map userid to experiement id  with criteria" in {


    val experiments = loadFile[ExperimentDefinition]("src/test/resources/experiment/experiments.json")
    val userdata = JSONUtils.deserialize[UserResponse](Source.fromInputStream
    (getClass.getResourceAsStream("/experiment/userResponse.json")).getLines().mkString)

    val criteria = JSONUtils.deserialize[CriteriaModel](experiments.collect().apply(0).criteria)


    (util.getUserDetails[UserResponse](_: String)(_: Manifest[UserResponse])).
      expects(JSONUtils.serialize(criteria.filters), *).returns(userdata)
    (util.getDeviceProfile(_: String)(_: SparkContext)).expects("local_device_profile", *).
      returns(loadFile[DeviceProfileModel]("src/test/resources/experiment/device_profile.txt"))


    val out = ExperimentDefinitionModel.algorithmProcess(experiments, schema)
    val result = out.fold(sc.emptyRDD[ExperimentDefinitionOutput])(_ ++ _)

    result.count() should be(6)

    val userMappedCount = result.filter { x => x.id.equals("U1234") }

    userMappedCount.count() should be(6)

  }

  it should "execute for null user response" in {


    val experiments = loadFile[ExperimentDefinition]("src/test/resources/experiment/experiments.json")
    val criteria = JSONUtils.deserialize[CriteriaModel](experiments.collect().apply(0).criteria)


    (util.getUserDetails[UserResponse](_: String)(_: Manifest[UserResponse])).
      expects(JSONUtils.serialize(criteria.filters), *).returns(null)
    (util.getDeviceProfile(_: String)(_: SparkContext)).expects("local_device_profile", *).
      returns(loadFile[DeviceProfileModel]("src/test/resources/experiment/device_profile.txt"))


    val out = ExperimentDefinitionModel.algorithmProcess(experiments, schema)
    val result = out.fold(sc.emptyRDD[ExperimentDefinitionOutput])(_ ++ _)

    result.count() should be(0)

    val userMappedCount = result.filter { x => x.id.equals("U1234") }

    userMappedCount.count() should be(0)

  }

  it should " return empty user response" in {


    val experiments = loadFile[ExperimentDefinition]("src/test/resources/experiment/experiments.json")
    val criteria = JSONUtils.deserialize[CriteriaModel](experiments.collect().apply(0).criteria)
    val userdata = JSONUtils.deserialize[UserResponse](Source.fromInputStream
    (getClass.getResourceAsStream("/experiment/userEmptyResponse.json")).getLines().mkString)


    (util.getUserDetails[UserResponse](_: String)(_: Manifest[UserResponse])).
      expects(JSONUtils.serialize(criteria.filters), *).returns(userdata)
    (util.getDeviceProfile(_: String)(_: SparkContext)).expects("local_device_profile", *).
      returns(loadFile[DeviceProfileModel]("src/test/resources/experiment/device_profile.txt"))


    val out = ExperimentDefinitionModel.algorithmProcess(experiments, schema)
    val result = out.fold(sc.emptyRDD[ExperimentDefinitionOutput])(_ ++ _)

    result.count() should be(0)

    val userMappedCount = result.filter { x => x.id.equals("U1234") }

    userMappedCount.count() should be(0)

  }

  it should " throws exception and returns empty user response " in {


    val experiments = loadFile[ExperimentDefinition]("src/test/resources/experiment/experiments.json")
    val criteria = JSONUtils.deserialize[CriteriaModel](experiments.collect().apply(0).criteria)
    val userdata = JSONUtils.deserialize[UserResponse](Source.fromInputStream
    (getClass.getResourceAsStream("/experiment/userErrorResponse.json")).getLines().mkString)

    (util.getUserDetails[UserResponse](_: String)(_: Manifest[UserResponse])).
      expects(JSONUtils.serialize(criteria.filters), *).returns(userdata)
    (util.getDeviceProfile(_: String)(_: SparkContext)).expects("local_device_profile", *).
      returns(loadFile[DeviceProfileModel]("src/test/resources/experiment/device_profile.txt"))


    val out = ExperimentDefinitionModel.algorithmProcess(experiments, schema)
    val result = out.fold(sc.emptyRDD[ExperimentDefinitionOutput])(_ ++ _)

    result.count() should be(0)

    val userMappedCount = result.filter { x => x.id.equals("U1234") }

    userMappedCount.count() should be(0)

  }

  it should " map deviceid to experiement with experiment id B1534 with criteria firstacccess and state" in {

    val experiments = loadFile[ExperimentDefinition]("src/test/resources/experiment/device-experiment-config.json")

    val userdata = JSONUtils.deserialize[UserResponse](Source.fromInputStream
    (getClass.getResourceAsStream("/experiment/userResponse.json")).getLines().mkString)

    (util.getDeviceProfile(_: String)(_: SparkContext)).expects("local_device_profile", *).
      returns(loadFile[DeviceProfileModel]("src/test/resources/experiment/device_profile.txt"))

    val out = ExperimentDefinitionModel.algorithmProcess(experiments, schema)
    val result = out.fold(sc.emptyRDD[ExperimentDefinitionOutput])(_ ++ _)

    result.count().shouldBe(41)

    val deviceMappedCount = result.filter { x => x.id.equals("D1234") }
    result.count().shouldBe(41)

  }


  it should "save mapped deviceid and userid for experiment to es" in {

    val config = "{\"search\":{\"type\":\"none\"},\"model\":\"org.ekstep.analytics.model.ExperimentDefinitionModel\",\"modelParams\":{\"sparkCassandraConnectionHost\":\"localhost\",\"sparkElasticsearchConnectionHost\":\"localhost\"},\"output\":[{\"to\":\"elasticsearch\",\"params\":{\"index\":\"experiment\"}}],\"parallelization\":8,\"appName\":\"Experiment-Definition\",\"deviceMapping\":false}"
    val jobconfig = JSONUtils.deserialize[JobConfig](config)
    val out = ExperimentDefinitionModel.execute(sc.emptyRDD, jobconfig.modelParams)
  }

}
