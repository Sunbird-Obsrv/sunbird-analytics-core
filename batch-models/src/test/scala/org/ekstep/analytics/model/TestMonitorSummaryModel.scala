package org.ekstep.analytics.model
/**
 * @author Yuva
 */
import org.ekstep.analytics.framework.{FrameworkContext, V3Event}
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.ekstep.analytics.framework.conf.AppConf

class TestMonitorSummaryModel extends SparkSpec(null) with EmbeddedKafka  {

    it should "monitor the data products logs for 1 job failure" in {

        val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)
        withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>

            Console.println("Kafka started...");
            createCustomTopic("test", Map(), 1, 1)
            implicit val serializer = new StringSerializer()
            implicit val deserializer = new StringDeserializer()

            implicit val fc = new FrameworkContext();
            val modelMapping = loadFile[ModelMapping]("src/test/resources/monitor-summary/model-mapping.log").collect().toList;
            val rdd1 = loadFile[V3Event]("src/test/resources/monitor-summary/joblog.log");
            val rdd2 = MonitorSummaryModel.execute(rdd1, Option(Map("model" -> modelMapping, "pushMetrics" -> true.asInstanceOf[AnyRef], "monitor.notification.slack" -> "true", "topic" -> "test", "brokerList" -> "localhost:9092")));
            val eks_map = rdd2.first().edata.eks.asInstanceOf[Map[String, AnyRef]]
            eks_map.get("jobs_completed_count").get.asInstanceOf[Number].longValue() should be(2)
            eks_map.get("total_events_generated").get.asInstanceOf[Number].longValue() should be(475)
            eks_map.get("jobs_failed_count").get.asInstanceOf[Number].longValue() should be(1)
            eks_map.get("total_ts").get.asInstanceOf[Number].doubleValue() should be(23.0)
            eks_map.get("jobs_start_count").get.asInstanceOf[Number].longValue() should be(3)
        }

    }

    it should "monitor the data products logs for all success" in {

        implicit val fc = new FrameworkContext();
        val modelMapping = loadFile[ModelMapping]("src/test/resources/monitor-summary/model-mapping.log").collect().toList;
        val rdd1 = loadFile[V3Event]("src/test/resources/monitor-summary/joblog1.log");
        val rdd2 = MonitorSummaryModel.execute(rdd1, Option(Map("model" -> modelMapping)));
        val eks_map = rdd2.first().edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks_map.get("jobs_completed_count").get.asInstanceOf[Number].longValue() should be(2)
        eks_map.get("total_events_generated").get.asInstanceOf[Number].longValue() should be(475)
        eks_map.get("jobs_failed_count").get.asInstanceOf[Number].longValue() should be(0)
        eks_map.get("total_ts").get.asInstanceOf[Number].doubleValue() should be(23.0)
        eks_map.get("jobs_start_count").get.asInstanceOf[Number].longValue() should be(2)
    }

    it should "monitor the data products logs for input output mismatch" in {

        implicit val fc = new FrameworkContext();
        val modelMapping = loadFile[ModelMapping]("src/test/resources/monitor-summary/model-mapping.log").collect().toList;
        val rdd1 = loadFile[V3Event]("src/test/resources/monitor-summary/joblog2.log");
        val rdd2 = MonitorSummaryModel.execute(rdd1, Option(Map("model" -> modelMapping)));
        val eks_map = rdd2.first().edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks_map.get("jobs_completed_count").get.asInstanceOf[Number].longValue() should be(3)
        eks_map.get("total_events_generated").get.asInstanceOf[Number].longValue() should be(775)
        eks_map.get("jobs_failed_count").get.asInstanceOf[Number].longValue() should be(0)
        eks_map.get("total_ts").get.asInstanceOf[Number].doubleValue() should be(43.0)
        eks_map.get("jobs_start_count").get.asInstanceOf[Number].longValue() should be(3)
    }

    it should "monitor the data products logs for input output mismatch and job failure" in {

        implicit val fc = new FrameworkContext();
        val modelMapping = loadFile[ModelMapping]("src/test/resources/monitor-summary/model-mapping.log").collect().toList;
        val rdd1 = loadFile[V3Event]("src/test/resources/monitor-summary/joblog3.log");
        val rdd2 = MonitorSummaryModel.execute(rdd1, Option(Map("model" -> modelMapping)));
        val eks_map = rdd2.first().edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks_map.get("jobs_completed_count").get.asInstanceOf[Number].longValue() should be(2)
        eks_map.get("total_events_generated").get.asInstanceOf[Number].longValue() should be(411)
        eks_map.get("jobs_failed_count").get.asInstanceOf[Number].longValue() should be(2)
        eks_map.get("total_ts").get.asInstanceOf[Number].doubleValue() should be(31.0)
        eks_map.get("jobs_start_count").get.asInstanceOf[Number].longValue() should be(4)
    }

    it should "monitor the data products logs for video streaming jobs" in {

        implicit val fc = new FrameworkContext();
        val modelMapping = loadFile[ModelMapping]("src/test/resources/monitor-summary/model-mapping.log").collect().toList;
        val rdd1 = loadFile[V3Event]("src/test/resources/monitor-summary/joblog4.log");
        val rdd2 = MonitorSummaryModel.execute(rdd1, Option(Map("model" -> modelMapping)));
        val eks_map = rdd2.first().edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks_map.get("jobs_completed_count").get.asInstanceOf[Number].longValue() should be(6)
        eks_map.get("total_events_generated").get.asInstanceOf[Number].longValue() should be(475)
        eks_map.get("jobs_failed_count").get.asInstanceOf[Number].longValue() should be(0)
        eks_map.get("total_ts").get.asInstanceOf[Number].doubleValue() should be(23.0)
        eks_map.get("jobs_start_count").get.asInstanceOf[Number].longValue() should be(6)
    }
}