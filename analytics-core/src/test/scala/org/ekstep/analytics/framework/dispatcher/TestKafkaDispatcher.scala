package org.ekstep.analytics.framework.dispatcher

import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{Event, SparkSpec}
import org.ekstep.analytics.framework.FrameworkContext
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import net.manub.embeddedkafka.EmbeddedKafkaConfig

class TestKafkaDispatcher extends SparkSpec with EmbeddedKafka {

    implicit val fc = new FrameworkContext();
    
    "KafkaDispatcher" should "send events to kafka" in {

        val rdd = sc.parallelize(Seq("msg1", "msg2", "msg3"), 1)
        val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)
        withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
          
            Console.println("Kafka started...");
            createCustomTopic("test1", Map(), 1, 1)
            implicit val serializer = new StringSerializer()
            implicit val deserializer = new StringDeserializer()
            
            KafkaDispatcher.dispatch(rdd.collect(), Map("brokerList" -> "localhost:9092", "topic" -> "topic"));
            val msg = consumeFirstMessageFrom("topic");
            msg should not be (null);
            msg should be ("msg1")
            
            KafkaDispatcher.dispatch(Map("brokerList" -> "localhost:9092", "topic" -> "test1"), rdd);
            val msg1 = consumeFirstMessageFrom("test1");
            msg1 should not be (null);
            msg1 should be ("msg1")
        }

    }

    it should "give DispatcherException if broker list or topic is missing" in {

        events = loadFile[Event](file)
        val rdd = events.map(f => JSONUtils.serialize(f))
        the[DispatcherException] thrownBy {
            KafkaDispatcher.dispatch(Map("topic" -> "test"), rdd);
        } should have message "brokerList parameter is required to send output to kafka"
        
        the[DispatcherException] thrownBy {
            KafkaDispatcher.dispatch(rdd.collect(), Map("topic" -> "test"));
        } should have message "brokerList parameter is required to send output to kafka"

        the[DispatcherException] thrownBy {
            KafkaDispatcher.dispatch(Map("brokerList" -> "localhost:9092"), rdd);
        } should have message "topic parameter is required to send output to kafka"
        
        the[DispatcherException] thrownBy {
            KafkaDispatcher.dispatch(rdd.collect(), Map("brokerList" -> "localhost:9092"));
        } should have message "topic parameter is required to send output to kafka"

    }
}
