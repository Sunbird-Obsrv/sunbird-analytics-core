package org.ekstep.analytics.streaming

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.concurrent.Future
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.clients.producer.Callback

class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {

    lazy val producer = createProducer()

    def send(topic: String, value: String, callBack: Callback): Future[RecordMetadata] = {
        producer.send(new ProducerRecord(topic, value), callBack);
    }
    def close(): Unit = producer.close();
    def flush(): Unit = producer.flush();

}

object KafkaSink {
    def apply(config: java.util.Map[String, Object]): KafkaSink = {
        val f = () => {
            val producer = new KafkaProducer[String, String](config)
            sys.addShutdownHook {
                producer.close()
            }
            producer
        }
        new KafkaSink(f)
    }
}