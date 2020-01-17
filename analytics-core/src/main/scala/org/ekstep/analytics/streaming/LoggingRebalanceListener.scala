package org.ekstep.analytics.streaming

import java.util
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger
import scala.collection.JavaConversions._

/**
 * LoggingRebalanceListener will log partition re-balancing events for selected topic.
 *
 * @param topic
 * @param log
 */
class LoggingRebalanceListener(topic: String, log: Logger) extends ConsumerRebalanceListener {
    private var currentAssignment = Set.empty[TopicPartition]

    override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        val newAssignment = partitions.toSet
        val addedPartitions = newAssignment -- currentAssignment
        val removedPartitions = currentAssignment -- newAssignment
        currentAssignment = newAssignment

        logPartitionChanges("added", addedPartitions)
        logPartitionChanges("removed", removedPartitions)
        logNewAssignment(newAssignment)
    }

    override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {}

    protected def logPartitionChanges(changeVerb: String, delta: Set[TopicPartition]): Unit = {
        if (delta.nonEmpty) {
            val partitioningInfo = getTopicPartitioningInfo(delta)
            log.info(s"Partitions $changeVerb: topic=${topic}, partitions=[$partitioningInfo]")
        }
    }

    protected def logNewAssignment(partitions: Set[TopicPartition]): Unit = {}

    protected def getTopicPartitioningInfo(
        partitions: Set[TopicPartition], maxLength: Option[Int] = None): String = {
        def shorten(string: String, length: Int) = {
            if (string.length > length) string.take(length - 3) + "..." else string
        }

        val orderedPartitions = getOrderedPartitionsForTopic(topic, partitions)
        val fullPartitioningInfo = orderedPartitions.mkString(",")

        if (fullPartitioningInfo.isEmpty) "unassigned"
        else if (maxLength.isDefined) shorten(fullPartitioningInfo, length = maxLength.get)
        else fullPartitioningInfo
    }

    private def getOrderedPartitionsForTopic(topic: String, partitions: Set[TopicPartition]): Seq[Int] = {
        val partitionsForTopic = partitions.collect {
            case partition if partition.topic == topic =>
                partition.partition
        }
        partitionsForTopic.toSeq.sorted
    }
}