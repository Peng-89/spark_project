package com.za.apps.sql
import java.nio.ByteBuffer

import kafka.api.{FetchRequestBuilder, OffsetRequest, Request}
import kafka.client.ClientUtils
import kafka.cluster.BrokerEndPoint
import kafka.common.TopicAndPartition
import kafka.consumer.{ConsumerConfig, SimpleConsumer}
import kafka.coordinator.{GroupMetadataKey, GroupMetadataManager, OffsetKey}
import kafka.coordinator.GroupMetadataManager.GroupMetadataMessageFormatter
import kafka.tools.SimpleConsumerShell.{debug, error, info}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.requests.DescribeGroupsResponse.GroupMetadata
import org.apache.kafka.common.utils.Utils

object KafkaUtils {
  def main(args: Array[String]): Unit = {
    val brokerList = "master:9092,slave1:9092,slave2:9092"
    val metadataTargetBrokers =ClientUtils.parseBrokerList(brokerList)
    val topic="__consumer_offsets"
    for(i<- 0 to 49) {
      val partitionId = i
      val replicaId = -1
      var startingOffset = OffsetRequest.EarliestTime
      val clientId = "SimpleConsumerShell"
      val topicsMetadata = ClientUtils.fetchTopicMetadata(Set(topic), metadataTargetBrokers,
        clientId, 1000).topicsMetadata
      if (topicsMetadata.size != 1 || !topicsMetadata(0).topic.equals(topic)) {
        System.err.println(("Error: no valid topic metadata for topic: %s, " + "what we get from server is only: %s").format(topic, topicsMetadata))
        System.exit(1)
      }
      val partitionsMetadata = topicsMetadata(0).partitionsMetadata
      val partitionMetadataOpt = partitionsMetadata.find(p => p.partitionId == partitionId)
      if (!partitionMetadataOpt.isDefined) {
        System.err.println("Error: partition %d does not exist for topic %s".format(partitionId, topic))
        System.exit(1)
      }

      // validating replica id and initializing target broker
      var fetchTargetBroker: BrokerEndPoint = null
      var replicaOpt: Option[BrokerEndPoint] = null
      if (replicaId == -1) {
        replicaOpt = partitionMetadataOpt.get.leader
        if (!replicaOpt.isDefined) {
          System.err.println("Error: user specifies to fetch from leader for partition (%s, %d) which has not been elected yet".format(topic, partitionId))
          System.exit(1)
        }
      }
      else {
        val replicasForPartition = partitionMetadataOpt.get.replicas
        replicaOpt = replicasForPartition.find(r => r.id == replicaId)
        if (!replicaOpt.isDefined) {
          System.err.println("Error: replica %d does not exist for partition (%s, %d)".format(replicaId, topic, partitionId))
          System.exit(1)
        }
      }
      fetchTargetBroker = replicaOpt.get

      if (startingOffset < OffsetRequest.EarliestTime) {
        System.err.println("Invalid starting offset: %d".format(startingOffset))
        System.exit(1)
      }

      if (startingOffset < 0) {
        val simpleConsumer = new SimpleConsumer(fetchTargetBroker.host,
          fetchTargetBroker.port,
          ConsumerConfig.SocketTimeout,
          ConsumerConfig.SocketBufferSize, clientId)
        try {
          startingOffset = simpleConsumer.earliestOrLatestOffset(TopicAndPartition(topic, partitionId), startingOffset,
            Request.DebuggingConsumerId)
        } catch {
          case t: Throwable =>
            System.err.println("Error in getting earliest or latest offset due to: " + Utils.stackTrace(t))
            System.exit(1)
        } finally {
          if (simpleConsumer != null)
            simpleConsumer.close()
        }
      }

      val formatter = new GroupMetadataMessageFormatter();

      val replicaString = if (replicaId > 0) "leader" else "replica"
      info("Starting simple consumer shell to partition [%s, %d], %s [%d], host and port: [%s, %d], from offset [%d]"
        .format(topic, partitionId, replicaString, replicaId,
          fetchTargetBroker.host,
          fetchTargetBroker.port, startingOffset))
      val simpleConsumer = new SimpleConsumer(fetchTargetBroker.host,
        fetchTargetBroker.port,
        10000, 64 * 1024, clientId)

      val maxMessages = Integer.MAX_VALUE
      val fetchSize = 1024 * 1024
      val maxWaitMs = 1000
      val noWaitAtEndOfLog = true
      val printOffsets = false

      val fetchRequestBuilder = new FetchRequestBuilder()
        .clientId(clientId)
        .maxWait(maxWaitMs)
        .minBytes(ConsumerConfig.MinFetchBytes)

      val thread = Utils.newThread("kafka-simpleconsumer-shell", new Runnable() {
        def run() {
          var offset = startingOffset
          var numMessagesConsumed = 0
          try {
            while (numMessagesConsumed < maxMessages) {
              val fetchRequest = fetchRequestBuilder
                .addFetch(topic, partitionId, offset, fetchSize)
                .build()
              val fetchResponse = simpleConsumer.fetch(fetchRequest)
              val messageSet = fetchResponse.messageSet(topic, partitionId)
              if (messageSet.validBytes <= 0 && noWaitAtEndOfLog) {
                println("Terminating. Reached the end of partition (%s, %d) at offset %d".format(topic, partitionId, offset))
                return
              }
              debug("multi fetched " + messageSet.sizeInBytes + " bytes from offset " + offset)
              for (messageAndOffset <- messageSet if numMessagesConsumed < maxMessages) {
                try {
                  offset = messageAndOffset.nextOffset
                  if (printOffsets)
                    System.out.println("next offset = " + offset)
                  val message = messageAndOffset.message
                  val key = if (message.hasKey) Utils.readBytes(message.key) else null
                  val value = if (message.isNull) null else Utils.readBytes(message.payload)
                  val serializedKeySize = if (message.hasKey) key.size else -1
                  val serializedValueSize = if (message.isNull) -1 else value.size
//                val consumerRecord=new ConsumerRecord(topic, partitionId, offset, message.timestamp,
//                    message.timestampType, message.checksum, serializedKeySize, serializedValueSize, key, value), System.out)
                  Option(key).map(key => GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key))).foreach {
                    case groupMetadataKey: GroupMetadataKey =>
                      val groupId = groupMetadataKey.key
                      println("group:"+groupId)

                    case offsetKey:OffsetKey=>
                      val groupId = offsetKey.key.group
                      val topic = offsetKey.key.topicPartition.topic();
                      println("group:"+groupId)
                      println("topic:"+topic)
                    case _ => // no-op
                  }

                  numMessagesConsumed += 1
                } catch {
                  case e: Throwable =>
                    if (true)
                      error("Error processing message, skipping this message: ", e)
                    else
                      throw e
                }
                if (System.out.checkError()) {
                  // This means no one is listening to our output stream any more, time to shutdown
                  System.err.println("Unable to write to standard out, closing consumer.")
                  formatter.close()
                  simpleConsumer.close()
                  System.exit(1)
                }
              }
            }
          } catch {
            case e: Throwable =>
              error("Error consuming topic, partition, replica (%s, %d, %d) with offset [%d]".format(topic, partitionId, replicaId, offset), e)
          } finally {
            info(s"Consumed $numMessagesConsumed messages")
          }
        }
      }, false)
      thread.start()
      thread.join()
      System.out.flush()
      formatter.close()
      simpleConsumer.close()
    }
  }
}
