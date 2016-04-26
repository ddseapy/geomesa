package org.locationtech.geomesa.kafka

import java.io.File
import java.nio.ByteBuffer
import java.util.ServiceLoader

import com.typesafe.scalalogging.LazyLogging
import kafka.admin.AdminUtils
import kafka.client.ClientUtils
import kafka.cluster.Broker
import kafka.common.TopicAndPartition
import kafka.consumer.{ConsumerThreadId, PartitionAssignor, AssignmentContext, ConsumerConfig}
import kafka.network.BlockingChannel
import kafka.utils.Utils
import org.I0Itec.zkclient.ZkClient
import org.apache.zookeeper.data.Stat

import scala.collection.Map

trait AbstractKafkaUtils {
  def channelToPayload: (BlockingChannel) => ByteBuffer
  def assign(partitionAssignor: PartitionAssignor, ac: AssignmentContext): Map[TopicAndPartition, ConsumerThreadId]
  def createZkUtils(config: ConsumerConfig): AbstractZkUtils =
    createZkUtils(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs)
  def createZkUtils(zkConnect: String, sessionTimeout: Int, connectTimeout: Int): AbstractZkUtils
  def rm(file: File): Unit
}

object KafkaUtilsLoader extends LazyLogging {
  lazy val kafkaUtils: AbstractKafkaUtils = {
    val kuIter = ServiceLoader.load(classOf[AbstractKafkaUtils]).iterator()
    if (kuIter.hasNext) {
      val first = kuIter.next()
      if (kuIter.hasNext) {
        logger.warn(s"Multiple geomesa KafkaUtils found.  Should only have one. Using the first: '$first'")
      }
      first
    } else {
      logger.debug(s"No geomesa KafkaUtils found.  Using default one for 0.8.")
      KafkaUtils
    }
  }
}

/**
  * Default KafkaUtils for kafka 0.8
  */
object KafkaUtils extends AbstractKafkaUtils {
  def channelToPayload: (BlockingChannel) => ByteBuffer = _.receive().buffer
  def assign(partitionAssignor: PartitionAssignor, ac: AssignmentContext) = partitionAssignor.assign(ac)
  def createZkUtils(zkConnect: String, sessionTimeout: Int, connectTimeout: Int): AbstractZkUtils =
    ZkUtils(new ZkClient(zkConnect, sessionTimeout, connectTimeout))
  def rm(file: File): Unit = Utils.rm(file)
}

/**
  * Default ZkUtils for kafka 0.8
  */
case class ZkUtils(zkClient: ZkClient) extends AbstractZkUtils {
  def channelToOffsetManager(groupId: String, socketTimeoutMs: Int, retryBackOffMs: Int): BlockingChannel =
    ClientUtils.channelToOffsetManager(groupId, zkClient, socketTimeoutMs, retryBackOffMs)
  def deleteTopic(topic: String): Unit = AdminUtils.deleteTopic(zkClient, topic)
  def topicExists(topic: String): Boolean = AdminUtils.topicExists(zkClient, topic)
  def createTopic(topic: String, partitions: Int, replication: Int) = AdminUtils.createTopic(zkClient, topic, partitions, replication)
  def getLeaderForPartition(topic: String, partition: Int): Option[Int] = kafka.utils.ZkUtils.getLeaderForPartition(zkClient, topic, partition)
  def createEphemeralPathExpectConflict(path: String, data: String): Unit = kafka.utils.ZkUtils.createEphemeralPathExpectConflict(zkClient, path, data)
  def createEphemeralPathExpectConflictHandleZKBug(path: String,
                                                   data: String,
                                                   expectedCallerData: Any,
                                                   checker: (String, Any) => Boolean,
                                                   backoffTime: Int): Unit =
    kafka.utils.ZkUtils.createEphemeralPathExpectConflictHandleZKBug(zkClient, path, data, expectedCallerData, checker, backoffTime)
  def deletePath(path: String) = kafka.utils.ZkUtils.deletePath(zkClient, path)
  def getConsumerPartitionOwnerPath(groupId: String, topic: String, partition: Int): String =
    kafka.utils.ZkUtils.getConsumerPartitionOwnerPath(groupId, topic, partition)
  def getChildrenParentMayNotExist(path: String): Seq[String] = kafka.utils.ZkUtils.getChildrenParentMayNotExist(zkClient, path)
  def getAllBrokersInCluster: Seq[Broker] = kafka.utils.ZkUtils.getAllBrokersInCluster(zkClient)
  def createAssignmentContext(group: String, consumerId: String, excludeInternalTopics: Boolean): AssignmentContext =
    new AssignmentContext(group, consumerId, excludeInternalTopics, zkClient)
  def readData(path: String): (String, Stat) = kafka.utils.ZkUtils.readData(zkClient, path)
  def close(): Unit = zkClient.close()
}

trait AbstractZkUtils {
  def zkClient: ZkClient
  def channelToOffsetManager(groupId: String, socketTimeoutMs: Int, retryBackOffMs: Int): BlockingChannel
  def deleteTopic(topic: String): Unit
  def topicExists(topic: String): Boolean
  def createTopic(topic: String, partitions: Int, replication: Int): Unit
  def getLeaderForPartition(topic: String, partition: Int): Option[Int]
  def createEphemeralPathExpectConflict(path: String, data: String): Unit
  def createEphemeralPathExpectConflictHandleZKBug(path: String,
                                                   data: String,
                                                   expectedCallerData: Any,
                                                   checker: (String, Any) => Boolean,
                                                   backoffTime: Int): Unit
  def deletePath(path: String): Unit
  def getConsumerPartitionOwnerPath(groupId: String, topic: String, partition: Int): String
  def getChildrenParentMayNotExist(path: String): Seq[String]
  def getAllBrokersInCluster: Seq[Broker]
  def createAssignmentContext(group: String, consumerId: String, excludeInternalTopics: Boolean): AssignmentContext
  def readData(path: String): (String, Stat)
  def close(): Unit
}
