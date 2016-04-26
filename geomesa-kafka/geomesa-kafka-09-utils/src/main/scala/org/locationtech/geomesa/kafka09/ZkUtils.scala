package org.locationtech.geomesa.kafka09

import kafka.admin.AdminUtils
import kafka.client.ClientUtils
import kafka.cluster.Broker
import kafka.consumer.AssignmentContext
import kafka.network.BlockingChannel
import kafka.utils.ZKCheckedEphemeral
import org.I0Itec.zkclient.ZkClient
import org.apache.zookeeper.data.Stat
import org.locationtech.geomesa.kafka.AbstractZkUtils

case class ZkUtils(zkUtils: kafka.utils.ZkUtils) extends AbstractZkUtils {
  def zkClient: ZkClient = zkUtils.zkClient
  def channelToOffsetManager(groupId: String, socketTimeoutMs: Int, retryBackOffMs: Int): BlockingChannel =
    ClientUtils.channelToOffsetManager(groupId, zkUtils, socketTimeoutMs, retryBackOffMs)
  def deleteTopic(topic: String): Unit = AdminUtils.deleteTopic(zkUtils, topic)
  def topicExists(topic: String): Boolean = AdminUtils.topicExists(zkUtils, topic)
  def createTopic(topic: String, partitions: Int, replication: Int) = AdminUtils.createTopic(zkUtils, topic, partitions, replication)
  def getLeaderForPartition(topic: String, partition: Int): Option[Int] = zkUtils.getLeaderForPartition(topic, partition)
  def createEphemeralPathExpectConflict(path: String, data: String): Unit = zkUtils.createEphemeralPathExpectConflict(path, data)
  def createEphemeralPathExpectConflictHandleZKBug(path: String,
                                                   data: String,
                                                   expectedCallerData: Any,
                                                   checker: (String, Any) => Boolean,
                                                   backoffTime: Int): Unit =
    new ZKCheckedEphemeral(path, data, zkUtils.zkConnection.getZookeeper, zkUtils.isSecure).create()
  def deletePath(path: String): Unit = zkUtils.deletePath(path)
  def getConsumerPartitionOwnerPath(groupId: String, topic: String, partition: Int): String =
    zkUtils.getConsumerPartitionOwnerPath(groupId, topic, partition)
  def getChildrenParentMayNotExist(path: String): Seq[String] = zkUtils.getChildrenParentMayNotExist(path)
  def getAllBrokersInCluster: Seq[Broker] = zkUtils.getAllBrokersInCluster
  def createAssignmentContext(group: String, consumerId: String, excludeInternalTopics: Boolean): AssignmentContext =
    new AssignmentContext(group, consumerId, excludeInternalTopics, zkUtils)
  def readData(path: String): (String, Stat) = zkUtils.readData(path)
  def close(): Unit = zkUtils.close()
}