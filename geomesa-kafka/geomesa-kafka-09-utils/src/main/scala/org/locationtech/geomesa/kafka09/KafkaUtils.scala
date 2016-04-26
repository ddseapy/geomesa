package org.locationtech.geomesa.kafka09

import java.io.File
import java.nio.ByteBuffer

import kafka.api.{PartitionMetadata, RequestOrResponse}
import kafka.common.TopicAndPartition
import kafka.consumer.{AssignmentContext, PartitionAssignor}
import kafka.network.BlockingChannel
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Utils
import org.locationtech.geomesa.kafka.consumer.Broker
import org.locationtech.geomesa.kafka.{AbstractZkUtils, AbstractKafkaUtils}

class KafkaUtils extends AbstractKafkaUtils {
  def channelToPayload: (BlockingChannel) => ByteBuffer = _.receive().payload()
  def channelSend(bc: BlockingChannel, requestOrResponse: RequestOrResponse): Long = bc.send(requestOrResponse)
  def leaderBrokerForPartition: PartitionMetadata => Option[Broker] = _.leader.map(l => Broker(l.host, l.port))
  def assign(partitionAssignor: PartitionAssignor, ac: AssignmentContext) = partitionAssignor.assign(ac).get(ac.consumerId)
  def createZkUtils(zkConnect: String, sessionTimeout: Int, connectTimeout: Int): AbstractZkUtils =
    ZkUtils(kafka.utils.ZkUtils(zkConnect, sessionTimeout, connectTimeout, JaasUtils.isZkSecurityEnabled))
  def tryFindNewLeader(tap: TopicAndPartition,
                       partitions: Option[Seq[PartitionMetadata]],
                       oldLeader: Option[Broker],
                       tries: Int): Option[Broker] = {
    val maybeLeader = partitions.flatMap(_.find(_.partitionId == tap.partition)).flatMap(_.leader)
    val leader = oldLeader match {
      // first time through if the leader hasn't changed give ZooKeeper a second to recover
      // second time, assume the broker did recover before failover, or it was a non-Broker issue
      case Some(old) => maybeLeader.filter(m => (m.host != old.host && m.port != old.port) || tries > 1)
      case None      => maybeLeader
    }

    leader.map(l => Broker(l.host, l.port))
  }
  def rm(file: File): Unit = Utils.delete(file)
}
