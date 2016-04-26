package org.locationtech.geomesa.kafka09

import java.io.File
import java.nio.ByteBuffer

import kafka.consumer.{AssignmentContext, PartitionAssignor}
import kafka.network.BlockingChannel
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Utils
import org.locationtech.geomesa.kafka.{AbstractZkUtils, AbstractKafkaUtils}

class KafkaUtils extends AbstractKafkaUtils {
  def channelToPayload: (BlockingChannel) => ByteBuffer = _.receive().payload()
  def assign(partitionAssignor: PartitionAssignor, ac: AssignmentContext) = partitionAssignor.assign(ac).get(ac.consumerId)
  def createZkUtils(zkConnect: String, sessionTimeout: Int, connectTimeout: Int): AbstractZkUtils =
    ZkUtils(kafka.utils.ZkUtils(zkConnect, sessionTimeout, connectTimeout, JaasUtils.isZkSecurityEnabled))
  def rm(file: File): Unit = Utils.delete(file)
}
