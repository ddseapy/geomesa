/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka08

import java.io.File
import java.nio.ByteBuffer

import kafka.api.{OffsetCommitRequest, PartitionMetadata, RequestOrResponse}
import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.consumer.{AssignmentContext, PartitionAssignor}
import kafka.network.BlockingChannel
import kafka.utils.{Utils, ZKStringSerializer}
import org.I0Itec.zkclient.ZkClient
import org.locationtech.geomesa.kafka.common.{ZkUtils, KafkaUtils}
import org.locationtech.geomesa.kafka.consumer.Broker

import scala.collection.immutable

class KafkaUtils08 extends KafkaUtils {
  override def channelToPayload: (BlockingChannel) => ByteBuffer = _.receive().buffer
  override def channelSend(bc: BlockingChannel, requestOrResponse: RequestOrResponse): Long = bc.send(requestOrResponse).toLong
  override def leaderBrokerForPartition: PartitionMetadata => Option[Broker] = _.leader.map(l => Broker(l.host, l.port))
  override def assign(partitionAssignor: PartitionAssignor, ac: AssignmentContext) = partitionAssignor.assign(ac)
  override def createZkUtils(zkConnect: String, sessionTimeout: Int, connectTimeout: Int): ZkUtils = {
    // zkStringSerializer is required - otherwise topics won't be created correctly
    ZkUtils08(new ZkClient(zkConnect, sessionTimeout, connectTimeout, ZKStringSerializer))
  }
  override def tryFindNewLeader(tap: TopicAndPartition,
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
  override def rm(file: File): Unit = Utils.rm(file)
  override def createOffsetAndMetadata(offset: Long, time: Long): OffsetAndMetadata = OffsetAndMetadata(offset, timestamp = time)
  override def createOffsetCommitRequest(groupId: String,
                                requestInfo: immutable.Map[TopicAndPartition, OffsetAndMetadata],
                                versionId: Short,
                                correlationId: Int,
                                clientId: String): OffsetCommitRequest =
    new OffsetCommitRequest(groupId, requestInfo, versionId, correlationId, clientId)
}
