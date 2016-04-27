/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka.common

import java.io.File
import java.nio.ByteBuffer

import kafka.api.{OffsetCommitRequest, PartitionMetadata, RequestOrResponse}
import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.consumer.{ConsumerConfig, ConsumerThreadId, AssignmentContext, PartitionAssignor}
import kafka.network.BlockingChannel
import org.locationtech.geomesa.kafka.consumer.Broker

import scala.collection.immutable

trait KafkaUtils {
  def channelToPayload: (BlockingChannel) => ByteBuffer
  def channelSend(bc: BlockingChannel, requestOrResponse: RequestOrResponse): Long
  def leaderBrokerForPartition: PartitionMetadata => Option[Broker]
  def assign(partitionAssignor: PartitionAssignor, ac: AssignmentContext): scala.collection.Map[TopicAndPartition, ConsumerThreadId]
  def createZkUtils(config: ConsumerConfig): ZkUtils =
    createZkUtils(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs)
  def createZkUtils(zkConnect: String, sessionTimeout: Int, connectTimeout: Int): ZkUtils
  def tryFindNewLeader(tap: TopicAndPartition,
                       partitions: Option[Seq[PartitionMetadata]],
                       oldLeader: Option[Broker],
                       tries: Int): Option[Broker]
  def rm(file: File): Unit
  def createOffsetAndMetadata(offset: Long, time: Long): OffsetAndMetadata
  def createOffsetCommitRequest(groupId: String,
                                requestInfo: immutable.Map[TopicAndPartition, OffsetAndMetadata],
                                versionId: Short,
                                correlationId: Int,
                                clientId: String): OffsetCommitRequest
}
