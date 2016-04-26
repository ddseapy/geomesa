package org.locationtech.geomesa.kafka

import java.util.{Properties, ServiceLoader}

import com.typesafe.scalalogging.LazyLogging
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.TestUtils

trait AbstractTestKafkaUtils {
  def createBrokerConfig(nodeId: Int, zkConnect: String): Properties
  def choosePort: Int
  def createServer(props: Properties): KafkaServer
}

object TestKafkaUtilsLoader extends LazyLogging {
  lazy val testKafkaUtils: AbstractTestKafkaUtils = {
    val tkuIter = ServiceLoader.load(classOf[AbstractTestKafkaUtils]).iterator()
    if (tkuIter.hasNext) {
      val first = tkuIter.next()
      if (tkuIter.hasNext) {
        logger.warn(s"Multiple geomesa TestKafkaUtils found.  Should only have one. Using the first: '$first'")
      }
      first
    } else {
      logger.debug(s"No geomesa TestKafkaUtils found.  Using default one for 0.8.")
      TestKafkaUtils
    }
  }
}

/**
  * Default AbstractTestKafkaUtils for 0.8
  */
object TestKafkaUtils extends AbstractTestKafkaUtils {
  def createBrokerConfig(nodeId: Int, zkConnect: String): Properties = TestUtils.createBrokerConfig(nodeId)
  def choosePort: Int = TestUtils.choosePort()
  def createServer(props: Properties): KafkaServer = TestUtils.createServer(new KafkaConfig(props))
}