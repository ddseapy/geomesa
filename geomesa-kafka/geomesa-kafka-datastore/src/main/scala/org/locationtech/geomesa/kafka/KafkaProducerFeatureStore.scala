package org.locationtech.geomesa.kafka

import java.nio.charset.StandardCharsets
import java.{util => ju}

import com.vividsolutions.jts.geom.Envelope
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.data.{FeatureReader, FeatureWriter, Query}
import org.geotools.feature.{FeatureReaderIterator, FeatureCollection}
import org.geotools.feature.collection.{BridgeIterator, DelegateFeatureIterator}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.feature.{AvroFeatureEncoder, AvroSimpleFeature}
import org.locationtech.geomesa.utils.text.ObjectPoolFactory
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.identity.FeatureId
import org.opengis.filter.{Filter, Id}

import scala.collection.JavaConversions._

object KafkaProducerFeatureStore {
  val DELETE_KEY = "delete".getBytes(StandardCharsets.UTF_8)
}

class KafkaProducerFeatureStore(entry: ContentEntry,
                                schema: SimpleFeatureType,
                                broker: String,
                                query: Query)
  extends ContentFeatureStore(entry, query) {

  val typeName = entry.getTypeName

  override def getBoundsInternal(query: Query) =
    ReferencedEnvelope.create(new Envelope(-180, 180, -90, 90), DefaultGeographicCRS.WGS84)

  override def buildFeatureType(): SimpleFeatureType = schema

  type FW = FeatureWriter[SimpleFeatureType, SimpleFeature]

  val writerPool = ObjectPoolFactory(new ModifyingFeatureWriter(query), 5)
  override def addFeatures(featureCollection: FeatureCollection[SimpleFeatureType, SimpleFeature]): ju.List[FeatureId] = {
    writerPool.withResource { fw =>
      val ret = Array.ofDim[FeatureId](featureCollection.size())
      fw.setIter(new BridgeIterator[SimpleFeature](featureCollection.features()))
      var i = 0
      while(fw.hasNext) {
        val sf = fw.next()
        ret(i) = sf.getIdentifier
        i+=1
        fw.write()
      }
      ret.toList
    }
  }

  override def getWriterInternal(query: Query, flags: Int) =
    new ModifyingFeatureWriter(query)

  trait KafkaFeatureWriter {
    val encoder = new AvroFeatureEncoder(schema)
    val props = new ju.Properties()
    props.put("metadata.broker.list", broker)
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    val kafkaProducer = new Producer[Array[Byte], Array[Byte]](new ProducerConfig(props))
  }

  class ModifyingFeatureWriter(query: Query) extends FW with KafkaFeatureWriter {
    private var id = 1L
    def getNextId: FeatureId = {
      val ret = id
      id += 1
      new FeatureIdImpl(ret.toString)
    }

    var toModify: Iterator[SimpleFeature] =
      if(query == null) Iterator[SimpleFeature]()
      else if(query.getFilter == null) Iterator.continually(new AvroSimpleFeature(getNextId, schema))
      else query.getFilter match {
        case ids: Id        =>
          ids.getIDs.map(id => new AvroSimpleFeature(new FeatureIdImpl(id.toString), schema)).iterator

        case Filter.INCLUDE =>
          Iterator.continually(new AvroSimpleFeature(new FeatureIdImpl(""), schema))
      }

    def setIter(iter: Iterator[SimpleFeature]): Unit = {
      toModify = iter
    }

    var curFeature: SimpleFeature = null
    override def getFeatureType: SimpleFeatureType = schema
    override def next(): SimpleFeature = {
      curFeature = toModify.next()
      curFeature
    }
    override def remove(): Unit = {
      val bytes = curFeature.getID.getBytes(StandardCharsets.UTF_8)
      val delMsg = new KeyedMessage[Array[Byte], Array[Byte]](typeName, KafkaProducerFeatureStore.DELETE_KEY, bytes)
      curFeature = null
      kafkaProducer.send(delMsg)
    }
    override def write(): Unit = {
      val encoded = encoder.encode(curFeature)
      val msg = new KeyedMessage[Array[Byte], Array[Byte]](typeName, encoded)
      curFeature = null
      kafkaProducer.send(msg)
    }
    override def hasNext: Boolean = toModify.hasNext
    override def close(): Unit = {}
  }

  override def getCountInternal(query: Query): Int = 0
  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = null
}
