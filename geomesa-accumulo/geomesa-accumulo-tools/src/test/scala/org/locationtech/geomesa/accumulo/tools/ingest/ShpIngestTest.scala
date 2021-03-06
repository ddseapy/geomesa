/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.ingest

import java.io.File
import java.nio.file.{Files, Path}
import java.util.Date

import org.geotools.data.shapefile.ShapefileDataStoreFactory
import org.geotools.data.store.ReprojectingFeatureCollection
import org.geotools.data.{DataStore, Transaction}
import org.geotools.factory.Hints
import org.geotools.referencing.CRS
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.tools.AccumuloRunner
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.BeforeAfterAll

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class ShpIngestTest extends Specification with BeforeAfterAll {

  sequential

  // note: shpfile always puts geom first
  val schema = SimpleFeatureTypes.createType("shpingest", "*geom:Point:srid=4326,age:Integer,dtg:Date")

  val features = Seq(
    ScalaSimpleFeature.create(schema, "1", "POINT(10.0 30.0)", 1, "2011-01-01T00:00:00.000Z"),
    ScalaSimpleFeature.create(schema, "2", "POINT(20.0 40.0)", 2, "2012-01-01T00:00:00.000Z")
  )

  val baseArgs = Array[String]("ingest", "--zookeepers", "zoo", "--mock", "--instance", "mycloud", "--user", "myuser",
    "--password", "mypassword", "--catalog", "testshpingestcatalog", "--force")

  var dir: Path = _
  var shpFile: File = _
  var shpFileToReproject: File = _

  "ShpIngest" should {

    "should properly ingest a shapefile" in {
      val args = baseArgs :+ shpFile.getAbsolutePath

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloIngestCommand]
      command.execute()

      val fs = command.withDataStore(_.getFeatureSource("shpingest"))

      SelfClosingIterator(fs.getFeatures.features).toList must haveLength(2)

      val bounds = fs.getBounds
      bounds.getMinX mustEqual 10.0
      bounds.getMaxX mustEqual 20.0
      bounds.getMinY mustEqual 30.0
      bounds.getMaxY mustEqual 40.0

      command.withDataStore { ds =>
        ds.stats.getAttributeBounds[Date](ds.getSchema(schema.getTypeName), "dtg").map(_.tuple) must
            beSome((features.head.getAttribute("dtg"), features.last.getAttribute("dtg"), 2L))
      }
    }

    "should support renaming the feature type" in {
      val args = baseArgs ++ Array("--feature-name", "changed", shpFile.getAbsolutePath)

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloIngestCommand]
      command.execute()

      val fs = command.withDataStore(_.getFeatureSource("changed"))

      SelfClosingIterator(fs.getFeatures.features).toList must haveLength(2)

      val bounds = fs.getBounds
      bounds.getMinX mustEqual 10.0
      bounds.getMaxX mustEqual 20.0
      bounds.getMinY mustEqual 30.0
      bounds.getMaxY mustEqual 40.0

      command.withDataStore { ds =>
        ds.stats.getAttributeBounds[Date](ds.getSchema("changed"), "dtg").map(_.tuple) must
            beSome((features.head.getAttribute("dtg"), features.last.getAttribute("dtg"), 2L))
      }
    }

    "reproject to 4326 automatically on ingest" in {
      val args = baseArgs :+ shpFileToReproject.getAbsolutePath

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloIngestCommand]
      command.execute()

      val fs = command.withDataStore(_.getFeatureSource("shpingest3857"))

      SelfClosingIterator(fs.getFeatures.features).toList must haveLength(2)

      val bounds = fs.getBounds
      bounds.getMinX must beCloseTo(10.0, 0.0001)
      bounds.getMaxX must beCloseTo(20.0, 0.0001)
      bounds.getMinY must beCloseTo(30.0, 0.0001)
      bounds.getMaxY must beCloseTo(40.0, 0.0001)

      command.withDataStore { ds =>
        ds.stats.getAttributeBounds[Date](ds.getSchema(schema.getTypeName), "dtg").map(_.tuple) must
          beSome((features.head.getAttribute("dtg"), features.last.getAttribute("dtg"), 2L))
      }
    }
  }

  override def beforeAll(): Unit = {
    dir = Files.createTempDirectory("gm-shp-ingest-test")
    shpFile = new File(dir.toFile, "shpingest.shp")
    shpFileToReproject = new File(dir.toFile, "shpingest3857.shp")

    val shpStore = new ShapefileDataStoreFactory().createNewDataStore(Map("url" -> shpFile.toURI.toURL))
    val shpStoreToReproject: DataStore = new ShapefileDataStoreFactory().createNewDataStore(Map("url" -> shpFileToReproject.toURI.toURL))

    try {
      shpStore.createSchema(schema)
      WithClose(shpStore.getFeatureWriterAppend("shpingest", Transaction.AUTO_COMMIT)) { writer =>
        features.foreach { feature =>
          val toWrite = writer.next()
          toWrite.setAttributes(feature.getAttributes)
          toWrite.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          toWrite.getUserData.put(Hints.PROVIDED_FID, feature.getID)
          writer.write()
        }
      }

      val initialFeatures = shpStore.getFeatureSource("shpingest").getFeatures
      val projectedFeatures = new ReprojectingFeatureCollection(initialFeatures, CRS.decode("EPSG:3857"))

      shpStoreToReproject.createSchema(projectedFeatures.getSchema)
      WithClose(shpStoreToReproject.getFeatureWriterAppend("shpingest3857", Transaction.AUTO_COMMIT)) { writer =>
        CloseableIterator(projectedFeatures.features()).foreach { feature =>
          val toWrite = writer.next()
          toWrite.setAttributes(feature.getAttributes)
          toWrite.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          toWrite.getUserData.put(Hints.PROVIDED_FID, feature.getID)
          writer.write()
        }
      }
    } finally {
      shpStore.dispose()
      shpStoreToReproject.dispose()
    }
  }

  override def afterAll(): Unit = PathUtils.deleteRecursively(dir)
}
